"""Thin async GitHub REST API wrapper with typed exceptions.

Scope discipline: this module has zero knowledge of business logic,
database, or Thought2Build models. It performs only the operations the export
service needs.

Two token modes (Phase 21 — T-268):

- **Installation mode (v2, GitHub App):** the client holds no token. Each
  request resolves a short-lived installation token per call from an injected
  ``InstallationTokenSource`` (the T-267 ``TokenProvider``) keyed by
  ``installation_id``. On a 401 — the token may have rotated under us — the
  token is re-minted **exactly once** and the request retried; a second 401
  raises ``GitHubTokenExpiredError``.
- **Static mode (v1, legacy OAuth):** the client holds a plaintext token passed
  at construction. A 401 raises ``GitHubTokenExpiredError`` immediately (there
  is no installation to re-mint from).

A per-client :class:`GitHubCircuitBreaker` trips after repeated network/5xx/429
failures and raises :class:`GitHubUnavailableError` on subsequent calls, so a
caller can surface "sync paused" instead of hammering a degraded GitHub. Breaker
accounting is additive: it never changes how a single response maps to a typed
error (429 → ``GitHubRateLimitError``, 5xx → ``GitHubAPIError``).

Supported operations: create_repo, get_file_sha, upsert_file,
create_issue, update_issue. Later tasks add branch/PR/GraphQL/checks methods.
"""

from __future__ import annotations

import base64
import logging
import time
from typing import Any, Protocol
from urllib.parse import quote, urlencode

import httpx

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"

# Issue-listing pagination bounds. Exported because ``list_issues`` truncates
# silently at the cap: any caller holding a durable cursor has to know the exact
# number of rows that means "there may be more" (see ``list_issues``).
ISSUES_PAGE_SIZE = 100
ISSUES_MAX_PAGES = 20
ISSUES_FETCH_CAP = ISSUES_PAGE_SIZE * ISSUES_MAX_PAGES
_GITHUB_ACCEPT = "application/vnd.github+json"

# Bounded retries for a stale-SHA 409 on a content write (T-276): refetch the
# current SHA and retry, but never loop forever on a persistent conflict.
_UPSERT_MAX_ATTEMPTS = 3

# Bounded timeouts for GitHub REST calls. Unlike the LLM adapters (read=300s for
# streaming), GitHub calls are short request/response round-trips, so the read
# timeout is tight. connect/write/pool follow the project default.
_REQUEST_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
# Bounded connection pool so a burst of export issue-creates cannot exhaust
# sockets against api.github.com.
_CONNECTION_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)


# ---------------------------------------------------------------------------
# Typed exceptions
# ---------------------------------------------------------------------------


class GitHubNotConnectedError(Exception):
    """No GitHub integration exists for the current user."""


class GitHubRepoExistsError(Exception):
    """The requested repo name is already in use under the user's account."""


class GitHubTokenExpiredError(Exception):
    """GitHub returned 401 — the token is no longer valid.

    In installation mode this is raised only after a single re-mint also
    returns 401. Thought2Build never retries with a known-invalid token.
    """


class GitHubRateLimitError(Exception):
    """GitHub returned 429 or a rate-limit primary/secondary signal."""


class GitHubWorkflowsPermissionError(Exception):
    """A write to ``.github/workflows/*`` was refused with 403 (T-276).

    GitHub rejects a CI-workflow write when the App installation lacks the
    ``Workflows: write`` permission. This is a distinct, actionable condition
    (re-approve the App's permissions) — not a generic API error and not a
    rate-limit — so PR-mode export surfaces it as such rather than dead-lettering
    with an opaque 403.
    """


class GitHubUnavailableError(Exception):
    """The circuit breaker is open — GitHub is being treated as unavailable.

    Raised before a request is sent once repeated failures have tripped the
    breaker, so callers surface "sync paused" rather than hammering a degraded
    GitHub. Distinct from a one-off ``GitHubAPIError``.
    """


class GitHubAPIError(Exception):
    """Generic GitHub failure: non-2xx response not covered by the other types."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"GitHub API error {status}: {message}")
        self.status = status
        self.message = message


class GitHubGraphQLError(Exception):
    """A GraphQL request returned an ``errors`` array (T-281).

    The GitHub GraphQL endpoint answers ``200 OK`` even when a query fails,
    carrying the failure in a top-level ``errors`` array, so a 200 is not success
    on its own. This is raised when that array is non-empty and the failure is
    not a recognised permission problem (which maps to
    :class:`GitHubProjectsPermissionError`).
    """


class GitHubProjectsPermissionError(Exception):
    """A Projects v2 GraphQL call was refused for lack of the App's scope (T-281).

    GitHub reports a missing ``Projects`` (read/write) permission as a
    ``FORBIDDEN``/"resource not accessible by integration" GraphQL error. The
    Projects board is opt-in and additive, so the board sync surfaces this as a
    distinct, actionable condition (re-approve the App's permissions) and skips
    the board rather than dead-lettering the job with an opaque error — exactly
    like :class:`GitHubWorkflowsPermissionError` for CI-workflow writes.
    """


class GitHubChecksPermissionError(Exception):
    """A check-run write was refused for lack of the App's ``Checks: write`` (T-282).

    A non-rate-limit ``403`` on ``POST /repos/{repo}/check-runs`` means the
    installation lacks ``Checks: write``. The PR-diff evaluator catches this and
    **falls back to the commit Status API** (the planned v1 path), rather than
    failing the job — distinct + actionable, mirroring
    :class:`GitHubWorkflowsPermissionError`.
    """


class GitHubRepoCreationPermissionError(Exception):
    """A repo-create call was refused with a non-rate-limit 403.

    ``POST /user/repos`` never accepts any GitHub App token (installation or
    user-to-server) — only classic OAuth-app tokens and PATs — so this always
    403s for a User-account installation. ``POST /orgs/{org}/repos`` needs the
    App's org-level ``Administration: write`` permission; a 403 there means the
    App is missing it. Either way this is distinct + actionable (not a generic
    API error, not a rate-limit), mirroring :class:`GitHubWorkflowsPermissionError`.
    """


class GitHubRepoCreationUnsupportedError(Exception):
    """Thought2Build can't auto-create this repo — raised without calling GitHub.

    GitHub gives GitHub Apps no way to create a repo in a personal (User)
    account, and no App-compatible way to add a freshly-created repo to a
    "selected repositories" installation (that's PAT-only). So a target repo
    that doesn't already exist is only reachable when the installation is an
    Organization scoped to "All repositories" — every other combination is a
    dead end the export flow should fail on immediately rather than attempt.
    """


# ---------------------------------------------------------------------------
# Token source + circuit breaker
# ---------------------------------------------------------------------------


class InstallationTokenSource(Protocol):
    """Structural type for the T-267 ``TokenProvider``.

    Decouples this module from ``github_app_auth`` (scope discipline) while
    documenting the surface the client depends on.
    """

    async def get(self, installation_id: int) -> str:
        pass

    async def refresh(self, installation_id: int) -> str:
        pass


class RateGovernor(Protocol):
    """Structural type for the T-274 per-installation rate governor.

    Decouples this module from ``github_governor`` (scope discipline). When a
    governor is supplied, ``acquire`` reserves budget before each request and
    ``observe`` reads the rate-limit headers off each response — either may raise
    ``GitHubThrottledError`` to requeue the worker job on backpressure.
    """

    async def acquire(self, *, write: bool) -> None:
        pass

    async def observe(self, response: httpx.Response) -> None:
        pass


class GitHubCircuitBreaker:
    """Per-client circuit breaker following the T-217 failure-window pattern.

    Counts failures that signal a degraded GitHub (network errors, 5xx, 429).
    When ``failure_threshold`` failures occur within ``window_seconds`` the
    breaker opens and rejects calls with :class:`GitHubUnavailableError` for
    ``cooldown_seconds``, after which it half-opens to allow one trial request;
    a success closes it, a failure re-opens it.

    Default scope is per-client-instance (one export job). Cross-installation
    persistence is the per-installation governor's job (T-274); this breaker
    stops a single job from hammering a degraded GitHub. A monotonic clock is
    used so wall-clock changes cannot wedge the breaker open.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        window_seconds: float = 60.0,
        cooldown_seconds: float = 30.0,
    ) -> None:
        self._threshold = failure_threshold
        self._window = window_seconds
        self._cooldown = cooldown_seconds
        self._failures = 0
        self._window_start = 0.0
        self._open_until: float | None = None
        self._half_open = False

    def before_request(self) -> None:
        """Raise GitHubUnavailableError if the breaker is open; else permit."""
        if self._open_until is None:
            return
        now = time.monotonic()
        if now < self._open_until:
            raise GitHubUnavailableError(
                "GitHub is temporarily unavailable (circuit breaker open)"
            )
        # Cooldown elapsed → allow a single half-open trial request.
        self._open_until = None
        self._half_open = True

    def record_success(self) -> None:
        self._failures = 0
        self._window_start = 0.0
        self._open_until = None
        self._half_open = False

    def record_failure(self) -> None:
        now = time.monotonic()
        if self._half_open:
            # The half-open trial failed — re-open immediately.
            self._half_open = False
            self._open_until = now + self._cooldown
            self._failures = 0
            return
        if self._window_start == 0.0 or now - self._window_start > self._window:
            self._window_start = now
            self._failures = 0
        self._failures += 1
        if self._failures >= self._threshold:
            self._open_until = now + self._cooldown
            self._failures = 0


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class GitHubAPIClient:
    """Async wrapper around the GitHub REST API.

    The wrapper accepts a shared ``httpx.AsyncClient`` so callers can reuse a
    connection pool across multiple operations (the export service makes O(N)
    issue creates per push). Exactly one auth source must be supplied: a static
    ``token`` (legacy OAuth) or a ``token_provider`` + ``installation_id``
    (GitHub App).
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        token: str | None = None,
        token_provider: InstallationTokenSource | None = None,
        installation_id: int | None = None,
        breaker: GitHubCircuitBreaker | None = None,
        governor: RateGovernor | None = None,
    ) -> None:
        if token is not None and token_provider is not None:
            raise ValueError(
                "GitHubAPIClient takes either a static token or a token_provider, "
                "not both"
            )
        if token_provider is not None:
            if installation_id is None:
                raise ValueError(
                    "GitHubAPIClient requires installation_id with a token_provider"
                )
        elif token is not None:
            if not token:
                raise ValueError("GitHubAPIClient requires a non-empty token")
        else:
            raise ValueError("GitHubAPIClient requires a token or a token_provider")
        self._token = token
        self._token_provider = token_provider
        self._installation_id = installation_id
        self._client = client
        self._breaker = breaker if breaker is not None else GitHubCircuitBreaker()
        self._governor = governor

    # ----- repo -----

    def _handle_repo_create_response(
        self, response: httpx.Response, name: str
    ) -> dict[str, Any]:
        if response.status_code == 201:
            return response.json()

        if response.status_code == 422 and _is_already_exists(response):
            raise GitHubRepoExistsError(
                f"Repository {name!r} already exists in your GitHub account"
            )

        if response.status_code == 403 and not _is_rate_limited(response):
            raise GitHubRepoCreationPermissionError(
                f"GitHub refused to create {name!r}: the App installation "
                "lacks the required repo-creation permission."
            )

        self._raise_for_status(response)
        # _raise_for_status always raises on non-2xx — this is unreachable.
        raise GitHubAPIError(response.status_code, response.text)

    async def create_repo(self, name: str, private: bool) -> dict[str, Any]:
        """POST /user/repos.

        Only classic OAuth-app tokens and PATs can call this — never a GitHub
        App token — so this is exclusively the legacy v1-OAuth code path.
        Returns the full repo JSON on success.
        """
        body = {
            "name": name,
            "private": private,
            "auto_init": False,
            "description": "Generated by Thought2Build",
        }
        response = await self._request("POST", "/user/repos", json=body)
        return self._handle_repo_create_response(response, name)

    async def create_org_repo(
        self, org: str, name: str, private: bool
    ) -> dict[str, Any]:
        """POST /orgs/{org}/repos — the App-compatible way to create an org repo.

        Requires the App to hold org-level ``Administration: write``; a 403
        without that permission raises :class:`GitHubRepoCreationPermissionError`.
        Returns the full repo JSON on success.
        """
        body = {
            "name": name,
            "private": private,
            "auto_init": False,
            "description": "Generated by Thought2Build",
        }
        response = await self._request("POST", f"/orgs/{org}/repos", json=body)
        return self._handle_repo_create_response(response, name)

    async def get_repo(self, owner_repo: str) -> dict[str, Any] | None:
        """GET /repos/{owner}/{repo} → the repo JSON, or ``None`` on a miss.

        Deliberately does NOT consult the response's ``permissions`` block:
        that block describes an authenticated *user*, and with an installation
        token GitHub fills it with all-false values even for repos the
        installation can freely push to (verified live against the dev App —
        filtering on it rejects every accessible repo). Whether this
        installation is actually *granted* the repo is the caller's job
        (``_resolve_existing_repo`` in the export service checks the
        installation's grant list for "selected repositories" installs; an
        "all repositories" install is granted everything under its account by
        construction).
        """
        response = await self._request("GET", f"/repos/{owner_repo}")
        if response.status_code != 200:
            return None
        data = response.json()
        if not isinstance(data, dict):
            return None
        return data

    async def list_installation_repositories(
        self, *, max_pages: int = 5
    ) -> tuple[list[dict[str, Any]], bool]:
        """GET /installation/repositories — repos this token can access.

        Unlike ``list_issues`` the body is not a bare array: rows sit under a
        ``repositories`` key next to ``total_count``. This runs on the
        interactive request path (the export modal's repo picker), so
        pagination is capped at ``max_pages`` × 100 rows; the second element of
        the returned tuple is True when GitHub holds more rows than were
        fetched. It is derived from ``total_count`` against the number of rows
        *fetched* (pre-filter), so an account with exactly ``max_pages`` × 100
        repos is not a false positive and local filtering below never inflates
        it.

        Rows the export could never push to — archived/disabled repos (GitHub
        rejects contents writes to them) — are dropped. The rows' ``permissions``
        block is deliberately ignored: it describes an authenticated *user*, and
        under an installation token GitHub returns it all-false even for repos
        the token can freely push to (verified live — filtering on it would drop
        every repo). Access control needs no filter here anyway: this endpoint
        only ever returns the installation's own grant list.
        """
        repos: list[dict[str, Any]] = []
        fetched = 0
        total_count: int | None = None
        for page in range(1, max_pages + 1):
            response = await self._request(
                "GET", f"/installation/repositories?per_page=100&page={page}"
            )
            if response.status_code != 200:
                self._raise_for_status(response)
            body = response.json()
            if not isinstance(body, dict):
                raise GitHubAPIError(
                    response.status_code,
                    "installation repositories: malformed response body",
                )
            raw_count = body.get("total_count")
            if isinstance(raw_count, int):
                total_count = raw_count
            batch = body.get("repositories")
            if not isinstance(batch, list) or not batch:
                break
            fetched += len(batch)
            for item in batch:
                if not isinstance(item, dict):
                    continue
                if item.get("archived") or item.get("disabled"):
                    continue
                repos.append(item)
            if len(batch) < 100:
                break
            if total_count is not None and fetched >= total_count:
                break
        if total_count is not None:
            truncated = total_count > fetched
        else:
            # Defensive fallback (GitHub always sends total_count): assume more
            # rows exist only when every allowed page came back full.
            truncated = fetched >= max_pages * 100
        return repos, truncated

    # ----- contents (files) -----

    async def get_file_content(
        self, repo: str, path: str, *, ref: str | None = None
    ) -> tuple[str, str] | None:
        """GET a file's decoded text + blob SHA, or ``None`` on 404 (T-277).

        Used by the non-clobbering ``AGENTS.md`` write to read the current file
        before regenerating its managed block. GitHub returns base64 with
        embedded newlines, so the payload is whitespace-stripped before decoding.
        """
        url = f"/repos/{repo}/contents/{_quote_path(path)}"
        if ref is not None:
            url += f"?ref={quote(ref, safe='')}"
        response = await self._request("GET", url)
        if response.status_code == 404:
            return None
        if response.status_code == 200:
            body = response.json()
            if not isinstance(body, dict):
                raise GitHubAPIError(
                    response.status_code, f"bad contents body for {path}"
                )
            sha = body.get("sha")
            encoded = body.get("content")
            if not isinstance(sha, str) or not isinstance(encoded, str):
                raise GitHubAPIError(
                    response.status_code, f"contents response missing fields for {path}"
                )
            text = base64.b64decode("".join(encoded.split())).decode("utf-8")
            return text, sha
        self._raise_for_status(response)
        return None  # unreachable

    async def get_file_sha(
        self, repo: str, path: str, *, ref: str | None = None
    ) -> str | None:
        """GET /repos/{owner}/{repo}/contents/{path}.

        Returns the blob SHA if the file exists, or ``None`` on 404. ``ref``
        (a branch/tag/SHA) reads the file on that branch — required by PR mode so
        the SHA matches the branch being written, not the default branch.
        """
        url = f"/repos/{repo}/contents/{_quote_path(path)}"
        if ref is not None:
            url += f"?ref={quote(ref, safe='')}"
        response = await self._request("GET", url)
        if response.status_code == 404:
            return None
        if response.status_code == 200:
            body = response.json()
            sha = body.get("sha") if isinstance(body, dict) else None
            if isinstance(sha, str):
                return sha
            raise GitHubAPIError(
                response.status_code,
                f"contents response missing sha for {path}",
            )
        self._raise_for_status(response)
        return None  # unreachable

    async def upsert_file(
        self,
        repo: str,
        path: str,
        content: str,
        sha: str | None,
        commit_message: str,
        *,
        branch: str | None = None,
    ) -> None:
        """PUT /repos/{owner}/{repo}/contents/{path}.

        When ``sha`` is provided this updates an existing file; otherwise it
        creates one. ``branch`` writes on a named branch (PR mode). The content
        is base64-encoded.

        Resilience (T-276):

        - A ``409`` (the blob SHA went stale under a concurrent write) is
          recovered by re-reading the current SHA **on the same branch** and
          retrying, bounded by :data:`_UPSERT_MAX_ATTEMPTS` so a persistent
          conflict cannot loop forever.
        - A ``403`` on a ``.github/workflows/*`` path means the App lacks the
          ``Workflows: write`` permission; it raises
          :class:`GitHubWorkflowsPermissionError` (a distinct, actionable error)
          rather than an opaque API error.
        """
        encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
        current_sha = sha
        for attempt in range(1, _UPSERT_MAX_ATTEMPTS + 1):
            body: dict[str, Any] = {
                "message": commit_message,
                "content": encoded,
            }
            if current_sha is not None:
                body["sha"] = current_sha
            if branch is not None:
                body["branch"] = branch

            response = await self._request(
                "PUT",
                f"/repos/{repo}/contents/{_quote_path(path)}",
                json=body,
            )
            if response.status_code in (200, 201):
                return

            # Workflows:write 403 on a CI-workflow path — distinct, actionable.
            if response.status_code == 403 and _is_workflow_path(path):
                raise GitHubWorkflowsPermissionError(
                    "The GitHub App lacks 'Workflows: write' permission required "
                    f"to push {path!r}. Re-approve the App's permissions on GitHub, "
                    "then re-export."
                )

            # Stale-SHA 409: refetch the SHA on the write branch and retry.
            if response.status_code == 409 and attempt < _UPSERT_MAX_ATTEMPTS:
                current_sha = await self.get_file_sha(repo, path, ref=branch)
                continue

            self._raise_for_status(response)
            return  # unreachable — _raise_for_status always raises on non-2xx

    async def delete_file(
        self,
        repo: str,
        path: str,
        sha: str,
        commit_message: str,
        *,
        branch: str | None = None,
    ) -> None:
        """Delete a file by SHA; absent files are success and 409s retry safely."""
        current_sha: str | None = sha
        for attempt in range(1, _UPSERT_MAX_ATTEMPTS + 1):
            if current_sha is None:
                return
            body: dict[str, Any] = {"message": commit_message, "sha": current_sha}
            if branch is not None:
                body["branch"] = branch
            response = await self._request(
                "DELETE",
                f"/repos/{repo}/contents/{_quote_path(path)}",
                json=body,
            )
            if response.status_code in (200, 204, 404):
                return
            if response.status_code == 409 and attempt < _UPSERT_MAX_ATTEMPTS:
                current_sha = await self.get_file_sha(repo, path, ref=branch)
                continue
            self._raise_for_status(response)
            return

    # ----- git refs / branches / pulls (PR mode, T-276) -----

    async def get_ref(self, repo: str, ref: str) -> str:
        """GET /repos/{owner}/{repo}/git/ref/{ref} → the referenced commit SHA.

        ``ref`` is unprefixed, e.g. ``"heads/main"``. Raises ``GitHubAPIError``
        if the ref does not exist (an empty repo has no default branch yet).
        """
        response = await self._request(
            "GET", f"/repos/{repo}/git/ref/{quote(ref, safe='/')}"
        )
        if response.status_code == 200:
            body = response.json()
            obj = body.get("object") if isinstance(body, dict) else None
            sha = obj.get("sha") if isinstance(obj, dict) else None
            if isinstance(sha, str):
                return sha
            raise GitHubAPIError(response.status_code, f"ref {ref} missing object.sha")
        self._raise_for_status(response)
        raise GitHubAPIError(response.status_code, response.text)  # unreachable

    async def create_branch(self, repo: str, branch: str, base_sha: str) -> None:
        """POST /repos/{owner}/{repo}/git/refs — create ``refs/heads/{branch}``.

        Idempotent: a ``422`` ("Reference already exists") on re-export is a
        no-op, so a resumed PR export reuses the same branch instead of failing.
        """
        response = await self._request(
            "POST",
            f"/repos/{repo}/git/refs",
            json={"ref": f"refs/heads/{branch}", "sha": base_sha},
        )
        if response.status_code == 201:
            return
        if response.status_code == 422 and _ref_already_exists(response):
            return  # branch already present — idempotent
        self._raise_for_status(response)

    async def create_pull_request(
        self, repo: str, *, head: str, base: str, title: str, body: str
    ) -> int:
        """POST /repos/{owner}/{repo}/pulls → the new PR number.

        Idempotent under resume: if a PR already exists for ``head`` (a crash
        between create and persisting ``pr_number``), GitHub 422s; we recover the
        existing PR number by listing open pulls for the head branch instead of
        opening a duplicate.
        """
        response = await self._request(
            "POST",
            f"/repos/{repo}/pulls",
            json={"title": title, "head": head, "base": base, "body": body},
        )
        if response.status_code == 201:
            data = response.json()
            number = data.get("number") if isinstance(data, dict) else None
            if isinstance(number, int):
                return number
            raise GitHubAPIError(
                response.status_code, "pull creation response missing 'number'"
            )
        if response.status_code == 422:
            existing = await self.find_open_pull_number(repo, head=head)
            if existing is not None:
                return existing
        self._raise_for_status(response)
        raise GitHubAPIError(response.status_code, response.text)  # unreachable

    async def find_open_pull_number(self, repo: str, *, head: str) -> int | None:
        """GET /repos/{owner}/{repo}/pulls?head=… → an open PR number, or None.

        ``head`` may be a bare branch name or ``owner:branch``; GitHub expects
        the latter, so we pass it through and also match the branch suffix.
        """
        response = await self._request(
            "GET",
            f"/repos/{repo}/pulls?state=open&head={quote(head, safe=':')}",
        )
        if response.status_code != 200:
            return None
        rows = response.json()
        if not isinstance(rows, list):
            return None
        branch = head.split(":", 1)[-1]
        for row in rows:
            if not isinstance(row, dict):
                continue
            ref = (row.get("head") or {}).get("ref") if isinstance(row, dict) else None
            number = row.get("number")
            if isinstance(number, int) and (ref == branch or ref == head):
                return number
        return None

    # ----- issues -----

    async def create_issue(
        self,
        repo: str,
        title: str,
        body: str,
        *,
        labels: tuple[str, ...] | list[str] | None = None,
        milestone: int | None = None,
    ) -> int:
        """POST /repos/{owner}/{repo}/issues. Returns the new issue number.

        ``labels`` (T-277) are attached on creation; GitHub auto-creates any
        label that does not yet exist on the repo, so no separate ensure step is
        needed. ``milestone`` (T-280) is the milestone *number* a new increment
        issue is filed under so the increment's work is grouped on GitHub.
        """
        payload: dict[str, Any] = {"title": title, "body": body}
        if labels:
            payload["labels"] = list(labels)
        if milestone is not None:
            payload["milestone"] = milestone
        response = await self._request(
            "POST",
            f"/repos/{repo}/issues",
            json=payload,
        )
        if response.status_code == 201:
            data = response.json()
            number = data.get("number") if isinstance(data, dict) else None
            if isinstance(number, int):
                return number
            raise GitHubAPIError(
                response.status_code,
                "issue creation succeeded but response missing 'number'",
            )

        self._raise_for_status(response)
        raise GitHubAPIError(response.status_code, response.text)  # unreachable

    async def update_issue(
        self,
        repo: str,
        number: int,
        title: str,
        body: str,
        *,
        labels: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        """PATCH /repos/{owner}/{repo}/issues/{number}."""
        payload: dict[str, Any] = {"title": title, "body": body}
        if labels:
            payload["labels"] = list(labels)
        response = await self._request(
            "PATCH",
            f"/repos/{repo}/issues/{number}",
            json=payload,
        )
        if response.status_code == 200:
            return
        self._raise_for_status(response)

    async def list_issues(
        self,
        repo: str,
        *,
        state: str = "all",
        since: str | None = None,
        max_pages: int = ISSUES_MAX_PAGES,
    ) -> list[dict[str, Any]]:
        """GET /repos/{owner}/{repo}/issues — paginated, for backfill (T-273).

        Returns raw issue rows. NOTE: GitHub's issues endpoint also returns pull
        requests (each carries a ``pull_request`` key); the caller must filter
        those out. ``since`` is an ISO-8601 timestamp to fetch only rows updated
        after the last sync. Pagination is bounded by ``max_pages`` as a safety
        cap.

        The cap is SILENT — a truncated result is indistinguishable from a
        complete one by inspecting the list alone. A caller that advances a
        durable cursor on the strength of this call must therefore compare its
        result length against :data:`ISSUES_FETCH_CAP` and resume from the last
        row it actually saw, or it will skip everything past the cap forever.
        """
        issues: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            query: dict[str, str | int] = {
                "state": state,
                "per_page": ISSUES_PAGE_SIZE,
                "page": page,
                "sort": "updated",
                "direction": "asc",
            }
            if since:
                query["since"] = since
            # Never interpolate timestamps directly into a query string. A UTC
            # offset contains '+', which form-style query decoding turns into a
            # space; GitHub then returns an empty page and silently misses closed
            # issues. urlencode preserves the exact cursor as `%2B00%3A00`.
            path = f"/repos/{repo}/issues?{urlencode(query)}"
            response = await self._request("GET", path)
            if response.status_code != 200:
                self._raise_for_status(response)
            batch = response.json()
            if not isinstance(batch, list) or not batch:
                break
            issues.extend(batch)
            if len(batch) < ISSUES_PAGE_SIZE:
                break
        return issues

    async def get_issue_state(self, repo: str, number: int) -> str | None:
        """GET /repos/{owner}/{repo}/issues/{number} → ``"open"``/``"closed"``.

        Used by incremental sync (T-280) to read live state before closing an
        obsoleted issue, so a re-run never re-comments on an issue it already
        closed. Returns ``None`` if the issue cannot be read.
        """
        response = await self._request("GET", f"/repos/{repo}/issues/{number}")
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            self._raise_for_status(response)
        data = response.json()
        state = data.get("state") if isinstance(data, dict) else None
        return state if isinstance(state, str) else None

    async def add_issue_comment(self, repo: str, number: int, body: str) -> None:
        """POST /repos/{owner}/{repo}/issues/{number}/comments."""
        response = await self._request(
            "POST",
            f"/repos/{repo}/issues/{number}/comments",
            json={"body": body},
        )
        if response.status_code in (200, 201):
            return
        self._raise_for_status(response)

    async def close_issue(self, repo: str, number: int) -> None:
        """PATCH an issue to ``state="closed"`` (T-280, obsoleted tasks).

        A close is reversible (the issue and its history remain), unlike a delete.
        Re-closing an already-closed issue is a harmless no-op PATCH.
        """
        response = await self._request(
            "PATCH",
            f"/repos/{repo}/issues/{number}",
            json={"state": "closed"},
        )
        if response.status_code == 200:
            return
        self._raise_for_status(response)

    async def get_issue_node_id(self, repo: str, number: int) -> str | None:
        """GET an issue's GraphQL ``node_id`` (T-281).

        Projects v2 items reference their content by GraphQL node id, not the
        REST issue number, so the board sync resolves each task issue's node id
        before adding it to the board. Returns ``None`` if the issue cannot be
        read.
        """
        response = await self._request("GET", f"/repos/{repo}/issues/{number}")
        if response.status_code != 200:
            return None
        data = response.json()
        node_id = data.get("node_id") if isinstance(data, dict) else None
        return node_id if isinstance(node_id, str) else None

    async def set_issue_milestone(
        self, repo: str, number: int, milestone: int | None
    ) -> None:
        """PATCH an issue's milestone (T-281). ``None`` clears the assignment.

        Used by the board sync to file each task issue under its phase milestone.
        Re-assigning the same milestone is a harmless idempotent no-op PATCH.
        """
        response = await self._request(
            "PATCH",
            f"/repos/{repo}/issues/{number}",
            json={"milestone": milestone},
        )
        if response.status_code == 200:
            return
        self._raise_for_status(response)

    async def ensure_milestone(
        self, repo: str, title: str, *, description: str | None = None
    ) -> int | None:
        """Return the number of the milestone titled ``title``, creating it once.

        Idempotent (T-280): an increment push reads live milestones first and
        reuses a matching title rather than opening a duplicate, so re-running a
        push files new issues under the same milestone. Returns ``None`` only if
        GitHub neither lists nor accepts the milestone (best-effort — a missing
        milestone never blocks the issue work).
        """
        existing = await self._find_milestone_number(repo, title)
        if existing is not None:
            return existing
        payload: dict[str, Any] = {"title": title}
        if description:
            payload["description"] = description
        response = await self._request(
            "POST", f"/repos/{repo}/milestones", json=payload
        )
        if response.status_code == 201:
            data = response.json()
            number = data.get("number") if isinstance(data, dict) else None
            if isinstance(number, int):
                return number
        if response.status_code == 422:
            # Already exists (a concurrent create or a title-uniqueness clash) —
            # recover its number from the live list rather than failing.
            return await self._find_milestone_number(repo, title)
        return None

    async def _find_milestone_number(self, repo: str, title: str) -> int | None:
        """Locate an existing milestone by exact title, or ``None``."""
        for page in range(1, 11):  # bounded; a repo rarely has >1000 milestones
            response = await self._request(
                "GET",
                f"/repos/{repo}/milestones?state=all&per_page=100&page={page}",
            )
            if response.status_code != 200:
                return None
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                return None
            for row in rows:
                if isinstance(row, dict) and row.get("title") == title:
                    number = row.get("number")
                    if isinstance(number, int):
                        return number
            if len(rows) < 100:
                return None
        return None

    # ----- GraphQL / Projects v2 (T-281) -----

    async def graphql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """POST a GraphQL query/mutation to ``/graphql`` and return ``data``.

        Projects v2 is GraphQL-only, so this is the single low-level entry point
        for the board sync. It goes through the same per-call token resolution,
        governor budgeting, and circuit breaker as the REST path.

        GitHub's GraphQL endpoint answers **HTTP 200 even on failure**, carrying
        the failure in a top-level ``errors`` array, so a 200 is not success on
        its own: a non-empty ``errors`` raises
        :class:`GitHubProjectsPermissionError` when GitHub reports the App lacks
        Projects access, else :class:`GitHubGraphQLError`.
        """
        body: dict[str, Any] = {"query": query}
        if variables is not None:
            body["variables"] = variables
        response = await self._request("POST", "/graphql", json=body)
        if response.status_code != 200:
            self._raise_for_status(response)
        payload = response.json()
        if not isinstance(payload, dict):
            raise GitHubGraphQLError("GraphQL response was not a JSON object")
        errors = payload.get("errors")
        if errors:
            message = _graphql_error_message(errors)
            if _graphql_errors_forbidden(errors):
                raise GitHubProjectsPermissionError(
                    "The GitHub App lacks the 'Projects' permission required to "
                    "manage the Projects v2 board. Re-approve the App's "
                    f"permissions on GitHub, then re-sync. ({message})"
                )
            raise GitHubGraphQLError(f"GraphQL error: {message}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise GitHubGraphQLError("GraphQL response missing 'data'")
        return data

    async def get_repo_node_ids(self, repo: str) -> tuple[str, str] | None:
        """Return ``(repository_node_id, owner_node_id)`` for ``owner/name``.

        The owner node id is required to create/look up a Projects v2 board (a
        board is owned by a user or org, not a repo); the repository node id
        links a freshly created board back to the repo. ``None`` if the
        repository node cannot be resolved.
        """
        owner, _, name = repo.partition("/")
        data = await self.graphql(
            "query($owner:String!,$name:String!){"
            "repository(owner:$owner,name:$name){id owner{id}}}",
            {"owner": owner, "name": name},
        )
        repository = data.get("repository")
        if not isinstance(repository, dict):
            return None
        repo_id = repository.get("id")
        owner_obj = repository.get("owner")
        owner_id = owner_obj.get("id") if isinstance(owner_obj, dict) else None
        if isinstance(repo_id, str) and isinstance(owner_id, str):
            return repo_id, owner_id
        return None

    async def ensure_project_v2(
        self, owner_id: str, title: str, *, repository_id: str | None = None
    ) -> str | None:
        """Return the node id of the owner's Projects v2 board titled ``title``.

        Idempotent (mirrors :meth:`ensure_milestone`): ``createProjectV2`` is
        **not** idempotent — a naive re-run would spawn a second board — so the
        live boards are listed first and a matching title is reused; only a
        genuine miss creates one. A freshly created board is linked to
        ``repository_id`` when supplied. Returns ``None`` if GitHub neither lists
        nor accepts the board.
        """
        existing = await self._find_project_v2_id(owner_id, title)
        if existing is not None:
            return existing
        input_fields: dict[str, Any] = {"ownerId": owner_id, "title": title}
        if repository_id is not None:
            input_fields["repositoryId"] = repository_id
        data = await self.graphql(
            "mutation($input:CreateProjectV2Input!){"
            "createProjectV2(input:$input){projectV2{id}}}",
            {"input": input_fields},
        )
        created = (data.get("createProjectV2") or {}).get("projectV2")
        node_id = created.get("id") if isinstance(created, dict) else None
        return node_id if isinstance(node_id, str) else None

    async def _find_project_v2_id(self, owner_id: str, title: str) -> str | None:
        """Locate an owner's Projects v2 board by exact title, or ``None``."""
        cursor: str | None = None
        for _ in range(1, 21):  # bounded; an owner rarely has >2000 boards
            data = await self.graphql(
                "query($owner:ID!,$cursor:String){node(id:$owner){"
                "... on ProjectV2Owner{projectsV2(first:100,after:$cursor){"
                "nodes{id title} pageInfo{hasNextPage endCursor}}}}}",
                {"owner": owner_id, "cursor": cursor},
            )
            node = data.get("node")
            projects = node.get("projectsV2") if isinstance(node, dict) else None
            if not isinstance(projects, dict):
                return None
            for row in projects.get("nodes") or []:
                if isinstance(row, dict) and row.get("title") == title:
                    pid = row.get("id")
                    if isinstance(pid, str):
                        return pid
            page = projects.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                return None
            cursor = page.get("endCursor")
            if not isinstance(cursor, str):
                return None
        return None

    async def get_project_v2_status_field(
        self, project_id: str
    ) -> tuple[str, dict[str, str]] | None:
        """Return the board's ``Status`` single-select field id + its options.

        Returns ``(field_id, {option_name: option_id})``. GitHub auto-creates a
        ``Status`` field (Todo / In Progress / Done) on every new board; the
        board sync maps a task's ``open``/``done`` state onto these options.
        ``None`` if the board has no single-select ``Status`` field.
        """
        data = await self.graphql(
            "query($project:ID!){node(id:$project){... on ProjectV2{"
            'field(name:"Status"){... on ProjectV2SingleSelectField{'
            "id options{id name}}}}}}",
            {"project": project_id},
        )
        node = data.get("node")
        field = node.get("field") if isinstance(node, dict) else None
        if not isinstance(field, dict):
            return None
        field_id = field.get("id")
        if not isinstance(field_id, str):
            return None
        options: dict[str, str] = {}
        for opt in field.get("options") or []:
            if isinstance(opt, dict):
                name = opt.get("name")
                opt_id = opt.get("id")
                if isinstance(name, str) and isinstance(opt_id, str):
                    options[name] = opt_id
        return field_id, options

    async def add_project_v2_item(self, project_id: str, content_id: str) -> str | None:
        """Add an issue (by node id) to a board; return the project item id.

        Server-idempotent: ``addProjectV2ItemById`` returns the *existing* item
        if the content is already on the board, so re-running the sync never
        duplicates a card. ``None`` if GitHub does not return an item id.
        """
        data = await self.graphql(
            "mutation($project:ID!,$content:ID!){addProjectV2ItemById("
            "input:{projectId:$project,contentId:$content}){item{id}}}",
            {"project": project_id, "content": content_id},
        )
        item = (data.get("addProjectV2ItemById") or {}).get("item")
        item_id = item.get("id") if isinstance(item, dict) else None
        return item_id if isinstance(item_id, str) else None

    async def set_project_v2_item_status(
        self, project_id: str, item_id: str, field_id: str, option_id: str
    ) -> None:
        """Set a board item's single-select ``Status`` to ``option_id`` (column).

        Idempotent: setting the value to its current option is a harmless no-op.
        """
        await self.graphql(
            "mutation($project:ID!,$item:ID!,$field:ID!,$option:String!){"
            "updateProjectV2ItemFieldValue(input:{projectId:$project,"
            "itemId:$item,fieldId:$field,value:{singleSelectOptionId:$option}})"
            "{projectV2Item{id}}}",
            {
                "project": project_id,
                "item": item_id,
                "field": field_id,
                "option": option_id,
            },
        )

    # ----- pull requests / status checks (T-282) -----

    async def get_pull_request(self, repo: str, number: int) -> dict[str, Any] | None:
        """GET /repos/{owner}/{repo}/pulls/{number} → the PR JSON, or ``None``.

        The PR-diff evaluator needs the head commit SHA (check runs attach to a
        SHA), the head ``ref`` (branch fallback), and the body (``Closes #N``
        task linkage). ``None`` if the PR cannot be read.
        """
        response = await self._request("GET", f"/repos/{repo}/pulls/{number}")
        if response.status_code != 200:
            return None
        data = response.json()
        return data if isinstance(data, dict) else None

    async def get_pull_request_diff(self, repo: str, number: int) -> str | None:
        """GET the PR's unified diff via the ``diff`` media type (T-282).

        Returns the raw diff text (untrusted — the caller bounds + wraps it before
        prompting), or ``None`` if it cannot be read.
        """
        response = await self._request(
            "GET",
            f"/repos/{repo}/pulls/{number}",
            accept="application/vnd.github.v3.diff",
        )
        if response.status_code != 200:
            return None
        return response.text

    async def post_check_run(
        self,
        repo: str,
        *,
        name: str,
        head_sha: str,
        status: str,
        conclusion: str | None = None,
        title: str,
        summary: str,
    ) -> int | None:
        """POST /repos/{owner}/{repo}/check-runs → the new check run id (T-282).

        Needs ``Checks: write``. A non-rate-limit ``403`` raises
        :class:`GitHubChecksPermissionError` so the evaluator falls back to the
        commit Status API. ``status`` is ``queued``/``in_progress``/``completed``;
        ``conclusion`` (``success``/``failure``/``neutral``) is set only when
        completed.
        """
        return await self._write_check_run(
            "POST",
            f"/repos/{repo}/check-runs",
            name=name,
            head_sha=head_sha,
            status=status,
            conclusion=conclusion,
            title=title,
            summary=summary,
        )

    async def update_check_run(
        self,
        repo: str,
        check_run_id: int,
        *,
        status: str,
        conclusion: str | None = None,
        title: str,
        summary: str,
    ) -> int | None:
        """PATCH a check run to a new status/conclusion (T-282).

        Used to move the "pending" check posted up front to its completed verdict
        ("post pending, then update").
        """
        return await self._write_check_run(
            "PATCH",
            f"/repos/{repo}/check-runs/{check_run_id}",
            status=status,
            conclusion=conclusion,
            title=title,
            summary=summary,
        )

    async def _write_check_run(
        self,
        method: str,
        path: str,
        *,
        name: str | None = None,
        head_sha: str | None = None,
        status: str,
        conclusion: str | None,
        title: str,
        summary: str,
    ) -> int | None:
        payload: dict[str, Any] = {
            "status": status,
            "output": {"title": title, "summary": summary},
        }
        if name is not None:
            payload["name"] = name
        if head_sha is not None:
            payload["head_sha"] = head_sha
        if conclusion is not None:
            payload["conclusion"] = conclusion
        response = await self._request(method, path, json=payload)
        if response.status_code in (200, 201):
            data = response.json()
            run_id = data.get("id") if isinstance(data, dict) else None
            return run_id if isinstance(run_id, int) else None
        if response.status_code == 403 and not _is_rate_limited(response):
            raise GitHubChecksPermissionError(
                "The GitHub App lacks 'Checks: write'; falling back to the commit "
                "Status API."
            )
        self._raise_for_status(response)
        return None  # unreachable

    async def post_commit_status(
        self,
        repo: str,
        sha: str,
        *,
        state: str,
        context: str,
        description: str,
    ) -> None:
        """POST /repos/{owner}/{repo}/statuses/{sha} (T-282 v1 fallback).

        The commit Status API is the simpler v1 of the Thought2Build check, used when
        ``Checks: write`` is unavailable. ``state`` is ``success``/``failure``/
        ``pending``/``error`` (the Status API has no ``neutral`` — the evaluator
        maps a fail-open neutral to ``success`` so it never red-lights a PR).
        """
        response = await self._request(
            "POST",
            f"/repos/{repo}/statuses/{quote(sha, safe='')}",
            json={
                "state": state,
                "context": context,
                "description": description[:140],
            },
        )
        if response.status_code in (200, 201):
            return
        self._raise_for_status(response)

    # ----- internals -----

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        accept: str | None = None,
    ) -> httpx.Response:
        """Send a request, resolving the token per call and applying the breaker.

        A 401 in installation mode re-mints the token once and retries; a second
        401 (or any 401 in static mode) raises ``GitHubTokenExpiredError``.
        ``accept`` overrides the default JSON media type (e.g. the diff media type
        for a PR diff).
        """
        return await self._send(
            method, path, json=json, allow_remint=True, accept=accept
        )

    async def _send(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None,
        allow_remint: bool,
        accept: str | None = None,
    ) -> httpx.Response:
        # Breaker first: a tripped breaker rejects before any token resolution
        # or network call, so a degraded GitHub is not hammered.
        self._breaker.before_request()

        # Governor budget reservation (T-274): consume a primary token for every
        # request and a secondary token for content-creating writes. Exhaustion
        # raises GitHubThrottledError → the worker requeues the job (no token is
        # spent and no GitHub call is made).
        if self._governor is not None:
            await self._governor.acquire(write=_is_write_method(method))

        token = await self._resolve_token()
        url = f"{GITHUB_API_BASE}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": accept or _GITHUB_ACCEPT,
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "Thought2Build/1.0",
        }
        try:
            response = await self._client.request(
                method,
                url,
                headers=headers,
                json=json,
                timeout=_REQUEST_TIMEOUT,
            )
        except httpx.HTTPError as exc:
            # Network/timeout failure counts toward the breaker. The exception
            # class name only — never an interpolated untrusted string.
            self._breaker.record_failure()
            raise GitHubAPIError(0, f"network error: {type(exc).__name__}") from exc

        # 401 is mapped here before anything else so callers cannot accidentally
        # treat it as a generic API error. A 401 means GitHub is up (auth issue,
        # not downtime), so it is a breaker success.
        if response.status_code == 401:
            self._breaker.record_success()
            if allow_remint and self._token_provider is not None:
                await self._token_provider.refresh(self._installation_id)  # type: ignore[arg-type]
                return await self._send(
                    method, path, json=json, allow_remint=False, accept=accept
                )
            raise GitHubTokenExpiredError(
                "GitHub returned 401; the token is no longer valid"
            )

        # Governor observation (T-274) runs BEFORE breaker accounting so a plain
        # rate-limit 429/403 requeues the job as backpressure rather than tripping
        # the breaker into a false "GitHub unavailable" state. It realigns the
        # budget to GitHub's reported remaining on a healthy response and raises
        # GitHubThrottledError on a rate-limit response.
        if self._governor is not None:
            await self._governor.observe(response)

        # Degraded-GitHub signals trip the breaker; the response is still
        # returned so the calling method maps it to its typed error.
        if response.status_code >= 500 or response.status_code == 429:
            self._breaker.record_failure()
        else:
            self._breaker.record_success()
        return response

    async def _resolve_token(self) -> str:
        """Resolve the bearer token for this request (per-call in App mode)."""
        if self._token_provider is not None:
            return await self._token_provider.get(self._installation_id)  # type: ignore[arg-type]
        assert self._token is not None  # guaranteed by __init__  # nosec B101
        return self._token

    def _raise_for_status(self, response: httpx.Response) -> None:
        status = response.status_code
        if status < 400:
            return

        # 401 is already short-circuited in _request, but guard here too
        # for paranoia in case a subclass bypasses _request.
        if status == 401:
            raise GitHubTokenExpiredError(
                "GitHub returned 401; the stored token is no longer valid"
            )
        if status == 403 and _is_rate_limited(response):
            raise GitHubRateLimitError("GitHub API rate limit reached")
        if status == 429:
            raise GitHubRateLimitError("GitHub API rate limit reached")

        message = _short_error_message(response)
        raise GitHubAPIError(status, message)


def make_github_client(token: str, client: httpx.AsyncClient) -> GitHubAPIClient:
    """Factory for the legacy static-token (v1 OAuth) path.

    Tests inject a custom ``httpx.AsyncClient`` (often with a ``MockTransport``)
    via this entry point so the production call sites can build clients
    against the real ``httpx.AsyncClient`` without modification.
    """
    return GitHubAPIClient(token=token, client=client)


def make_app_github_client(
    token_provider: InstallationTokenSource,
    installation_id: int,
    client: httpx.AsyncClient,
    *,
    breaker: GitHubCircuitBreaker | None = None,
    governor: RateGovernor | None = None,
) -> GitHubAPIClient:
    """Factory for the GitHub App path: per-call installation tokens (T-268).

    The worker (T-269+) builds one client per export job; the token is resolved
    fresh on each request from ``token_provider`` and re-minted once on a 401.
    When a per-installation ``governor`` (T-274) is supplied, every call is
    budgeted and every response is observed for rate-limit headers.
    """
    return GitHubAPIClient(
        client=client,
        token_provider=token_provider,
        installation_id=installation_id,
        breaker=breaker,
        governor=governor,
    )


def make_shared_async_client() -> httpx.AsyncClient:
    """Build the shared httpx client for GitHub calls.

    Bounded timeouts and a bounded connection pool (spec §12 robustness) so a
    slow or degraded GitHub cannot stall the worker or exhaust sockets.
    Production call sites reuse one client across many operations; tests inject
    their own ``MockTransport`` client instead.
    """
    return httpx.AsyncClient(timeout=_REQUEST_TIMEOUT, limits=_CONNECTION_LIMITS)


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------


def _is_write_method(method: str) -> bool:
    """True for content-creating verbs that count against the secondary limit."""
    return method.upper() in {"POST", "PUT", "PATCH", "DELETE"}


def _is_workflow_path(path: str) -> bool:
    """True for a CI-workflow file requiring the App's Workflows:write scope."""
    return path.lstrip("/").startswith(".github/workflows/")


def _ref_already_exists(response: httpx.Response) -> bool:
    """True if a 422 from create-ref means the branch already exists."""
    try:
        body = response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    message = (body.get("message") or "").lower()
    return "already exists" in message or "reference already exists" in message


def _is_already_exists(response: httpx.Response) -> bool:
    """Return True if a 422 response indicates a name collision."""
    try:
        body = response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    errors = body.get("errors") or []
    if isinstance(errors, list):
        for err in errors:
            if isinstance(err, dict):
                msg = (err.get("message") or "").lower()
                if "already exists" in msg or err.get("code") == "already_exists":
                    return True
    message = (body.get("message") or "").lower()
    return "already exists" in message or "name already exists" in message


def _is_rate_limited(response: httpx.Response) -> bool:
    """Distinguish a permission-denied 403 from a rate-limit 403."""
    remaining = response.headers.get("x-ratelimit-remaining")
    if remaining == "0":
        return True
    try:
        body = response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    message = (body.get("message") or "").lower()
    return "rate limit" in message or "abuse" in message or "secondary rate" in message


def _short_error_message(response: httpx.Response) -> str:
    """Extract a concise error message from a GitHub response body."""
    try:
        body = response.json()
    except ValueError:
        return (response.text or "")[:200]
    if isinstance(body, dict):
        message = body.get("message")
        if isinstance(message, str):
            return message[:200]
    return (response.text or "")[:200]


def _graphql_error_message(errors: Any) -> str:
    """Concatenate a bounded, human-readable summary of a GraphQL errors array."""
    messages: list[str] = []
    if isinstance(errors, list):
        for err in errors:
            if isinstance(err, dict):
                msg = err.get("message")
                if isinstance(msg, str):
                    messages.append(msg)
    return ("; ".join(messages) or "unknown GraphQL error")[:200]


def _graphql_errors_forbidden(errors: Any) -> bool:
    """True if a GraphQL errors array signals a missing App permission/scope.

    GitHub reports an insufficient App permission as a ``FORBIDDEN`` error type
    or a "resource not accessible by integration" message, distinct from a query
    that merely returned no data.
    """
    if not isinstance(errors, list):
        return False
    for err in errors:
        if not isinstance(err, dict):
            continue
        if err.get("type") == "FORBIDDEN":
            return True
        message = (err.get("message") or "").lower()
        if "not accessible by integration" in message or "must have" in message:
            return True
    return False


def _quote_path(path: str) -> str:
    """GitHub Contents API accepts paths containing slashes verbatim.

    We strip any leading slash and forbid traversal segments — callers
    should pass repo-relative paths.
    """
    cleaned = path.lstrip("/")
    if ".." in cleaned.split("/"):
        raise ValueError(f"invalid path with traversal segment: {path!r}")
    return cleaned
