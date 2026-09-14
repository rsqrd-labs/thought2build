"""GitHub export orchestrator.

Drives the full export sequence: validates the user's GitHub connection,
creates or reuses a repo, pushes all four stage files (same layout as the
ZIP export), and creates one GitHub Issue per T-NNN task.

All push rows — App and legacy — use the single canonical status vocabulary
``pending``/``completed``/``failed``/``stale`` (audit #4). The retired legacy
words (``in_progress``/``success``/``error``) are gone so a completed push always
serialises through ``IntegrationPushRead`` and ``find_live_push`` /
``reconcile_drift`` never misread a legacy value.

State machine:

    [no push row]                   [push row exists]
            \\                          /
             v                        v
        status="pending" (push row is the lock — commit before any GitHub call)
                       |
        if repo_full_name is None: create_repo
                       |
              push files (idempotent: get_file_sha → upsert_file)
                       |
              create/update issues sequentially
                       |
        status="completed" + pushed_at + integration.last_used_at

On ``GitHubTokenExpiredError``:
  1. Delete UserIntegration in its own commit (a single-transaction wrap
     would lose the invalidation if the push update fails for any reason).
  2. Mark push as ``status="failed"``.
  3. Re-raise.

On any other exception after the push row is committed:
  - Mark push as ``status="failed"`` and re-raise. Re-export resumes from
    the same row — partial state (files pushed, issues partially created)
    is preserved because the IntegrationPushTask rows are committed one
    per issue.

Idempotent re-export:
  - Existing IntegrationPush row is reused — never creates a duplicate repo.
  - Files: ``get_file_sha`` returns the existing blob SHA; ``upsert_file``
    sends a PUT with the SHA which GitHub treats as an update.
  - Issues: existing IntegrationPushTask rows map T-NNN refs to issue
    numbers; we ``update_issue`` for those and ``create_issue`` for any
    new tasks added since the last export.
"""

from __future__ import annotations

import json
import logging
import re
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_shared_redis
from models import (
    GitHubInstallation,
    IntegrationPush,
    IntegrationPushTask,
    Stage,
    StageVersion,
    UserIntegration,
    Workspace,
)
from services.integrations import agents_md_builder, github_projects, pr_export_builder
from services.integrations.github_api_client import (
    GitHubAPIClient,
    GitHubAPIError,
    GitHubNotConnectedError,
    GitHubRateLimitError,
    GitHubRepoCreationPermissionError,
    GitHubRepoCreationUnsupportedError,
    GitHubRepoExistsError,
    GitHubTokenExpiredError,
    GitHubUnavailableError,
    GitHubWorkflowsPermissionError,
    make_app_github_client,
    make_github_client,
    make_shared_async_client,
)
from services.integrations.github_app_auth import make_token_provider
from services.integrations.github_governor import (
    GitHubThrottledError,
    InstallationRateGovernor,
    make_governor,
)
from services.integrations.task_issue_reconcile import retire_obsolete_task_issues
from services.integrations.task_parser import ParsedTask, compute_task_ref, parse_tasks
from services.integrations.task_ref_migration import migrate_legacy_task_refs
from services.observability import (
    GITHUB_AUDIT_EXPORT_COMPLETED,
    GITHUB_AUDIT_PR_OPENED,
    GITHUB_EXPORT_TOTAL,
    GITHUB_PR_TOTAL,
    github_audit,
)
from services.pipeline import agent_manual_service, construction_verdict_service
from services.pipeline.export_service import (
    ExportNotReadyError,
    _redact_stage_content,
    parse_harness_files,
)
from services.security import key_vault

logger = logging.getLogger(__name__)

GITHUB_PROVIDER = "github"
_STAGE_FILES = {"spec": "SPEC.md", "plan": "PLAN.md", "tasks": "TASKS.md"}
_HTTP_TIMEOUT_SECONDS = 30.0

# PR-mode (T-276) constants. A new repo is created with no commits, so its
# default branch only exists after the first push; ``create_repo`` makes a
# repo whose default branch is ``main``. The increment branch name is
# deterministic so a resumed export reuses the same branch (create → 422 no-op)
# instead of opening a second PR.
_DEFAULT_BRANCH = "main"
_INCREMENT_BRANCH = "thought2build/inc-1"

# Errors where retrying the identical job cannot change the outcome — the
# repo name is taken, the App is missing a permission, the token is invalid
# even after one re-mint, or the workspace's stages aren't finalised. These
# always mark the push 'failed' immediately, regardless of how many attempts
# the worker has left. Contrast with GitHubAPIError/GitHubRateLimitError/any
# other exception, which may be a transient blip that a retry resolves, and
# with GitHubThrottledError/GitHubUnavailableError (backpressure — handled
# separately, never marked failed here).
_PERMANENT_EXPORT_ERRORS: tuple[type[Exception], ...] = (
    GitHubTokenExpiredError,
    GitHubRepoExistsError,
    GitHubWorkflowsPermissionError,
    GitHubRepoCreationPermissionError,
    GitHubRepoCreationUnsupportedError,
    ExportNotReadyError,
)

# Type alias for a GitHubAPIClient factory. Tests inject a stub here so they
# do not need to construct an httpx.AsyncClient.
ClientFactory = type[GitHubAPIClient]


async def push_to_github(
    workspace_id: UUID,
    user_id: UUID,
    repo_name: str,
    visibility: str,
    db: AsyncSession,
    *,
    client_factory: object = None,
) -> IntegrationPush:
    """Push a workspace to a GitHub repo, creating issues per task.

    Parameters mirror the T-153 router signature. ``client_factory`` is an
    optional override used by tests to inject a stub GitHub client — the
    production code path passes ``None`` and a real httpx client is built.

    Returns the IntegrationPush row in ``status="completed"`` on completion.
    Raises:
        GitHubNotConnectedError: user has no GitHub integration row.
        ExportNotReadyError: workspace or any stage not finalised.
        GitHubRepoExistsError: first-time export and the repo name is taken.
        GitHubTokenExpiredError: any 401 from GitHub. The UserIntegration
            row is deleted before re-raising.
        GitHubRateLimitError: any 429 or rate-limited 403.
        GitHubAPIError: any other non-2xx GitHub response.
    """
    # 1. Pre-flight validation (do this before creating the push row so a
    #    failed precondition does not leave orphan "in_progress" rows).
    workspace = await _load_workspace(db, workspace_id, user_id)
    stages = await _load_finalised_stages(db, workspace_id)

    integration = await _load_integration(db, user_id)
    if integration is None:
        raise GitHubNotConnectedError("GitHub integration not connected for this user")

    plaintext_token = key_vault.decrypt(integration.encrypted_token)

    # 2. Upsert the push row — this is the durable lock against duplicate
    #    repo creation. Commit before any GitHub call.
    push = await _upsert_push_row(db, workspace_id, user_id)

    # 3. Drive the actual export with a typed-exception boundary.
    if client_factory is None:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as http:
            client = make_github_client(plaintext_token, http)
            try:
                return await _run_export(
                    db=db,
                    workspace=workspace,
                    stages=stages,
                    integration=integration,
                    push=push,
                    client=client,
                    repo_name=repo_name,
                    visibility=visibility,
                )
            except GitHubTokenExpiredError:
                await _handle_token_expired(db, user_id, push)
                raise
            except (
                GitHubRepoExistsError,
                GitHubRateLimitError,
                GitHubAPIError,
                ExportNotReadyError,
                Exception,
            ):
                await _mark_push_failed(db, push)
                raise
    else:
        # Test path: caller provides a ready-made GitHubAPIClient.
        client = client_factory  # type: ignore[assignment]
        try:
            return await _run_export(
                db=db,
                workspace=workspace,
                stages=stages,
                integration=integration,
                push=push,
                client=client,
                repo_name=repo_name,
                visibility=visibility,
            )
        except GitHubTokenExpiredError:
            await _handle_token_expired(db, user_id, push)
            raise
        except (
            GitHubRepoExistsError,
            GitHubRateLimitError,
            GitHubAPIError,
            ExportNotReadyError,
            Exception,
        ):
            await _mark_push_failed(db, push)
            raise


# ---------------------------------------------------------------------------
# Public lookup
# ---------------------------------------------------------------------------


async def get_push(
    workspace_id: UUID,
    user_id: UUID,
    db: AsyncSession,
) -> IntegrationPush | None:
    """Return the GitHub push record for the workspace, with issue_count populated.

    Returns ``None`` if no push has ever been initiated. Caller is expected
    to translate ``None`` to a 404 at the HTTP layer.
    """
    result = await db.execute(
        select(IntegrationPush).where(
            IntegrationPush.workspace_id == workspace_id,
            IntegrationPush.user_id == user_id,
            IntegrationPush.provider == GITHUB_PROVIDER,
        )
    )
    push = result.scalar_one_or_none()
    if push is None:
        return None
    push.issue_count = await _count_issues(db, push.id)  # type: ignore[attr-defined]
    return push


# ---------------------------------------------------------------------------
# App-mode export (Phase 21 — T-269): request path prepares, worker executes
# ---------------------------------------------------------------------------


def installation_can_create_repos(installation: GitHubInstallation) -> bool:
    """True when Thought2Build can auto-create a repo under this installation.

    GitHub gives GitHub Apps no way to create a repo in a personal (User)
    account, and no App-compatible way to add a freshly-created repo to a
    "selected repositories" installation — so auto-creation is only reachable
    for an Organization installation scoped to "All repositories". This is the
    single source of truth for that gate: :func:`_run_app_export` (the worker)
    and the repo-picker listing endpoint (``can_create`` on ``RepoList``) both
    read it, so the UI's "create a new repo" affordance can never drift from
    what the export will actually do.
    """
    return (
        installation.account_type == "Organization"
        and installation.repository_selection == "all"
    )


async def list_repos_for_installation(
    installation: GitHubInstallation,
    *,
    client: GitHubAPIClient | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """List the repos an installation's token can access (the repo-picker feed).

    Unlike the worker paths above, this serves an interactive request, so the
    client is built **without** the per-installation governor — the governor's
    ``GitHubThrottledError`` is a worker-requeue signal that would surface as an
    unhandled 500 here, and picker reads must never spend the write budget the
    export jobs depend on. GitHub's request budget is instead protected by the
    middleware rate tier on the endpoint plus the client's circuit breaker.

    ``client`` is injected by tests; production builds one exactly like
    :func:`run_export_push` (minus the governor).
    """
    if client is not None:
        return await client.list_installation_repositories()
    async with make_shared_async_client() as http:
        redis = get_shared_redis()
        token_provider = make_token_provider(redis, http)
        built = make_app_github_client(
            token_provider,
            installation.installation_id,
            http,
        )
        return await built.list_installation_repositories()


async def load_owned_installation(
    db: AsyncSession,
    installation_id: UUID,
    user_id: UUID,
) -> GitHubInstallation | None:
    """Return the installation iff it exists and is owned by ``user_id``.

    The owner check is the confused-deputy guard: a user may only export under an
    installation they linked, never another tenant's.
    """
    result = await db.execute(
        select(GitHubInstallation).where(GitHubInstallation.id == installation_id)
    )
    installation = result.scalar_one_or_none()
    if installation is None or installation.user_id != user_id:
        return None
    return installation


async def prepare_export_push(
    db: AsyncSession,
    *,
    workspace_id: UUID,
    user_id: UUID,
    installation: GitHubInstallation,
    export_mode: str,
) -> IntegrationPush:
    """Validate and create/reset the ``pending`` push row for an App export.

    Runs on the request path before enqueuing the ``export_push`` job. Validates
    workspace ownership and that every stage is finalised (raising
    :class:`ExportNotReadyError`), then reuses the workspace's single GitHub push
    row (or creates one), binding it to the target installation and resetting it
    to ``pending`` so :func:`run_export_push` can drive it on the worker.

    The actual GitHub I/O — repo creation, file push, issue creation — happens on
    the worker, never here.
    """
    await _load_workspace(db, workspace_id, user_id)
    await _load_finalised_stages(db, workspace_id)

    result = await db.execute(
        select(IntegrationPush).where(
            IntegrationPush.workspace_id == workspace_id,
            IntegrationPush.provider == GITHUB_PROVIDER,
        )
    )
    push = result.scalar_one_or_none()
    if push is None:
        push = IntegrationPush(
            workspace_id=workspace_id,
            user_id=user_id,
            provider=GITHUB_PROVIDER,
            status="pending",
        )
        db.add(push)
    else:
        push.status = "pending"
    push.installation_id = installation.id
    push.export_mode = export_mode
    await db.commit()
    await db.refresh(push)
    return push


async def run_export_push(
    push_id: UUID,
    repo_name: str,
    visibility: str,
    *,
    db: AsyncSession,
    client: GitHubAPIClient | None = None,
    is_final_attempt: bool = True,
) -> IntegrationPush | None:
    """Execute a prepared App-mode export (the body of the ``export_push`` job).

    Idempotent and resumable (keyed by ``push_id``): a re-run after a crash or
    rate-limit resumes from the ledger — it never re-creates a repo whose
    ``repo_full_name`` is already recorded, nor an issue already in
    ``integration_push_tasks``. A push already ``completed`` is a no-op.

    On success it persists the three fields the rest of the loop depends on —
    ``repo_id`` (from ``repository.id``), ``installation_id`` (already bound by
    :func:`prepare_export_push`), and ``source_stage_version_id`` (the Tasks
    ``StageVersion`` that produced this push) — and sets ``status='completed'``.
    Without these, token re-mint/confused-deputy (T-272), drift compare (T-273),
    and the ``find_live_push`` lookup (T-266) are all inert.

    ``client`` is injected by tests; in production an App-mode client is built
    from the bound installation's token provider.

    ``is_final_attempt`` is ``ctx["job_try"] >= JOB_MAX_TRIES`` from the arq
    wrapper (default ``True`` for the rare direct/non-arq caller, preserving
    today's "fail on first exception" semantics for them). It governs whether
    a possibly-transient failure below is allowed to leave the push row live
    for the worker's own retry, or must be marked ``failed`` now because no
    further attempt is coming — see :func:`_fail_or_retry`.
    """
    push = await _load_push_by_id(db, push_id)
    if push is None:
        logger.warning("github_export.push_missing", extra={"push_id": str(push_id)})
        return None
    if push.status == "completed":
        return push  # already done — at-least-once safe

    installation = await _load_installation(db, push.installation_id)
    if installation is None:
        await _mark_push_failed(db, push, status="failed")
        raise GitHubNotConnectedError("GitHub App installation not found for this push")

    try:
        workspace = await _load_workspace_by_id(db, push.workspace_id)
        stages = await _load_finalised_stages(db, push.workspace_id)
        source_version_id = await _resolve_source_stage_version_id(db, stages["tasks"])
    except Exception as exc:
        # Most likely ExportNotReadyError — a stage was un-finalised between
        # `prepare_export_push` (request time) and the worker picking this job
        # up. Routed through the same permanent/transient decision as the
        # GitHub-call errors below so it doesn't strand the push `pending`
        # through a pointless full retry cycle.
        await _fail_or_retry(db, push, exc, is_final_attempt=is_final_attempt)
        raise

    if client is not None:
        # Injected client (tests): no governor — serialization/budget are no-ops.
        return await _drive_app_export(
            db=db,
            workspace=workspace,
            stages=stages,
            push=push,
            client=client,
            governor=None,
            installation=installation,
            repo_name=repo_name,
            visibility=visibility,
            source_version_id=source_version_id,
            is_final_attempt=is_final_attempt,
        )

    async with make_shared_async_client() as http:
        redis = get_shared_redis()
        token_provider = make_token_provider(redis, http)
        # Per-installation governor (T-274): budgets every call against GitHub's
        # primary/secondary limits and serializes this repo's content writes.
        governor = make_governor(installation.installation_id, redis)
        built = make_app_github_client(
            token_provider,
            installation.installation_id,
            http,
            governor=governor,
        )
        return await _drive_app_export(
            db=db,
            workspace=workspace,
            stages=stages,
            push=push,
            client=built,
            governor=governor,
            installation=installation,
            repo_name=repo_name,
            visibility=visibility,
            source_version_id=source_version_id,
            is_final_attempt=is_final_attempt,
        )


async def _fail_or_retry(
    db: AsyncSession,
    push: IntegrationPush,
    exc: Exception,
    *,
    is_final_attempt: bool,
) -> None:
    """Mark ``push`` terminally 'failed' now, or leave it live for a retry.

    A deterministic error (:data:`_PERMANENT_EXPORT_ERRORS` — bad token, taken
    repo name, missing App permission, an un-finalised stage) fails immediately:
    no number of identical retries changes the outcome, so there is nothing to
    wait for. Anything else (a GitHub 5xx, a network blip, an unexpected bug)
    is only marked 'failed' once ``is_final_attempt`` is true — i.e. the arq
    wrapper (``services/queue.py``) is not going to try again — so a single
    transient hiccup on attempt 1 of ``JOB_MAX_TRIES`` doesn't make the push
    row (and therefore the frontend's poll) report a terminal failure while a
    retry that may well succeed is still coming.
    """
    if isinstance(exc, _PERMANENT_EXPORT_ERRORS) or is_final_attempt:
        GITHUB_EXPORT_TOTAL.labels(export_mode=push.export_mode, outcome="failed").inc()
        await _mark_push_failed(db, push, status="failed")


async def _drive_app_export(
    *,
    db: AsyncSession,
    workspace: Workspace,
    stages: dict[str, Stage],
    push: IntegrationPush,
    client: GitHubAPIClient,
    governor: InstallationRateGovernor | None,
    installation: GitHubInstallation,
    repo_name: str,
    visibility: str,
    source_version_id: UUID | None,
    is_final_attempt: bool = True,
) -> IntegrationPush:
    try:
        return await _run_app_export(
            db=db,
            workspace=workspace,
            stages=stages,
            push=push,
            client=client,
            governor=governor,
            installation=installation,
            repo_name=repo_name,
            visibility=visibility,
            source_version_id=source_version_id,
        )
    except (GitHubThrottledError, GitHubUnavailableError):
        # Backpressure (T-274) and a tripped circuit breaker are NOT failures:
        # the push stays live (non-'failed', so find_live_push still sees it) and
        # the worker requeues — throttle defers off the try budget, breaker-open
        # surfaces "sync paused" and retries. Marking 'failed' here would wrongly
        # drop the live push and skip the resume.
        raise
    except Exception as exc:
        await _fail_or_retry(db, push, exc, is_final_attempt=is_final_attempt)
        raise


async def _resolve_existing_repo(
    client: GitHubAPIClient,
    installation: GitHubInstallation,
    repo_name: str,
) -> dict[str, Any] | None:
    """Resolve ``repo_name`` to a repo this installation can actually write to.

    The repo JSON's ``permissions`` block can never be the access signal here:
    it describes an authenticated *user*, and under an installation token
    GitHub returns it all-false even for fully writable repos (verified live —
    a filter on it rejects every repo). Instead:

    - A **"selected repositories"** installation's authoritative grant is the
      ``GET /installation/repositories`` list — resolve by case-insensitive
      bare name (GitHub repo names are case-insensitively unique per owner).
      A visible-but-ungranted repo (e.g. a public repo under the same account
      that wasn't added to the installation) correctly misses.
    - An **"all repositories"** installation is granted everything under its
      account by construction, so a plain ``GET /repos/{owner}/{repo}`` hit is
      sufficient (the owner is pinned to ``installation.account_login``, so a
      same-named repo under a different owner can never be resolved).
    """
    if installation.repository_selection == "selected":
        granted, _truncated = await client.list_installation_repositories()
        wanted = repo_name.lower()
        for row in granted:
            name = row.get("name")
            if isinstance(name, str) and name.lower() == wanted:
                return row
        return None
    return await client.get_repo(f"{installation.account_login}/{repo_name}")


async def _run_app_export(
    *,
    db: AsyncSession,
    workspace: Workspace,
    stages: dict[str, Stage],
    push: IntegrationPush,
    client: GitHubAPIClient,
    governor: InstallationRateGovernor | None,
    installation: GitHubInstallation,
    repo_name: str,
    visibility: str,
    source_version_id: UUID | None,
) -> IntegrationPush:
    # Step 1 — resolve or create the repo (first run only). Resume skips this
    # when the repo is already recorded, so a re-run never re-resolves it. A
    # brand-new repo has no concurrent writer yet, so this single step runs
    # outside the per-repo lock (which keys on repo_id, only known after this
    # step).
    #
    # Auto-creation is only reachable when installation_can_create_repos (see
    # its docstring for GitHub's platform restrictions). Everywhere else, the
    # repo must already exist (and already be granted to this installation) or
    # export can't proceed.
    if push.repo_full_name is None:
        owner_repo = f"{installation.account_login}/{repo_name}"
        existing = await _resolve_existing_repo(client, installation, repo_name)
        if existing is not None:
            repo_data = existing
        elif installation_can_create_repos(installation):
            repo_data = await client.create_org_repo(
                installation.account_login,
                repo_name,
                private=(visibility == "private"),
            )
        else:
            raise GitHubRepoCreationUnsupportedError(
                f"Thought2Build can't auto-create {owner_repo!r}. Create it on "
                "GitHub yourself, make sure this installation can access it, "
                "then export again."
            )
        full_name = repo_data.get("full_name")
        repo_id = repo_data.get("id")
        html_url = repo_data.get("html_url")
        if not isinstance(full_name, str):
            raise GitHubAPIError(0, "create_repo response missing full_name")
        push.repo_full_name = full_name
        push.repo_url = html_url if isinstance(html_url, str) else None
        # repo_id is the immutable reconciliation key (T-266/T-272/T-273).
        if isinstance(repo_id, int):
            push.repo_id = repo_id
        await db.commit()
        await db.refresh(push)

    assert push.repo_full_name is not None  # nosec B101 — set by the branch above

    # Steps 2–3 — all content writes to this repo are serialized under a
    # per-repo_id lock (T-274) so two concurrent jobs cannot race on the blob SHA
    # (stale-SHA 409) or trip GitHub's secondary abuse-detection on one repo. PR
    # mode dispatches *inside* this wrapped + locked path so it inherits the
    # T-274 throttle/breaker handling and the serialization for free.
    lock = (
        governor.repo_write_lock(push.repo_id)
        if governor is not None
        else nullcontext()
    )
    tasks = parse_tasks(stages["tasks"].content or "")
    async with lock:
        if push.export_mode == "pr_with_tests":
            await _write_pr_with_tests(
                db=db,
                client=client,
                push=push,
                workspace=workspace,
                stages=stages,
                tasks=tasks,
            )
        else:
            await _write_files_to_default(
                db=db,
                client=client,
                push=push,
                workspace=workspace,
                stages=stages,
                tasks=tasks,
            )

    # Step 4 — finalise. Persist the three loop-critical fields and complete.
    push.source_stage_version_id = source_version_id
    push.status = "completed"
    push.pushed_at = datetime.now(UTC)
    await db.commit()
    await db.refresh(push)
    push.issue_count = await _count_issues(db, push.id)  # type: ignore[attr-defined]
    GITHUB_EXPORT_TOTAL.labels(export_mode=push.export_mode, outcome="completed").inc()
    github_audit(
        GITHUB_AUDIT_EXPORT_COMPLETED,
        push_id=str(push.id),
        workspace_id=str(push.workspace_id),
        repo_id=push.repo_id,
        status="completed",
    )
    # Forward-sync the Projects v2 board + milestones (T-281) off the completed
    # push. Best-effort: the export has already committed, so a queue outage must
    # never fail it.
    await github_projects.trigger_board_sync(push.id)
    return push


async def _write_files_to_default(
    *,
    db: AsyncSession,
    client: GitHubAPIClient,
    push: IntegrationPush,
    workspace: Workspace,
    stages: dict[str, Stage],
    tasks: list[ParsedTask],
) -> None:
    """Phase-13 export: push every file to the default branch, create issues."""
    assert push.repo_full_name is not None  # nosec B101
    repo = push.repo_full_name
    files_to_push = _build_file_map(stages, workspace)
    commit_message = f"chore: Thought2Build export — {workspace.name}"
    for path, content in files_to_push.items():
        sha = await client.get_file_sha(repo, path)
        await client.upsert_file(repo, path, content, sha, commit_message)
    await _sync_agent_instruction_files(client, repo, workspace, stages)
    # Construction report on the default branch (plan §8.2), both modes.
    await _push_construction_report(client, repo, db, workspace, stages)
    await _sync_issues(db, client, repo, push, tasks)


async def _write_pr_with_tests(
    *,
    db: AsyncSession,
    client: GitHubAPIClient,
    push: IntegrationPush,
    workspace: Workspace,
    stages: dict[str, Stage],
    tasks: list[ParsedTask],
) -> None:
    """Open the Harness stage as an executable pull request (T-276).

    Docs (SPEC/PLAN/TASKS) land on the default branch as the base; the harness
    contracts, the CI workflow, and one red stub per task land on a deterministic
    increment branch; a single PR links the issues via ``Closes #N``. Every step
    is idempotent so a resumed export updates in place and never duplicates the
    branch or the PR.
    """
    assert push.repo_full_name is not None  # nosec B101
    repo = push.repo_full_name

    # 1. Docs → default branch (establishes `main` on a fresh, empty repo).
    docs = {
        filename: _redact_stage_content(
            stages[stage_type].content or "",
            workspace_id=workspace.id,
            file_path=filename,
        )
        for stage_type, filename in _STAGE_FILES.items()
    }
    docs_message = f"docs: Thought2Build spec — {workspace.name}"
    for path, content in docs.items():
        sha = await client.get_file_sha(repo, path)
        await client.upsert_file(repo, path, content, sha, docs_message)
    # Mode-aware agent instructions live on the default branch with the docs.
    await _sync_agent_instruction_files(client, repo, workspace, stages)
    # Operating manual (Demo Day, already in the file map) + the construction
    # report on the default branch (§8). The report ships for both modes.
    await _push_construction_report(client, repo, db, workspace, stages)

    # 2. Issues (records issue numbers used for the PR's Closes #N links).
    issue_numbers = await _sync_issues(db, client, repo, push, tasks)

    # 3. Branch off the default branch base. Deterministic name + 422-tolerant
    #    create → a resumed export reuses the same branch instead of failing.
    if push.branch_name is None:
        push.branch_name = _INCREMENT_BRANCH
    base_sha = await client.get_ref(repo, f"heads/{_DEFAULT_BRANCH}")
    await client.create_branch(repo, push.branch_name, base_sha)
    await db.commit()

    # 4. Harness contracts + CI workflow + per-task red stubs → the branch.
    harness_files = parse_harness_files(
        stages["harness"].content or "", workspace_id=workspace.id
    )
    stacks = pr_export_builder.detect_stacks(
        stages["plan"].content or "", harness_files
    )
    scaffold = pr_export_builder.build_scaffold(
        harness_files=harness_files, tasks=tasks, stacks=stacks
    )
    scaffold_message = f"test: Thought2Build harness — {workspace.name}"
    for path, content in scaffold.items():
        sha = await client.get_file_sha(repo, path, ref=push.branch_name)
        await client.upsert_file(
            repo, path, content, sha, scaffold_message, branch=push.branch_name
        )

    # 5. One PR, reused on re-export (persisted pr_number → never duplicated).
    if push.pr_number is None:
        body = pr_export_builder.build_pr_body(
            tasks=tasks, issue_numbers=issue_numbers, stacks=stacks
        )
        title = f"Thought2Build harness — {workspace.name}"
        push.pr_number = await client.create_pull_request(
            repo, head=push.branch_name, base=_DEFAULT_BRANCH, title=title, body=body
        )
        await db.commit()
        GITHUB_PR_TOTAL.labels(outcome="opened").inc()
        github_audit(
            GITHUB_AUDIT_PR_OPENED,
            push_id=str(push.id),
            workspace_id=str(push.workspace_id),
            repo_id=push.repo_id,
            status="opened",
        )


async def _sync_issues(
    db: AsyncSession,
    client: GitHubAPIClient,
    repo: str,
    push: IntegrationPush,
    tasks: list[ParsedTask],
) -> dict[str, int]:
    """Create/update one issue per task; return the ``{task_ref: issue#}`` map.

    Matching/identity is the **stable** ``compute_task_ref(title)`` (audit #2),
    never the volatile human ``T-NNN`` — so a re-finalise that renumbers/reorders
    tasks still updates each task's *same* issue instead of mismapping and
    opening duplicates. Pre-existing rows on the legacy key are migrated in place
    first. Each new issue is committed immediately so a mid-run rate-limit
    resumes without recreating issues 1..N-1.
    """
    await migrate_legacy_task_refs(db, push.id, client, repo)
    existing = await _load_existing_push_tasks(db, push.id)
    issue_numbers: dict[str, int] = dict(existing)
    current_refs = {compute_task_ref(parsed.title) for parsed in tasks}
    for parsed in tasks:
        ref = compute_task_ref(parsed.title)
        existing_number = existing.get(ref)
        # Agent-ready body + labels (T-277): the issue is an optimal agent prompt.
        if existing_number is not None:
            await client.update_issue(
                repo,
                existing_number,
                parsed.title,
                parsed.agent_body_md,
                labels=parsed.labels,
            )
        else:
            number = await client.create_issue(
                repo, parsed.title, parsed.agent_body_md, labels=parsed.labels
            )
            db.add(
                IntegrationPushTask(
                    push_id=push.id,
                    task_ref=ref,
                    external_issue_number=number,
                )
            )
            await db.commit()
            issue_numbers[ref] = number
    retired = await retire_obsolete_task_issues(db, client, repo, push.id, current_refs)
    for ref in retired:
        issue_numbers.pop(ref, None)
    return issue_numbers


async def _push_construction_report(
    client: GitHubAPIClient,
    repo: str,
    db: AsyncSession,
    workspace: Workspace,
    stages: dict[str, Stage],
    *,
    branch: str | None = None,
) -> None:
    """Write CONSTRUCTION_REPORT.md to the repo — BOTH modes (§8.2).

    Mode decides which linter produced the verdict, not whether one exists; the
    report is the only place a standard-mode user can read what the badge's gap
    count actually refers to.

    Refreshes the verdict if stale (zero-LLM, cheap) before rendering, then pushes
    it idempotently — reads the current file first and skips the write when the
    content is unchanged so a re-export emits no spurious commit.

    Best-effort END TO END, not just on the verdict: the report is a supplementary
    document, and the export it rides on carries the artifacts the user actually
    paid for. Now that it runs for every workspace rather than the Demo Day
    minority, a transient GitHub error on this one optional file must not fail an
    otherwise-complete push. Errors are logged, never raised.
    """
    try:
        verdict = await construction_verdict_service.ensure_fresh_verdict(
            db, workspace, stages
        )
        if not verdict:
            return
        content = agent_manual_service.build_construction_report(verdict)
        filename = agent_manual_service.CONSTRUCTION_REPORT_FILENAME
        existing = await client.get_file_content(repo, filename, ref=branch)
        existing_text = existing[0] if existing else None
        existing_sha = existing[1] if existing else None
        if existing is not None and content == existing_text:
            return
        await client.upsert_file(
            repo,
            filename,
            content,
            existing_sha,
            "docs: Thought2Build construction report",
            branch=branch,
        )
    except Exception:
        logger.warning(
            "github_export.construction_report_failed workspace_id=%s repo=%s",
            getattr(workspace, "id", None),
            repo,
            exc_info=True,
        )


_DEMO_MANAGED_RE = re.compile(
    re.escape(agent_manual_service.DEMO_MANAGED_START)
    + r".*?"
    + re.escape(agent_manual_service.DEMO_MANAGED_END),
    re.DOTALL,
)


def _merge_instruction_file(existing: str | None, generated: str) -> str:
    """Merge a selected instruction block without touching user-authored text."""
    if agent_manual_service.DEMO_MANAGED_START in generated:
        block = generated.strip()
        if not existing:
            return block + "\n"
        match = _DEMO_MANAGED_RE.search(existing)
        if match:
            return existing[: match.start()] + block + existing[match.end() :]
        # Conservative migration of the old unmarked Thought2Build manual. Its full
        # fixed structure is distinctive; ambiguous files are preserved.
        legacy_markers = (
            existing.startswith("# Build Protocol — "),
            "## Non-negotiable invariants" in existing,
            "## The loop" in existing,
            "## Definition of done" in existing,
            "The tests are the frozen oracle" in existing,
        )
        if all(legacy_markers):
            return block + "\n"
        separator = (
            ""
            if existing.endswith("\n\n")
            else "\n" if existing.endswith("\n") else "\n\n"
        )
        return f"{existing}{separator}{block}\n"
    # Standard renderer already implements safe generic managed-block merging.
    block = agents_md_builder.managed_block(generated)
    if block is None:
        return generated
    if not existing:
        return block + "\n"
    return agents_md_builder.merge_managed_block(existing, block)


def _remove_managed_instructions(existing: str) -> tuple[str, bool]:
    updated, generic_changed = agents_md_builder.remove_managed_block(existing)
    updated, demo_count = _DEMO_MANAGED_RE.subn("", updated)
    if not generic_changed and not demo_count:
        return existing, False
    # Preserve user bytes outside the managed block. Whitespace-only residue is
    # considered empty so a Thought2Build-only file can be deleted.
    return (updated if updated.strip() else ""), True


async def _sync_agent_instruction_files(
    client: GitHubAPIClient,
    repo: str,
    workspace: Workspace,
    stages: dict[str, Stage],
    *,
    branch: str | None = None,
) -> None:
    """Reconcile selected files and safely remove only deselected managed blocks."""
    stage_md = {kind: (stage.content or "") for kind, stage in stages.items()}
    selected = agent_manual_service.build_instruction_files(workspace, stage_md)
    for filename in ("AGENTS.md", "CLAUDE.md"):
        existing = await client.get_file_content(repo, filename, ref=branch)
        if filename in selected:
            existing_text = existing[0] if existing else None
            existing_sha = existing[1] if existing else None
            content = _merge_instruction_file(existing_text, selected[filename])
            if existing is not None and content == existing_text:
                continue
            await client.upsert_file(
                repo,
                filename,
                content,
                existing_sha,
                "docs: Thought2Build agent instructions",
                branch=branch,
            )
            logger.info(
                "agent instruction file synced: repo=%s path=%s", repo, filename
            )
            github_audit(
                "github.agent_instruction.synced",
                workspace_id=str(workspace.id),
                status=filename,
            )
            continue
        if existing is None:
            continue
        cleaned, changed = _remove_managed_instructions(existing[0])
        if not changed:
            logger.info(
                "agent instruction cleanup skipped for unowned file: repo=%s path=%s",
                repo,
                filename,
            )
            github_audit(
                "github.agent_instruction.cleanup_skipped",
                workspace_id=str(workspace.id),
                status=filename,
            )
            continue
        if cleaned:
            await client.upsert_file(
                repo,
                filename,
                cleaned,
                existing[1],
                "docs: remove stale Thought2Build agent instructions",
                branch=branch,
            )
        else:
            await client.delete_file(
                repo,
                filename,
                existing[1],
                "docs: remove stale Thought2Build agent instructions",
                branch=branch,
            )
        logger.info("agent instruction file cleaned: repo=%s path=%s", repo, filename)
        github_audit(
            "github.agent_instruction.cleaned",
            workspace_id=str(workspace.id),
            status=filename,
        )


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------


async def _run_export(
    *,
    db: AsyncSession,
    workspace: Workspace,
    stages: dict[str, Stage],
    integration: UserIntegration,
    push: IntegrationPush,
    client: GitHubAPIClient,
    repo_name: str,
    visibility: str,
) -> IntegrationPush:
    # Step 4 — create repo (first export only)
    if push.repo_full_name is None:
        repo_data = await client.create_repo(
            repo_name, private=(visibility == "private")
        )
        full_name = repo_data.get("full_name")
        html_url = repo_data.get("html_url")
        if not isinstance(full_name, str):
            raise GitHubAPIError(0, "create_repo response missing full_name")
        push.repo_full_name = full_name
        push.repo_url = html_url if isinstance(html_url, str) else None
        await db.commit()
        await db.refresh(push)

    assert push.repo_full_name is not None  # nosec — guaranteed by branch above

    # Step 5 — push files
    files_to_push = _build_file_map(stages, workspace)
    commit_message = f"chore: Thought2Build export — {workspace.name}"
    for path, content in files_to_push.items():
        sha = await client.get_file_sha(push.repo_full_name, path)
        await client.upsert_file(
            push.repo_full_name,
            path,
            content,
            sha,
            commit_message,
        )
    # Construction report (plan §8.2), both modes. The manual already rode the
    # file map; the report needs the (refreshed) verdict.
    await _push_construction_report(client, push.repo_full_name, db, workspace, stages)

    # Step 6 — create/update issues sequentially. GitHub has a secondary
    # rate limit on content creation; gathering these would trip it. Identity is
    # the stable compute_task_ref(title) (audit #2), migrating any legacy rows
    # first so a renumber/reorder updates the same issue rather than duplicating.
    await migrate_legacy_task_refs(db, push.id, client, push.repo_full_name)
    existing_tasks = await _load_existing_push_tasks(db, push.id)
    tasks = parse_tasks(stages["tasks"].content or "")
    current_refs = {compute_task_ref(parsed.title) for parsed in tasks}
    for parsed in tasks:
        ref = compute_task_ref(parsed.title)
        existing_number = existing_tasks.get(ref)
        if existing_number is not None:
            await client.update_issue(
                push.repo_full_name,
                existing_number,
                parsed.title,
                parsed.body_md,
            )
        else:
            number = await client.create_issue(
                push.repo_full_name,
                parsed.title,
                parsed.body_md,
            )
            db.add(
                IntegrationPushTask(
                    push_id=push.id,
                    task_ref=ref,
                    external_issue_number=number,
                )
            )
            # Commit each new issue so partial failure (e.g. rate limit on
            # issue #50 of 100) preserves issues 1-49 — re-export resumes
            # from #50 without recreating them.
            await db.commit()

    await retire_obsolete_task_issues(
        db, client, push.repo_full_name, push.id, current_refs
    )

    # Step 7 — finalise (canonical status — audit #4)
    push.status = "completed"
    push.pushed_at = datetime.now(UTC)
    integration.last_used_at = datetime.now(UTC)
    await db.commit()
    await db.refresh(push)

    push.issue_count = await _count_issues(db, push.id)  # type: ignore[attr-defined]
    return push


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


async def _handle_token_expired(
    db: AsyncSession,
    user_id: UUID,
    push: IntegrationPush,
) -> None:
    """Delete the invalidated UserIntegration row and mark the push as error.

    The two updates run as separate commits so a failure in the second
    cannot leave a known-invalid token in place. Thought2Build never retries
    with a known-bad token; the user is prompted to reconnect.
    """
    try:
        await db.execute(
            delete(UserIntegration).where(
                UserIntegration.user_id == user_id,
                UserIntegration.provider == GITHUB_PROVIDER,
            )
        )
        await db.commit()
    except Exception:
        logger.exception("github_export.token_expiry_delete_failed")
        # Continue to mark the push as failed — the integration row may
        # be gone or may not be, but we still want a status on the push.
        try:
            await db.rollback()
        except Exception:  # pragma: no cover - best-effort rollback
            logger.exception("github_export.token_expiry_rollback_failed")

    await _mark_push_failed(db, push)


async def mark_push_unstarted(db: AsyncSession, push: IntegrationPush) -> None:
    """Mark a just-prepared push ``failed`` when enqueue could not start it.

    Leaves no dangling non-``failed`` row that ``find_live_push`` would treat as
    a live push for the repo. Correct for a *first* export (the row had no prior
    live state); a **resync** must use :func:`restore_push_status` instead so a
    queue blip never drops an already-healthy push (audit #3).
    """
    await _mark_push_failed(db, push, status="failed")


async def restore_push_status(
    db: AsyncSession, push: IntegrationPush, status: str
) -> None:
    """Restore a push to a prior status after a transient enqueue failure.

    Used by **resync** (audit #3): resync flips a live (``completed``/``stale``)
    push to ``pending`` before enqueuing. If the queue is briefly unavailable,
    failing the row would make ``find_live_push`` (``status <> 'failed'``)
    exclude it, silently dropping the workspace's bidirectional sync until a full
    re-export. Restoring the prior live status keeps the push live and re-syncable
    on retry. Uses the same safe commit-with-rollback as ``_mark_push_failed``.
    """
    try:
        push.status = status
        await db.commit()
    except Exception:
        logger.exception("github_export.restore_status_commit_error")
        try:
            await db.rollback()
        except Exception:  # pragma: no cover - best-effort rollback
            logger.exception("github_export.restore_status_rollback_error")


async def _mark_push_failed(
    db: AsyncSession, push: IntegrationPush, *, status: str = "failed"
) -> None:
    try:
        push.status = status
        await db.commit()
    except Exception:
        logger.exception("github_export.mark_failed_commit_error")
        try:
            await db.rollback()
        except Exception:  # pragma: no cover - best-effort rollback
            logger.exception("github_export.mark_failed_rollback_error")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


async def _load_workspace(
    db: AsyncSession,
    workspace_id: UUID,
    user_id: UUID,
) -> Workspace:
    result = await db.execute(select(Workspace).where(Workspace.id == workspace_id))
    workspace = result.scalar_one_or_none()
    if workspace is None or workspace.user_id != user_id:
        # 404-style: never leak existence of someone else's workspace.
        raise ExportNotReadyError("Workspace not found")
    return workspace


async def _load_finalised_stages(
    db: AsyncSession,
    workspace_id: UUID,
) -> dict[str, Stage]:
    result = await db.execute(select(Stage).where(Stage.workspace_id == workspace_id))
    stages = {s.type: s for s in result.scalars()}
    for stage_type in ("spec", "plan", "harness", "tasks"):
        stage = stages.get(stage_type)
        if stage is None or stage.status != "finalised":
            raise ExportNotReadyError(
                f"Stage {stage_type!r} is not finalised — export unavailable"
            )
    return stages


async def _load_integration(
    db: AsyncSession,
    user_id: UUID,
) -> UserIntegration | None:
    result = await db.execute(
        select(UserIntegration).where(
            UserIntegration.user_id == user_id,
            UserIntegration.provider == GITHUB_PROVIDER,
        )
    )
    return result.scalar_one_or_none()


async def _upsert_push_row(
    db: AsyncSession,
    workspace_id: UUID,
    user_id: UUID,
) -> IntegrationPush:
    """Reuse the workspace's existing github push or create one, marking it
    ``pending`` (the canonical in-flight status — audit #4).

    Historically this was a single ``INSERT ... ON CONFLICT`` on the
    ``uq_integration_push_workspace_provider`` constraint. Phase 21's migration
    ``0016`` rekeys the table onto the immutable ``repo_id`` and drops that
    constraint (a live push is now one per ``(workspace_id, repo_id)``). Legacy
    v1-OAuth pushes carry no ``repo_id`` (the partial index treats their NULL
    keys as distinct), so the legacy path keeps its "one push per workspace"
    semantics at the application level: look the row up by
    ``(workspace_id, provider)`` and reset its status, otherwise insert. Repo
    fields (``repo_full_name`` / ``repo_url``) are left untouched on re-export.
    """
    result = await db.execute(
        select(IntegrationPush).where(
            IntegrationPush.workspace_id == workspace_id,
            IntegrationPush.provider == GITHUB_PROVIDER,
        )
    )
    push = result.scalar_one_or_none()
    if push is None:
        push = IntegrationPush(
            workspace_id=workspace_id,
            user_id=user_id,
            provider=GITHUB_PROVIDER,
            status="pending",
        )
        db.add(push)
    else:
        push.status = "pending"
    await db.commit()
    await db.refresh(push)
    return push


async def _load_push_by_id(
    db: AsyncSession,
    push_id: UUID,
) -> IntegrationPush | None:
    result = await db.execute(
        select(IntegrationPush).where(IntegrationPush.id == push_id)
    )
    return result.scalar_one_or_none()


async def _load_workspace_by_id(db: AsyncSession, workspace_id: UUID) -> Workspace:
    """Load a workspace by id (worker context — ownership already enforced when
    the export was enqueued)."""
    result = await db.execute(select(Workspace).where(Workspace.id == workspace_id))
    workspace = result.scalar_one_or_none()
    if workspace is None:
        raise ExportNotReadyError("Workspace not found")
    return workspace


async def _load_installation(
    db: AsyncSession,
    installation_id: UUID | None,
) -> GitHubInstallation | None:
    if installation_id is None:
        return None
    result = await db.execute(
        select(GitHubInstallation).where(GitHubInstallation.id == installation_id)
    )
    return result.scalar_one_or_none()


async def _resolve_source_stage_version_id(
    db: AsyncSession,
    tasks_stage: Stage,
) -> UUID | None:
    """Return the StageVersion id of the Tasks stage's current version.

    Persisted on the push so drift detection (T-273) can compare the pushed
    Tasks version against the workspace's latest finalised Tasks.
    """
    result = await db.execute(
        select(StageVersion.id).where(
            StageVersion.stage_id == tasks_stage.id,
            StageVersion.version == tasks_stage.current_version,
        )
    )
    return result.scalar_one_or_none()


async def _load_existing_push_tasks(
    db: AsyncSession,
    push_id: UUID,
) -> dict[str, int]:
    """Return a map of task_ref → issue_number for the given push."""
    result = await db.execute(
        select(
            IntegrationPushTask.task_ref,
            IntegrationPushTask.external_issue_number,
        ).where(IntegrationPushTask.push_id == push_id)
    )
    return {row.task_ref: row.external_issue_number for row in result.all()}


async def _count_issues(db: AsyncSession, push_id: UUID) -> int:
    from sqlalchemy import func

    result = await db.execute(
        select(func.count())
        .select_from(IntegrationPushTask)
        .where(IntegrationPushTask.push_id == push_id)
    )
    return int(result.scalar() or 0)


def _build_file_map(
    stages: dict[str, Stage], workspace: Workspace | None = None
) -> dict[str, str]:
    """Return the full {path: content} map of every file to push.

    SPEC.md / PLAN.md / TASKS.md at repo root, plus everything from the
    harness/ directory parsed out of stages['harness'].content via the
    same parser the ZIP export uses (single source of truth). Agent instruction
    files are reconciled separately so user-authored content can be preserved.
    """
    files: dict[str, str] = {}
    for stage_type, filename in _STAGE_FILES.items():
        content = stages[stage_type].content or ""
        if workspace is not None:
            content = _redact_stage_content(
                content, workspace_id=workspace.id, file_path=filename
            )
        files[filename] = content
    harness_files = parse_harness_files(
        stages["harness"].content or "",
        workspace_id=workspace.id if workspace is not None else None,
    )
    files.update(harness_files)
    if workspace is not None:
        from services.pipeline.export_verification import verification_manifest

        files["VERIFICATION.json"] = json.dumps(
            verification_manifest(workspace, stages), indent=2
        )
    return files
