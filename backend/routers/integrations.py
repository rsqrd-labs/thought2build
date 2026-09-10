"""User-facing integration management endpoints.

Phase 13 (legacy OAuth, retained behind a flag) routes:

- ``GET /integrations/github`` — legacy connection status (connected + username).
- ``DELETE /integrations/github`` — idempotently removes the user's v1 OAuth
  integration. Always returns 204.

Phase 21 GitHub **App** install routes (T-270):

- ``GET /integrations/github/install`` — returns the App installation URL
  (``/apps/{slug}/installations/new``) with a one-time, user-bound state.
- ``GET /integrations/github/setup`` — the install callback. Authenticated
  solely by the state parameter (no bearer token on GitHub's browser redirect);
  upserts the installation and redirects to ``/settings?github_installed=true``.
- ``GET /integrations/github/installations`` — the user's installations
  (org + personal) plus ``on_legacy_oauth`` for the migration prompt and the
  export picker.
- ``DELETE /integrations/github/{installation_id}`` — locally revoke one
  installation (CSRF-protected by the global middleware; GitHub is unaffected).
- ``POST /integrations/github/webhook`` — the public GitHub event ingress
  (HMAC-verified, CSRF/rate-limit exempt). Verifies, dedups, hands the delivery
  to the worker, and returns a fast 2xx. All routing happens on the worker.

OAuth initiation/callback routes live in ``routers/auth.py``. Lifecycle webhook
events are dispatched from T-271's reconcile path via
:func:`handle_lifecycle_event`.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from redis.asyncio import Redis
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db, get_redis
from middleware.auth import get_current_user
from models import GitHubWebhookEvent, User
from schemas.github import (
    ExportSummary,
    InstallationList,
    InstallationOption,
    RepoList,
    RepoOption,
)
from schemas.integration import GitHubStatusResponse
from services.integrations import github_auth_service, github_install_service
from services.integrations.github_api_client import (
    GitHubAPIError,
    GitHubRateLimitError,
    GitHubTokenExpiredError,
    GitHubUnavailableError,
)
from services.integrations.github_app_auth import verify_webhook_signature
from services.integrations.github_install_service import (
    AppNotConfiguredError,
    InstallStateError,
    InstallVerificationError,
)
from services.integrations.push_repo import list_user_live_exports
from services.observability import (
    GITHUB_AUDIT_INSTALL_REJECTED,
    GITHUB_AUDIT_WEBHOOK_DUPLICATE_SKIPPED,
    GITHUB_AUDIT_WEBHOOK_RECEIVED,
    GITHUB_WEBHOOK_DEDUPED_TOTAL,
    GITHUB_WEBHOOK_FAILED_TOTAL,
    GITHUB_WEBHOOK_RECEIVED_TOTAL,
    GITHUB_WEBHOOK_VERIFIED_TOTAL,
    github_audit,
)
from services.pipeline import github_export_service
from services.queue import QueueUnavailableError, enqueue

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/integrations", tags=["integrations"])


# ---------------------------------------------------------------------------
# Phase 13 — legacy OAuth status/disconnect (retained, unchanged)
# ---------------------------------------------------------------------------


@router.get("/github", response_model=GitHubStatusResponse)
async def get_github_integration(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> GitHubStatusResponse:
    integration = await github_auth_service.get_integration(user.id, db)
    if integration is None:
        return GitHubStatusResponse(connected=False, github_username=None)
    return GitHubStatusResponse(
        connected=True,
        github_username=integration.github_username,
    )


@router.delete("/github", status_code=status.HTTP_204_NO_CONTENT)
async def disconnect_github(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await github_auth_service.disconnect_github(user.id, db)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Phase 21 — GitHub App install flow (T-270)
# ---------------------------------------------------------------------------


@router.get("/github/install")
async def github_app_install_url(
    user: User = Depends(get_current_user),
    redis: Redis = Depends(get_redis),
) -> dict[str, str]:
    """Return the App installation URL for the signed-in user.

    503 when the App is not configured so Settings can render a clear
    "feature disabled" state.
    """
    try:
        url = await github_install_service.build_install_url(user.id, redis)
    except AppNotConfiguredError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GitHub App is not configured.",
        ) from exc
    return {"url": url}


@router.get("/github/setup")
async def github_app_setup(
    request: Request,
    installation_id: int | None = None,
    setup_action: str | None = None,
    code: str | None = None,
    state: str | None = None,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> RedirectResponse:
    """GitHub App install callback (identity-verified — audit #1).

    The browser redirect from GitHub carries no bearer token, so the request is
    authenticated by the one-time ``state`` *and* — critically — the
    installation is only bound to the caller after a user-to-server OAuth check
    proves the caller administers the installation's account. Without that proof
    an attacker could supply a victim's ``installation_id`` with their own valid
    ``state`` and hijack the install (IDOR).

    Two shapes arrive here:

    1. **First hop (install redirect)** — ``installation_id`` + ``state``, and,
       when "Request user authorization (OAuth) during installation" is enabled
       on the App, an OAuth ``code`` too. With a ``code`` we verify inline
       (single hop). Without one we mint a verify-state and redirect to GitHub's
       authorize endpoint (the App's callback URL must point back here).
    2. **Second hop (verify return)** — ``code`` + a verify ``state`` minted in
       hop 1; we exchange the code, confirm admin, and bind.

    Any failure to *prove* control refuses the bind and redirects to Settings
    with ``github_installed=false`` (fail closed). ``setup_action`` may be
    ``install`` / ``update`` / ``request``; only a real install id is persisted.
    """
    # --- Second hop: a verify-state round-trip returning with an OAuth code. ---
    verify = await github_install_service.consume_identity_state(state, redis)
    if verify is not None:
        verify_user_id, verify_install_id = verify
        return await _complete_verified_install(
            db,
            user_id=verify_user_id,
            installation_id=verify_install_id,
            code=code,
        )

    # --- First hop: the install redirect, authenticated by the install state. ---
    try:
        user_id = await github_install_service.consume_install_state(state, redis)
    except InstallStateError:
        return _settings_redirect(installed=False)

    if installation_id is None or setup_action == "request":
        # No install completed (e.g. an org admin approval request) — nothing to
        # persist; bounce back to Settings without an error.
        return _settings_redirect(installed=False)

    if not github_install_service.app_identity_enabled():
        # Cannot prove the caller administers the installation, so we must not
        # bind it (audit #1). In production this is unreachable —
        # validate_production_settings requires identity OAuth when the App is
        # enabled — but fail closed and log loudly if it is ever hit.
        logger.error(
            "github_app_setup.identity_oauth_unconfigured: refusing to bind "
            "installation without proof of control (set GITHUB_APP_CLIENT_ID/"
            "GITHUB_APP_CLIENT_SECRET)."
        )
        _audit_install_rejected(installation_id, "identity_oauth_unconfigured")
        return _settings_redirect(installed=False)

    if code:
        # Single hop: GitHub already authorized the user during installation.
        return await _complete_verified_install(
            db,
            user_id=user_id,
            installation_id=installation_id,
            code=code,
        )

    # Two hops: obtain user authorization now, then bind on the verify return.
    authorize_url = await github_install_service.build_identity_verify_url(
        user_id, installation_id, redis
    )
    return RedirectResponse(
        url=authorize_url,
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    )


@router.get("/github/installations", response_model=InstallationList)
async def list_github_installations(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> InstallationList:
    """List the user's App installations + the legacy-OAuth migration flag."""
    rows = await github_install_service.list_installations(db, user.id)
    on_legacy = await github_install_service.user_on_legacy_oauth(db, user.id)
    return InstallationList(
        installations=[
            InstallationOption(
                id=row.id,
                installation_id=row.installation_id,
                account_login=row.account_login,
                account_type=row.account_type,
                repository_selection=row.repository_selection,
                suspended=row.suspended_at is not None,
            )
            for row in rows
        ],
        on_legacy_oauth=on_legacy,
    )


@router.get("/github/exports", response_model=list[ExportSummary])
async def list_github_exports(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[ExportSummary]:
    """List every live GitHub export the caller owns — one row per workspace,
    newest first — for the account-wide exports hub.

    Unlike the per-workspace ``GET /workspaces/{id}/sync`` (which 404s when a
    workspace has never been exported), this returns an **empty list** when the
    user has no exports: the hub renders its own empty state, never an error.
    """
    rows = await list_user_live_exports(db, user.id)
    return [
        ExportSummary(
            workspace_id=row.push.workspace_id,
            workspace_name=row.workspace_name,
            push_id=row.push.id,
            status=row.push.status,
            export_mode=row.push.export_mode,
            repo_full_name=row.push.repo_full_name,
            repo_url=row.push.repo_url,
            pr_number=row.push.pr_number,
            task_sync_status=row.task_sync_status,
            sync_paused=row.sync_paused,
            out_of_sync=(row.task_sync_status == "changes_pending"),
            shipped=row.shipped,
            total=row.total,
            pushed_at=row.push.pushed_at,
            last_inbound_sync_at=row.push.last_inbound_sync_at,
        )
        for row in rows
    ]


@router.get(
    "/github/installations/{installation_id}/repos",
    response_model=RepoList,
)
async def list_installation_repos(
    installation_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> RepoList:
    """List repositories one installation can export into (repo-picker feed).

    Guards mirror the export route (``POST /workspaces/{id}/export/github``):
    ownership is a 403 with the same wording — never a 404, so installation
    existence is not leaked — and a suspended installation is a 403. GitHub
    errors map to the same statuses the export route uses (429/502/503). The
    interactive client carries no governor, so a burst here can never requeue
    or throttle the worker's export jobs; the middleware rate tier plus the
    client's circuit breaker bound the GitHub budget spent.
    """
    installation = await github_export_service.load_owned_installation(
        db, installation_id, user.id
    )
    if installation is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Install the GitHub App and choose an installation you own.",
        )
    if installation.suspended_at is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This GitHub App installation is suspended. Re-enable it on GitHub.",
        )

    try:
        repos, truncated = await github_export_service.list_repos_for_installation(
            installation
        )
    except GitHubRateLimitError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="GitHub API rate limit exceeded.",
        ) from exc
    except GitHubUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GitHub is temporarily unavailable. Try again shortly.",
        ) from exc
    except (GitHubTokenExpiredError, GitHubAPIError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="GitHub API request failed.",
        ) from exc

    options: list[RepoOption] = []
    for repo in repos:
        repo_id = repo.get("id")
        name = repo.get("name")
        full_name = repo.get("full_name")
        if (
            not isinstance(repo_id, int)
            or not isinstance(name, str)
            or not isinstance(full_name, str)
        ):
            continue  # never let one malformed row 500 the whole picker
        html_url = repo.get("html_url")
        options.append(
            RepoOption(
                id=repo_id,
                name=name,
                full_name=full_name,
                private=bool(repo.get("private")),
                html_url=html_url if isinstance(html_url, str) else None,
            )
        )
    return RepoList(
        repositories=options,
        truncated=truncated,
        can_create=github_export_service.installation_can_create_repos(installation),
    )


@router.delete("/github/{installation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_github_installation(
    installation_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """Locally revoke one installation the caller owns (CSRF-protected).

    Idempotent: a missing/unowned install also returns 204 so the endpoint never
    leaks which installations exist. GitHub repos/issues are untouched.
    """
    await github_install_service.revoke_installation(db, installation_id, user.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# Webhook ingress (T-271) — verify → dedup → dispatch → fast 2xx
# ---------------------------------------------------------------------------


@router.post("/github/webhook")
async def github_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    """Public ingress for GitHub App webhook deliveries.

    Mirrors the audited Stripe receiver. Ordering is a hard security contract:
    the raw bytes are read first, the HMAC-SHA256 signature is verified
    (constant-time, against the current and previous secret for rotation) and a
    mismatch is rejected with 400 *before* any database or background work, the
    delivery is recorded for idempotency (a retry is skipped), and the event is
    handed to the worker for routing. The handler stays O(1) and never branches
    on event type — all fan-out happens on the worker (``reconcile_event``,
    T-272). No GitHub or LLM I/O on the request path; the body size is capped by
    the global middleware.
    """
    # Current secret first, then the previous one during a rotation window (T-271).
    # Centralised on Settings (T-283) so the [secret, prev] derivation lives in one
    # place; an empty list means the App webhook is unconfigured.
    secrets = settings.github_app_webhook_secrets
    if not secrets:
        # App webhook not configured → the route is effectively disabled.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")

    raw_body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256")
    if not verify_webhook_signature(raw_body, signature, secrets):
        GITHUB_WEBHOOK_FAILED_TOTAL.labels(error_type="bad_signature").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="invalid signature"
        )

    delivery_id = request.headers.get("X-GitHub-Delivery")
    event_type = request.headers.get("X-GitHub-Event")
    if not delivery_id or not event_type:
        GITHUB_WEBHOOK_FAILED_TOTAL.labels(error_type="missing_headers").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="missing delivery headers",
        )

    GITHUB_WEBHOOK_VERIFIED_TOTAL.inc()
    GITHUB_WEBHOOK_RECEIVED_TOTAL.labels(event_type=event_type).inc()

    # Idempotency: record the delivery before dispatch. A unique violation on
    # delivery_id means GitHub re-delivered this event — skip it.
    #
    # The payload rides along so a delivery that is recorded but never processed
    # can be REPLAYED (``reconcile_drift``'s inbox sweep). Because the dedup row
    # commits on receipt, GitHub's redelivery of such an event is answered
    # "duplicate" and it would otherwise be lost with no record. Parsing is
    # best-effort and unconditional — the handler still never branches on event
    # type — so a body that is not a JSON object simply stores no payload rather
    # than 500ing a signature-verified webhook.
    try:
        async with db.begin_nested():
            db.add(
                GitHubWebhookEvent(
                    delivery_id=delivery_id,
                    event_type=event_type,
                    payload=_decode_payload(raw_body),
                )
            )
            await db.flush()
    except IntegrityError:
        GITHUB_WEBHOOK_DEDUPED_TOTAL.labels(event_type=event_type).inc()
        github_audit(
            GITHUB_AUDIT_WEBHOOK_DUPLICATE_SKIPPED,
            delivery_id=delivery_id,
            event_type=event_type,
            status="duplicate",
        )
        return {"status": "duplicate"}

    # Hand off to the worker, THEN commit (at-least-once). If the handoff fails
    # the dedup row is never committed, so GitHub's retry re-delivers and
    # reconcile_event (idempotent by delivery_id, T-272) runs once. This
    # deliberately reorders the Plan §24.4 sketch (commit-then-handoff), which
    # would silently drop an event on a transient broker failure. The worker is
    # the single dumb dispatcher: the raw bytes are passed so it re-parses and
    # fans out by (event_type, action).
    # Issue events are tiny, directly user-visible state transitions and must
    # not sit behind long exports. Other webhook types retain the bulk lane;
    # routing all GitHub event traffic onto the fast lane would undermine its
    # billing/interactive isolation during a check-suite storm.
    reconcile_job = (
        "reconcile_issue_event" if event_type == "issues" else "reconcile_event"
    )
    try:
        await enqueue(reconcile_job, delivery_id, event_type, raw_body)
    except QueueUnavailableError as exc:
        GITHUB_WEBHOOK_FAILED_TOTAL.labels(error_type="enqueue_unavailable").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="background processing temporarily unavailable",
        ) from exc
    await db.commit()
    github_audit(
        GITHUB_AUDIT_WEBHOOK_RECEIVED,
        delivery_id=delivery_id,
        event_type=event_type,
        status="queued",
    )
    return {"status": "queued"}


# ---------------------------------------------------------------------------
# Lifecycle webhook dispatch (called from T-271 reconcile path)
# ---------------------------------------------------------------------------


async def handle_lifecycle_event(
    db: AsyncSession,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    """Apply an installation-lifecycle webhook to the stored install row.

    ``installation`` (``suspend`` / ``unsuspend`` / ``deleted``) sets/clears
    ``suspended_at`` and marks the install's pushes ``stale`` (sync paused);
    ``installation_repositories`` (``added`` / ``removed``) stales pushes for
    repos the App can no longer touch. Unknown event types are ignored.
    """
    installation = payload.get("installation") or {}
    installation_id = installation.get("id")
    if not isinstance(installation_id, int):
        return
    action = payload.get("action")

    if event_type == "installation":
        if action in ("suspend", "unsuspend", "deleted"):
            await github_install_service.apply_installation_event(
                db, action=action, installation_id=installation_id
            )
    elif event_type == "installation_repositories":
        removed = payload.get("repositories_removed") or []
        removed_repo_ids = [
            r.get("id") for r in removed if isinstance(r.get("id"), int)
        ]
        await github_install_service.apply_installation_repositories_event(
            db,
            action=action or "",
            installation_id=installation_id,
            removed_repo_ids=removed_repo_ids,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _complete_verified_install(
    db: AsyncSession,
    *,
    user_id: UUID,
    installation_id: int,
    code: str | None,
) -> RedirectResponse:
    """Verify the installer administers the install, then bind it (audit #1).

    Binds (upserts) the installation only when the user-to-server OAuth check
    confirms the caller administers the installation's account. Any failure —
    missing code, a failed verdict, or an inability to determine the verdict —
    refuses the bind, audits the rejection, and redirects to Settings with a
    ``false`` flag. Fails closed in every branch.
    """
    if not code:
        _audit_install_rejected(installation_id, "missing_identity_code")
        return _settings_redirect(installed=False)
    try:
        verified = await github_install_service.verify_installer_can_access(
            code, installation_id
        )
    except InstallVerificationError:
        logger.warning(
            "github_app_setup.verification_error installation_id=%s",
            installation_id,
        )
        _audit_install_rejected(installation_id, "verification_error")
        return _settings_redirect(installed=False)
    if not verified:
        _audit_install_rejected(installation_id, "no_installation_access")
        return _settings_redirect(installed=False)

    try:
        account = await github_install_service.fetch_installation_account(
            installation_id
        )
        await github_install_service.upsert_installation(
            db,
            installation_id=installation_id,
            user_id=user_id,
            account=account,
        )
    except InstallStateError:
        _audit_install_rejected(installation_id, "account_lookup_failed")
        return _settings_redirect(installed=False)

    return _settings_redirect(installed=True)


def _decode_payload(raw_body: bytes) -> dict[str, Any] | None:
    """The delivery body as a JSON object, or ``None`` if it is not one.

    Never raises: the signature already proved this came from GitHub, so a body
    we cannot store must degrade to "unreplayable", never to a rejected webhook.
    """
    try:
        parsed = json.loads(raw_body)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _audit_install_rejected(installation_id: int, reason: str) -> None:
    """Emit a structured audit row for a refused install bind (audit #1)."""
    github_audit(
        GITHUB_AUDIT_INSTALL_REJECTED,
        installation_id=installation_id,
        action=reason,
        status="rejected",
    )


def _settings_redirect(*, installed: bool) -> RedirectResponse:
    base = settings.frontend_url.rstrip("/")
    flag = "true" if installed else "false"
    return RedirectResponse(
        url=f"{base}/settings?github_installed={flag}",
        status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    )
