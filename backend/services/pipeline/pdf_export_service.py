"""PDF export — renders SPEC.md, PLAN.md, and TASKS.md into a branded PDF.

T-USE-07 / Phase 14. Uses WeasyPrint (no headless browser). The HARNESS
directory is intentionally excluded — PDFs are for human audiences.

Security: the renderer is configured with a `no_network_url_fetcher` that
refuses every non-data URL so a malicious `<img src="https://evil/exfil">`
injected into spec content cannot exfiltrate via the resource loader at
render time (Plan §18.4). Data URLs are permitted because the inline CSS
in `export.html.j2` may rely on them.

Dependencies:
- weasyprint  : HTML/CSS → PDF renderer (cairo/pango bound). The Railway
  Python base image bundles the underlying native libraries.
- jinja2      : template engine.
- markdown    : Markdown → HTML with codehilite (Pygments) extension.
- pygments    : code-block syntax highlighting (used via the markdown
  codehilite extension; classes inlined in the template stylesheet).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import re
import threading
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import UUID

import bleach
import markdown as md
from bleach.css_sanitizer import CSSSanitizer
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings

# Note: weasyprint is imported lazily inside render_pdf so the module loads on
# dev environments without the native pango/cairo/gobject libraries — only
# the actual render path requires them. The Railway Docker image ships the
# native deps via the Dockerfile's apt-get step.
from models import Stage, Workspace
from services.coverage_utils import derive_coverage_summary
from services.observability import PDF_EXPORT_DURATION
from services.pipeline.export_service import ExportNotReadyError

logger = logging.getLogger(__name__)


# Dedicated executor isolates CPU-bound WeasyPrint rendering from Langfuse I/O calls.
# Default max_workers=2 because:
# (a) WeasyPrint is CPU-bound and creates a new HTML Document per call,
#     so two workers run without internal object contention; and
# (b) PDF export must not share the default executor with Langfuse
#     get_prompt() calls and other I/O-offloaded work — a burst of PDF
#     requests would starve those operations.  HF-4 — T-201.  LF-2 — T-211.
# The size is env-driven (settings.pdf_export_max_workers, scalability audit P2 /
# F10) so ops can raise it if PDF export becomes a hot path, without a code change;
# the per-tier _PDF_EXPORT_LIMIT caps admission separately.  Clamped to >=1.
def _pdf_executor_workers() -> int:
    return max(1, settings.pdf_export_max_workers)


def _new_pdf_executor() -> concurrent.futures.ThreadPoolExecutor:
    return concurrent.futures.ThreadPoolExecutor(
        max_workers=_pdf_executor_workers(),
        thread_name_prefix="pdf-export",
    )


_PDF_EXECUTOR = _new_pdf_executor()
_PDF_EXECUTOR_LOCK = threading.Lock()


def _get_pdf_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Return a live PDF executor, recreating it after test-client shutdowns."""
    global _PDF_EXECUTOR
    with _PDF_EXECUTOR_LOCK:
        if getattr(_PDF_EXECUTOR, "_shutdown", False):
            _PDF_EXECUTOR = _new_pdf_executor()
        return _PDF_EXECUTOR


def shutdown_pdf_executor() -> None:
    """Shut down the PDF thread pool on process exit.

    Called from the FastAPI lifespan so in-flight renders finish or are
    abandoned cleanly without leaking OS threads.  wait=False lets the
    lifespan complete immediately; threads drain naturally at interpreter exit.
    HF-4 — T-201.
    """
    with _PDF_EXECUTOR_LOCK:
        _PDF_EXECUTOR.shutdown(wait=False)


_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"
_TEMPLATE_NAME = "export.html.j2"

_STAGE_SECTIONS: tuple[tuple[str, str, str], ...] = (
    # (stage_type, section title, kicker shown above title)
    ("spec", "Specification", "Stage 1 of 3"),
    ("plan", "Implementation Plan", "Stage 2 of 3"),
    ("tasks", "Task List", "Stage 3 of 3"),
)

_jinja_env = Environment(
    loader=FileSystemLoader(_TEMPLATES_DIR),
    autoescape=select_autoescape(["html", "j2"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


class _NoNetworkFetch(Exception):
    """Raised when WeasyPrint tries to fetch a non-data URL during render."""


@lru_cache(maxsize=1)
def _no_network_fetcher() -> Any:
    """Build the WeasyPrint URL fetcher that refuses every non-`data:` URL.

    Plan §18.4 requires that PDF rendering never trigger an outbound HTTP
    request: a malicious workspace must not be able to exfiltrate via an
    `<img src="https://...">` injected into spec content. Data URLs are
    explicitly allowed so inline-encoded base64 assets in the template
    remain functional.

    WeasyPrint 70 replaced the callable ``url_fetcher`` with a ``URLFetcher``
    class and removed ``default_url_fetcher``; its internals also read
    ``url_fetcher._fail_on_errors``, so a plain function is no longer a
    usable fetcher. ``allowed_protocols`` is belt-and-braces — it re-checks
    the scheme inside the delegated ``data:`` path. ``fail_on_errors`` keeps
    its default on purpose: a blocked fetch must degrade to a missing image,
    never to a failed export.

    Built lazily and cached so the module stays importable on dev boxes
    without the native cairo/pango libraries.

    `no_network` marker — keep this token in the source for the harness
    contract test that scans for the no-network guard.
    """
    from weasyprint.urls import URLFetcher

    class _NoNetworkURLFetcher(URLFetcher):
        def fetch(self, url: str, headers: Any = None) -> Any:
            if url.startswith("data:"):
                return super().fetch(url, headers)
            logger.warning("pdf_export_blocked_url_fetch url=%s", url[:120])
            raise _NoNetworkFetch(f"Network fetch blocked: {url[:120]}")

    return _NoNetworkURLFetcher(allowed_protocols=("data",))


def no_network_url_fetcher(url: str, **_: Any) -> Any:
    """Fetch ``url`` under the no-network policy, or raise `_NoNetworkFetch`.

    Thin shim over `_no_network_fetcher` so the policy has exactly one
    implementation. WeasyPrint itself is handed the fetcher *object*, never
    this function.
    """
    return _no_network_fetcher().fetch(url)


# Boundary-local sanitization of the *rendered* HTML (audit F1 / plan §1.0).
# Stage content is stored raw — python-markdown passes inline HTML through
# verbatim, so without this pass a `<script>`/`<style>`/arbitrary tag typed
# into a stage edit lands in the document WeasyPrint renders. WeasyPrint
# executes no JS and the no_network_url_fetcher blocks outbound fetches, so
# the residual risk is layout/content injection into the user's own PDF —
# closed here by allowlisting exactly what the markdown converter emits.
# Never sanitize the markdown *source*: that is the at-rest bleach this phase
# removed (it destroys code like `List<String>`); by the time this runs, code
# spans/fences are already HTML-escaped by the converter and survive intact.
_PDF_ALLOWED_TAGS = frozenset(
    {
        "a",
        "blockquote",
        "br",
        "code",
        "div",
        "em",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "img",
        "li",
        "ol",
        "p",
        "pre",
        "span",
        "strong",
        "table",
        "tbody",
        "td",
        "th",
        "thead",
        "tr",
        "ul",
    }
)
_PDF_ALLOWED_ATTRIBUTES = {
    "a": ["href", "title"],
    # Remote img URLs are refused by no_network_url_fetcher at render time;
    # src stays allowed so data-URI images survive.
    "img": ["src", "alt", "title"],
    # class carries the codehilite syntax-highlighting hooks.
    "code": ["class"],
    "div": ["class"],
    "pre": ["class"],
    "span": ["class"],
    # The toc extension stamps ids on headings.
    "h1": ["id"],
    "h2": ["id"],
    "h3": ["id"],
    "h4": ["id"],
    "h5": ["id"],
    "h6": ["id"],
    # The tables extension expresses column alignment as inline text-align.
    "td": ["style"],
    "th": ["style"],
    "ol": ["start"],
    "li": ["value"],
}
_PDF_ALLOWED_PROTOCOLS = frozenset({"http", "https", "mailto", "data"})
_PDF_CSS_SANITIZER = CSSSanitizer(allowed_css_properties=["text-align"])
_PDF_SAFE_DATA_IMAGE = re.compile(
    r"\Adata:image/(?:png|jpeg|gif|webp);base64,[A-Za-z0-9+/]*={0,2}\Z",
    re.IGNORECASE,
)
_URL_ASCII_WHITESPACE_OR_CONTROL = re.compile(r"[\x00-\x20\x7f]+")

# bleach's strip=True removes a disallowed element but keeps its text children;
# for script/style that would leak the payload as visible PDF text. Drop those
# elements with their content first — same policy as
# services.security.sanitizer.
_PDF_SCRIPT_OR_STYLE_BLOCK = re.compile(
    r"<\s*(script|style)\b[^>]*>.*?<\s*/\s*\1\s*>",
    re.IGNORECASE | re.DOTALL,
)


def _allow_pdf_attribute(tag: str, name: str, value: str) -> bool:
    """Apply tag-aware URL policy in addition to the static allowlist.

    Bleach's ``protocols`` option is global: adding ``data`` for inline images
    also permits ``data:text/html`` on links. Keep the global parser support but
    restrict data URLs here to base64 raster image payloads on ``img[src]`` only.
    SVG is deliberately excluded because it is an active document format.
    """
    if name not in _PDF_ALLOWED_ATTRIBUTES.get(tag, ()):
        return False
    if name not in {"href", "src"}:
        return True

    compact_url = _URL_ASCII_WHITESPACE_OR_CONTROL.sub("", value)
    lowered = compact_url.lower()
    if lowered.startswith("data:"):
        return (
            tag == "img"
            and name == "src"
            and bool(_PDF_SAFE_DATA_IMAGE.fullmatch(compact_url))
        )
    return True


def _sanitize_rendered_html(html_text: str) -> str:
    """Allowlist-clean converter output before it reaches WeasyPrint."""
    without_executable_blocks = _PDF_SCRIPT_OR_STYLE_BLOCK.sub("", html_text)
    return bleach.clean(
        without_executable_blocks,
        tags=_PDF_ALLOWED_TAGS,
        attributes=_allow_pdf_attribute,
        protocols=_PDF_ALLOWED_PROTOCOLS,
        css_sanitizer=_PDF_CSS_SANITIZER,
        strip=True,
        strip_comments=True,
    )


def _render_markdown_to_html(markdown_text: str) -> str:
    """Convert a Markdown string to sanitized HTML with codehilite classes."""
    if not markdown_text:
        return ""
    rendered = md.markdown(
        markdown_text,
        extensions=[
            "fenced_code",
            "tables",
            "codehilite",
            "sane_lists",
            "toc",
        ],
        extension_configs={
            "codehilite": {
                "guess_lang": False,
                "css_class": "codehilite",
                "noclasses": False,
            },
        },
        output_format="html5",
    )
    return _sanitize_rendered_html(rendered)


def _build_sections(stages: dict[str, Stage]) -> list[dict[str, str]]:
    """Build the per-stage section objects consumed by the template."""
    sections: list[dict[str, str]] = []
    for stage_type, title, kicker in _STAGE_SECTIONS:
        stage = stages.get(stage_type)
        body = (stage.content or "") if stage else ""
        sections.append(
            {
                "stage_type": stage_type,
                "title": title,
                "kicker": kicker,
                "html": _render_markdown_to_html(body),
            }
        )
    return sections


def _coverage_label(coverage_summary: Any) -> str | None:
    """One-line coverage chip text for the cover page, or None if unknown.

    Accepts a pre-computed CoverageSummary (Pydantic model), a plain dict,
    or a string.  The caller is responsible for deriving the value from the
    database — the ORM Workspace model has no coverage_summary attribute
    (it is a computed Pydantic-only field that always returns None when read
    via getattr on an ORM instance).  M-1 — T-183.
    """
    summary = coverage_summary
    if not summary:
        return None
    # Handle Pydantic CoverageSummary objects and duck-typed equivalents.
    pct = getattr(summary, "percent", None)
    if isinstance(pct, (int, float)):
        return f"Harness coverage: {int(pct)}%"
    covered = getattr(summary, "covered", None)
    total = getattr(summary, "total", None)
    if covered is not None and total:
        return f"Harness coverage: {covered}/{total}"
    # Fallback: plain dict (legacy callers).
    if isinstance(summary, dict):
        dict_pct = summary.get("coverage_percent") or summary.get("percent")
        if isinstance(dict_pct, (int, float)):
            return f"Harness coverage: {int(dict_pct)}%"
        dict_covered = summary.get("covered_count") or summary.get("covered")
        dict_total = summary.get("total_count") or summary.get("total")
        if dict_covered is not None and dict_total:
            return f"Harness coverage: {dict_covered}/{dict_total}"
        if "label" in summary:
            return str(summary["label"])
    if isinstance(summary, str):
        return summary
    return None


def _render_pdf_sync(html_text: str) -> bytes:
    """Synchronous WeasyPrint render — must be called via run_in_executor.

    WeasyPrint is CPU-bound and holds the GIL for the full render duration
    (typically 0.5–3 s). Keeping it in a standalone module-level function
    allows the async caller to dispatch it to the dedicated thread pool,
    leaving the event loop free to serve other requests.  C-4 — T-176.

    `no_network` marker — keep this token in the source for the harness
    contract test that scans for the no-network guard.
    """
    import time

    # Lazy import — keeps the module loadable on dev boxes without the
    # native cairo/pango libs.
    from weasyprint import HTML

    _start = time.perf_counter()
    # `url_fetcher` is an HTML() constructor argument. write_pdf() takes
    # **options and silently swallows unknown keywords, so passing it there
    # left this guard inert and every remote <img> was fetched for real.
    pdf_bytes = HTML(string=html_text, url_fetcher=_no_network_fetcher()).write_pdf()
    # Observe render duration in the Prometheus histogram.  T-194.
    PDF_EXPORT_DURATION.observe(time.perf_counter() - _start)
    if not pdf_bytes:
        # WeasyPrint returns bytes or None on certain failure modes; we
        # treat None as a hard failure so callers don't ship empty PDFs.
        raise RuntimeError("PDF rendering returned no bytes")
    return pdf_bytes


def render_pdf(
    *,
    workspace_name: str,
    stages: dict[str, Stage],
    coverage_label: str | None,
) -> bytes:
    """Pure synchronous render entry point — used by tests without a DB.

    Calling this directly from an async context blocks the event loop; use
    render() instead, which dispatches via run_in_executor.
    """
    template = _jinja_env.get_template(_TEMPLATE_NAME)
    html_text = template.render(
        workspace_name=workspace_name,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        coverage_label=coverage_label,
        sections=_build_sections(stages),
    )
    return _render_pdf_sync(html_text)


async def render(
    workspace_id: UUID, user_id: UUID, db: AsyncSession
) -> tuple[bytes, str]:
    """Render the PDF for the given workspace.

    Returns (pdf_bytes, filename_slug). Raises ExportNotReadyError if any
    of the three user-facing stages are not finalised.
    """
    ws_result = await db.execute(select(Workspace).where(Workspace.id == workspace_id))
    workspace = ws_result.scalar_one_or_none()
    if workspace is None or workspace.user_id != user_id:
        raise ExportNotReadyError("Workspace not found")

    stages_result = await db.execute(
        select(Stage).where(Stage.workspace_id == workspace_id)
    )
    stages = {s.type: s for s in stages_result.scalars()}

    # Harness stage must exist for the cover-page coverage chip to be meaningful,
    # but we only require the three user-facing stages to be finalised for the
    # PDF body itself.
    for stage_type in ("spec", "plan", "tasks"):
        stage = stages.get(stage_type)
        if stage is None or stage.status != "finalised":
            raise ExportNotReadyError(
                f"Stage {stage_type!r} is not finalised — PDF export unavailable"
            )

    # Derive coverage_summary explicitly — the ORM Workspace has no such
    # attribute; it is only on the Pydantic response schema.  M-1 — T-183.
    # Imported from the shared coverage_utils module (not public_share_service)
    # to avoid cross-module private imports.  MF-2 — T-206.
    coverage_summary = await derive_coverage_summary(workspace_id, db)
    template = _jinja_env.get_template(_TEMPLATE_NAME)
    html_text = template.render(
        workspace_name=workspace.name,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        coverage_label=_coverage_label(coverage_summary),
        sections=_build_sections(stages),
    )
    # Dispatch WeasyPrint to the dedicated PDF thread pool so the event loop is
    # not blocked during the CPU-bound render (typically 0.5–3 s).
    # get_running_loop() is used (not the deprecated 3.10+ get_event_loop
    # variant) — it raises RuntimeError if no loop is running, making bugs
    # explicit.  C-4 — T-176.  HF-4 — T-201.
    pdf_bytes = await asyncio.get_running_loop().run_in_executor(
        _get_pdf_executor(), _render_pdf_sync, html_text
    )
    slug = _safe_filename_slug(workspace.name)
    return pdf_bytes, slug


def _safe_filename_slug(name: str) -> str:
    """Squash to ASCII-safe filename component; never empty."""
    cleaned = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in (name or "")
    ).strip("-_")
    return cleaned[:60] or "workspace"


async def render_html_to_pdf(html_text: str) -> bytes:
    """Render a trusted, fully-inlined HTML document to PDF bytes.

    Shared no-network PDF entry point: dispatches the CPU-bound WeasyPrint render
    to the dedicated PDF thread pool (so the event loop is never blocked) and
    applies ``no_network_url_fetcher`` so a render can never trigger an outbound
    HTTP request. Callers own the HTML and are responsible for escaping/
    sanitising any untrusted content before it reaches this function.

    Reused by the Storyboard renderer (T-255) so both PDF surfaces share one
    executor, one no-network guard, and one render-duration metric.
    """
    return await asyncio.get_running_loop().run_in_executor(
        _get_pdf_executor(), _render_pdf_sync, html_text
    )
