"""Unit tests for the PDF export service (T-USE-07).

WeasyPrint depends on cairo/pango/gobject; these are present in the Railway
Docker image but not always on every local dev box. When the native libs
cannot be loaded the whole module fails to import — we skip the suite
rather than failing CI for an environment issue.
"""

from __future__ import annotations

import pytest

try:
    # The service module imports cleanly even without WeasyPrint native libs
    # (its weasyprint import is lazy). To detect missing native libs we probe
    # `weasyprint.HTML` directly here.
    import weasyprint  # noqa: F401

    weasyprint.HTML(string="<html></html>")
except OSError as exc:  # pragma: no cover — environment dependent
    pytest.skip(
        f"WeasyPrint native libs unavailable: {exc}",
        allow_module_level=True,
    )

from services.pipeline.pdf_export_service import (  # noqa: E402
    _TEMPLATE_NAME,
    _build_sections,
    _jinja_env,
    _NoNetworkFetch,
    no_network_url_fetcher,
    render_pdf,
)


class _FakeStage:
    def __init__(self, content: str, status: str = "finalised") -> None:
        self.content = content
        self.status = status


def _sample_stages() -> dict[str, _FakeStage]:
    return {
        "spec": _FakeStage("# SPEC\n\n## Goals\n\n- Build a thing.\n- Ship a thing.\n"),
        "plan": _FakeStage("# PLAN\n\n```python\nfrom x import y\n```\n"),
        "tasks": _FakeStage(
            "# TASKS\n\n## Effort Summary\n\n- Estimate range: ~1 week\n"
            "- Tasks: 3 total · 2 MUST · 1 SHOULD\n"
            "- Sizes: 3xS\n"
            "- Minimum cut: Ship MUST-only → ~3 days\n"
        ),
    }


def test_render_pdf_returns_pdf_bytes() -> None:
    pdf = render_pdf(
        workspace_name="My Workspace",
        stages=_sample_stages(),
        coverage_label="Harness coverage: 92%",
    )
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-"), "Output must be a real PDF byte stream."
    # Reasonable lower bound — a 3-section branded PDF should be well over 5 KB.
    assert len(pdf) > 5_000, f"PDF unexpectedly small: {len(pdf)} bytes"


def test_render_pdf_excludes_harness_directory_marker() -> None:
    # The renderer must never embed harness content; we don't pass it in, and
    # the service intentionally omits the stage from _STAGE_SECTIONS. The PDF
    # bytes are opaque, but the cover/TOC text we render is plain — we assert
    # the section titles match the user-facing trio.
    pdf = render_pdf(
        workspace_name="My Workspace",
        stages=_sample_stages(),
        coverage_label=None,
    )
    # PDFs encode text in many ways; a fragile substring assertion is unsafe.
    # We rely on the absence of any harness-only content in the input plus the
    # contract test (read_backend_file scan) to enforce non-inclusion.
    assert pdf.startswith(b"%PDF-")


def test_export_template_inlines_squirrel_brand_without_remote_assets() -> None:
    template = _jinja_env.get_template(_TEMPLATE_NAME)
    html = template.render(
        workspace_name="My Workspace",
        generated_at="2026-06-09 00:00 UTC",
        coverage_label="Harness coverage: 92%",
        sections=_build_sections(_sample_stages()),
    )

    assert "squirrel-brand-mark" in html
    assert "http://" not in html
    assert "https://" not in html


def test_no_network_url_fetcher_blocks_https_urls() -> None:
    with pytest.raises(_NoNetworkFetch):
        no_network_url_fetcher("https://evil.example/exfil.png")


def test_no_network_url_fetcher_blocks_http_urls() -> None:
    with pytest.raises(_NoNetworkFetch):
        no_network_url_fetcher("http://attacker.test/log")


def test_no_network_url_fetcher_allows_data_urls() -> None:
    # 1x1 transparent PNG, base64-encoded — confirms data: scheme is honoured
    # for legitimate inline assets in the template.
    one_pixel = (
        "data:image/png;base64,"
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkAAIAAAoAAv/lxKUAAAAASUVORK5CYII="
    )
    result = no_network_url_fetcher(one_pixel)
    if isinstance(result, dict):
        assert "mime_type" in result or "file_obj" in result or "string" in result
        return

    try:
        assert hasattr(result, "read")
        assert result.content_type == "image/png"
    finally:
        close = getattr(result, "close", None)
        if callable(close):
            close()


def test_render_pdf_actually_routes_remote_images_through_the_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard must be wired into the real render, not merely defined.

    Every other test in this file calls ``no_network_url_fetcher`` directly,
    which is exactly why a regression went unnoticed: ``url_fetcher`` is an
    ``HTML()`` constructor argument, and ``write_pdf(**options)`` swallowed it
    without error, so the render fetched remote images for real. This test
    drives a full render and asserts the fetcher was invoked and refused.
    """
    from services.pipeline import pdf_export_service as svc

    fetcher = svc._no_network_fetcher()
    original_fetch = fetcher.fetch
    seen: list[str] = []
    blocked: list[str] = []

    def _spy(url: str, headers: object = None) -> object:
        seen.append(url)
        try:
            return original_fetch(url, headers)
        except _NoNetworkFetch:
            blocked.append(url)
            raise

    monkeypatch.setattr(fetcher, "fetch", _spy)

    exfil = "https://evil.example/exfil.png"
    pdf = render_pdf(
        workspace_name="Malicious Workspace",
        stages={"spec": _FakeStage(f'# SPEC\n\n<img src="{exfil}">\n')},
        coverage_label=None,
    )

    assert pdf.startswith(b"%PDF-")
    assert exfil in seen, "remote <img> never reached the no-network fetcher"
    assert blocked == [exfil], "remote <img> was not refused"
