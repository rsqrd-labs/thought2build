"""PR-with-tests export scaffold builder (Phase 21 — T-276).

Pure, deterministic, zero-I/O. Given the four finalised stages and the parsed
tasks, it produces the *additional* artifacts that turn a plain file export into
an **executable pull request**: a CI workflow, a per-stack red test runner, and
one failing stub test per task tagged with its stable ``task_ref``. The repo
starts red (the harness fails) and goes green as work lands — TDD-from-spec.

This is the *user's* repo harness, distinct from Thought2Build's own contract tests.

Stack handling is the load-bearing subtlety. The canonical Thought2Build workspace is
full-stack — the Harness stage carries both ``.py`` and ``.ts`` contract files —
so a single-stack guess would silently drop half the contracts (no runner, the
harness only half-red). :func:`detect_stacks` therefore returns the *set* of
stacks present, and the CI workflow + scaffold cover every one of them.

The orchestration (branch/PR plumbing, persistence) lives in
``github_export_service``; this module only computes the bytes to push.
"""

from __future__ import annotations

from services.integrations.task_parser import ParsedTask

# Stable on-branch location for the generated scaffold so re-export updates the
# same files in place rather than littering new ones.
CI_WORKFLOW_PATH = ".github/workflows/thought2build.yml"
_STUB_DIR = "tests/thought2build"

# Supported stacks → the test runner the CI workflow drives.
STACK_PYTHON = "python"
STACK_NODE = "node"

_PYTHON_HARNESS_SUFFIXES = (".py",)
_NODE_HARNESS_SUFFIXES = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs")

_PYTHON_PLAN_HINTS = (
    "pytest",
    "fastapi",
    "django",
    "flask",
    "sqlalchemy",
    " python",
    "uv run",
    "pip install",
)
_NODE_PLAN_HINTS = (
    "vitest",
    "jest",
    "react",
    "typescript",
    "vite",
    "npm ",
    "pnpm",
    "node ",
    "next.js",
)


def detect_stacks(plan_content: str, harness_files: dict[str, str]) -> list[str]:
    """Return the ordered set of stacks the harness/plan target.

    The harness file extensions are the strongest signal (they are the actual
    contract files); the Plan text is a fallback when the harness layout is
    ambiguous. Always returns at least one stack (``python`` as the safe default)
    so a workspace can never export a PR with no test runner at all.
    """
    stacks: list[str] = []

    for path in harness_files:
        lowered = path.lower()
        if lowered.endswith(_PYTHON_HARNESS_SUFFIXES) and STACK_PYTHON not in stacks:
            stacks.append(STACK_PYTHON)
        elif lowered.endswith(_NODE_HARNESS_SUFFIXES) and STACK_NODE not in stacks:
            stacks.append(STACK_NODE)

    if not stacks:
        text = plan_content.lower()
        if any(hint in text for hint in _PYTHON_PLAN_HINTS):
            stacks.append(STACK_PYTHON)
        if any(hint in text for hint in _NODE_PLAN_HINTS):
            stacks.append(STACK_NODE)

    if not stacks:
        stacks.append(STACK_PYTHON)
    return stacks


def build_scaffold(
    *,
    harness_files: dict[str, str],
    tasks: list[ParsedTask],
    stacks: list[str],
) -> dict[str, str]:
    """Build the {path: content} map pushed to the increment branch.

    Disjoint from the docs pushed to the default branch — the harness contracts,
    the CI workflow, and the per-task red stubs all live only on the PR branch,
    so the PR is a clean "here is the executable red harness" diff.
    """
    files: dict[str, str] = {}
    # The user's harness contract files (the tests that define done).
    files.update(harness_files)
    # CI that runs them — one job per detected stack.
    files[CI_WORKFLOW_PATH] = _build_ci_workflow(stacks)
    # One failing stub per task, per stack, tagged with the stable task_ref.
    for task in tasks:
        for stack in stacks:
            path, content = _task_stub(task, stack)
            files[path] = content
    return files


def build_pr_body(
    *,
    tasks: list[ParsedTask],
    issue_numbers: dict[str, int],
    stacks: list[str],
) -> str:
    """Render the PR body: an executable-harness summary + ``Closes #N`` links.

    Each task's issue is linked with ``Closes #N`` so merging the PR closes the
    matching issues, tying issues to the tests that must pass.
    """
    stack_label = ", ".join(stacks)
    lines = [
        "## Thought2Build — executable harness",
        "",
        (
            "This pull request opens the Harness stage as an **executable spec**: "
            "failing tests, a CI workflow, and one stub per task. The harness starts "
            "**red** and goes green as each task is implemented."
        ),
        "",
        f"**Stacks:** {stack_label}",
        "",
        "### Tasks",
        "",
    ]
    for task in tasks:
        # ``issue_numbers`` is keyed on the stable compute_task_ref (audit #2);
        # the human ``T-NNN`` stays in the link's display text only.
        number = issue_numbers.get(task.task_ref)
        if number is not None:
            lines.append(f"- Closes #{number} — {task.ref}: {task.title}")
        else:
            lines.append(f"- {task.ref}: {task.title}")
    lines.append("")
    lines.append("*Generated by [Thought2Build](https://thought2build.com)*")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _build_ci_workflow(stacks: list[str]) -> str:
    """A GitHub Actions workflow with one harness job per detected stack."""
    jobs: list[str] = []
    if STACK_PYTHON in stacks:
        jobs.append(_PYTHON_JOB)
    if STACK_NODE in stacks:
        jobs.append(_NODE_JOB)
    body = "\n".join(jobs)
    return _CI_WORKFLOW_HEADER + body


def _task_stub(task: ParsedTask, stack: str) -> tuple[str, str]:
    """A single failing stub test for a task, tagged with its stable task_ref."""
    task_ref = task.task_ref
    slug = task_ref.replace("-", "_")
    title = _escape(task.title)
    if stack == STACK_NODE:
        path = f"{_STUB_DIR}/{task_ref}.test.ts"
        content = _NODE_STUB.format(task_ref=task_ref, ref=task.ref, title=title)
    else:
        path = f"{_STUB_DIR}/test_{slug}.py"
        content = _PYTHON_STUB.format(
            task_ref=task_ref, ref=task.ref, slug=slug, title=title
        )
    return path, content


def _escape(text: str) -> str:
    """Neutralise characters that would break a comment / string literal in the
    generated stub (the title is workspace-controlled, untrusted input)."""
    return text.replace("\\", "\\\\").replace('"', "'").replace("\n", " ").strip()


_CI_WORKFLOW_HEADER = """\
# Generated by Thought2Build — the executable harness for this workspace.
name: Thought2Build Harness

on:
  push:
  pull_request:

jobs:
"""

_PYTHON_JOB = """\
  python-harness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install test deps
        run: pip install pytest
      - name: Run the Thought2Build harness (red until implemented)
        run: pytest -q
"""

_NODE_JOB = """\
  node-harness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - name: Install deps
        run: npm install
      - name: Run the Thought2Build harness (red until implemented)
        run: npx vitest run
"""

_PYTHON_STUB = """\
# Thought2Build task stub — task_ref: {task_ref}
# {ref}: {title}
#
# This test fails on purpose. Implement {ref}, then replace the failing
# assertion below with a real test that proves it.


def test_{slug}() -> None:
    raise AssertionError("Not implemented yet — {ref}: {title}")
"""

_NODE_STUB = """\
// Thought2Build task stub — task_ref: {task_ref}
// {ref}: {title}
//
// This test fails on purpose. Implement {ref}, then replace the failing
// assertion below with a real test that proves it.
import {{ describe, expect, it }} from "vitest"

describe("{ref}: {title}", () => {{
  it("is implemented", () => {{
    expect(false).toBe(true)
  }})
}})
"""
