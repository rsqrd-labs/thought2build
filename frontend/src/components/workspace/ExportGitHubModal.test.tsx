import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react"
import { MemoryRouter } from "react-router"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { ExportGitHubModal } from "./ExportGitHubModal"
import {
  exportWorkspaceToGitHub,
  getGitHubInstallations,
  getGitHubPush,
  getGitHubRepositories,
} from "../../services/api"
import type { IntegrationPushRead } from "../../services/api"
import type {
  InstallationList,
  InstallationOption,
  RepoList,
} from "../../types/github"

vi.mock("../../services/api", () => ({
  exportWorkspaceToGitHub: vi.fn(),
  getGitHubInstallations: vi.fn(),
  getGitHubPush: vi.fn(),
  getGitHubRepositories: vi.fn(),
  // The real helper digs the backend `detail` out of an axios error; the modal
  // only needs the fallback for our cases.
  getApiErrorMessage: (_e: unknown, fallback: string) => fallback,
}))

const mockInstalls = vi.mocked(getGitHubInstallations)
const mockExport = vi.mocked(exportWorkspaceToGitHub)
const mockPush = vi.mocked(getGitHubPush)
const mockRepos = vi.mocked(getGitHubRepositories)

function installation(
  overrides: Partial<InstallationOption> = {},
): InstallationOption {
  return {
    id: "inst-row-1",
    installation_id: 4242,
    account_login: "octocat",
    account_type: "Organization",
    repository_selection: "all",
    suspended: false,
    ...overrides,
  }
}

function installed(suspended = false): InstallationList {
  return {
    installations: [installation({ suspended })],
    on_legacy_oauth: false,
  }
}

function push(overrides: Partial<IntegrationPushRead> = {}): IntegrationPushRead {
  return {
    push_id: "push-1",
    status: "pending",
    repo_full_name: "octocat/my-spec",
    repo_url: "https://github.com/octocat/my-spec",
    issue_count: 4,
    installation_id: "inst-row-1",
    pushed_at: null,
    ...overrides,
  }
}

function repoList(overrides: Partial<RepoList> = {}): RepoList {
  return {
    repositories: [
      {
        id: 11,
        name: "api-server",
        full_name: "octocat/api-server",
        private: true,
        html_url: "https://github.com/octocat/api-server",
      },
      {
        id: 12,
        name: "docs-site",
        full_name: "octocat/docs-site",
        private: false,
        html_url: "https://github.com/octocat/docs-site",
      },
    ],
    truncated: false,
    can_create: true,
    ...overrides,
  }
}

function renderModal(props: Partial<Parameters<typeof ExportGitHubModal>[0]> = {}) {
  const onClose = vi.fn()
  render(
    <MemoryRouter>
      <ExportGitHubModal
        workspaceId="ws-1"
        workspaceName="My Spec"
        taskCount={4}
        onClose={onClose}
        {...props}
      />
    </MemoryRouter>,
  )
  return { onClose }
}

beforeEach(() => {
  // Defaults every test starts from: an unbound workspace (no push yet) whose
  // installation has no listable repos but can create — this reproduces the
  // pre-picker "type a name" configure phase, so the legacy-flow tests below
  // exercise the same submit path they always did.
  mockPush.mockResolvedValue(null)
  mockRepos.mockResolvedValue(repoList({ repositories: [] }))
})

afterEach(() => {
  vi.clearAllMocks()
})

describe("ExportGitHubModal", () => {
  it("prompts to install when there is no active installation", async () => {
    mockInstalls.mockResolvedValue({ installations: [], on_legacy_oauth: false })

    renderModal()

    expect(
      await screen.findByText(/install the github app in settings/i),
    ).toBeInTheDocument()
    expect(
      screen.getByRole("button", { name: /open settings/i }),
    ).toBeInTheDocument()
  })

  it("offers the Files vs PR-with-tests choice, with Files recommended + selected", async () => {
    mockInstalls.mockResolvedValue(installed())

    renderModal()

    const group = await screen.findByRole("radiogroup", { name: /export mode/i })
    const files = within(group).getByRole("radio", { name: /files/i })
    const pr = within(group).getByRole("radio", { name: /pr with tests/i })

    expect(files).toHaveAttribute("aria-checked", "true")
    expect(pr).toHaveAttribute("aria-checked", "false")
    expect(within(group).getByText(/recommended/i)).toBeInTheDocument()
    expect(
      within(group).getByText(/commit the four files \+ harness/i),
    ).toBeInTheDocument()
    expect(
      within(group).getByText(/open a pull request with failing tests/i),
    ).toBeInTheDocument()
  })

  it("slides in the concrete branch preview when PR-with-tests is chosen", async () => {
    mockInstalls.mockResolvedValue(installed())

    renderModal()

    const pr = await screen.findByRole("radio", { name: /pr with tests/i })
    expect(screen.queryByText(/thought2build\/inc-1/i)).not.toBeInTheDocument()

    fireEvent.click(pr)

    expect(await screen.findByText(/thought2build\/inc-1/i)).toBeInTheDocument()
  })

  it("submits with the installation_id + export_mode, then polls to a success state", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockExport.mockResolvedValue(push({ status: "pending", repo_url: null }))
    // Mount read: no push yet (unbound). Then the post-submit polls: one
    // pending, then completed with the repo url.
    mockPush
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(push({ status: "pending" }))
      .mockResolvedValue(push({ status: "completed" }))

    renderModal()

    // Choose PR mode so we also assert the mode is forwarded.
    fireEvent.click(await screen.findByRole("radio", { name: /pr with tests/i }))
    fireEvent.click(screen.getByRole("button", { name: /export/i }))

    await waitFor(() =>
      expect(mockExport).toHaveBeenCalledWith(
        "ws-1",
        {
          repo_name: "my-spec",
          visibility: "public",
          installation_id: "inst-row-1",
          export_mode: "pr_with_tests",
        },
        expect.any(AbortSignal),
      ),
    )

    // Staged progress (not a bare spinner): the per-mode stages render.
    expect(await screen.findByText(/creating branch/i)).toBeInTheDocument()

    // Poll drives the terminal transition.
    expect(
      await screen.findByText(/pull request opened/i, undefined, { timeout: 4000 }),
    ).toBeInTheDocument()
    const link = screen.getByRole("link", {
      name: /github\.com\/octocat\/my-spec/i,
    })
    expect(link).toHaveAttribute("href", "https://github.com/octocat/my-spec")
  })

  it("locks a bound workspace to its repo and does not consume the prior terminal status before the 202", async () => {
    mockInstalls.mockResolvedValue(installed())
    // Hold the POST so we can observe the window where a stale push could leak.
    let resolveExport: (v: IntegrationPushRead) => void = () => {}
    mockExport.mockReturnValue(
      new Promise<IntegrationPushRead>((resolve) => {
        resolveExport = resolve
      }),
    )
    // The workspace already exported: the push row reads completed and is
    // bound to octocat/my-spec.
    mockPush.mockResolvedValue(push({ status: "completed" }))

    renderModal()

    // Bound view: the connected repo is shown as a locked banner — no picker,
    // no free-text name, no repo-list fetch (a bound push ignores them all).
    expect(await screen.findByText("octocat/my-spec")).toBeInTheDocument()
    expect(
      screen.getByText(/updates this repository's files and issues in place/i),
    ).toBeInTheDocument()
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument()
    expect(screen.queryByLabelText(/filter repositories/i)).not.toBeInTheDocument()
    expect(mockRepos).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole("button", { name: /update export/i }))

    // The submit derives the bound repo's bare name.
    await waitFor(() =>
      expect(mockExport).toHaveBeenCalledWith(
        "ws-1",
        expect.objectContaining({
          repo_name: "my-spec",
          installation_id: "inst-row-1",
        }),
        expect.any(AbortSignal),
      ),
    )

    // Staged progress is showing, but polling is gated on the 202: past a poll
    // interval, only the single mount read may have happened — the stale
    // "completed" must not have been consumed as this export's outcome.
    expect(await screen.findByRole("list")).toBeInTheDocument()
    await new Promise((r) => setTimeout(r, 1700))
    expect(screen.queryByText(/exported to github/i)).not.toBeInTheDocument()
    expect(mockPush).toHaveBeenCalledTimes(1)

    // Once the POST resolves, polling begins and the export completes.
    resolveExport(push({ status: "pending", repo_url: null }))
    expect(
      await screen.findByText(/exported to github/i, undefined, { timeout: 4000 }),
    ).toBeInTheDocument()
  })

  it("tells the user an already-completed export is already complete", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockPush.mockResolvedValue(
      push({ status: "completed", pushed_at: "2026-07-12T12:00:00" }),
    )

    renderModal()

    // The prior push's outcome is surfaced, with when it landed — never a
    // plain fresh-export screen for a workspace that already exported.
    const badge = await screen.findByText(/already exported/i)
    expect(badge.textContent).toMatch(/already exported — .+/i)
    expect(
      screen.getByText(/never duplicates them/i),
    ).toBeInTheDocument()
    // The copy flips to re-export language: issues sync in place, the CTA is
    // an update, not a first export.
    expect(screen.getByText(/4 issues will be synced/i)).toBeInTheDocument()
    expect(
      screen.getByRole("button", { name: /update export/i }),
    ).toBeInTheDocument()
    expect(
      screen.queryByRole("button", { name: /^export$/i }),
    ).not.toBeInTheDocument()
  })

  it("explains drift on a stale push: exported before, workspace moved on", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockPush.mockResolvedValue(
      push({ status: "stale", pushed_at: "2026-07-10T12:00:00" }),
    )

    renderModal()

    expect(await screen.findByText(/already exported/i)).toBeInTheDocument()
    expect(
      screen.getByText(/changed since that export/i),
    ).toBeInTheDocument()
  })

  it("frames a re-export after a failed push as a safe retry, not a first export", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockPush.mockResolvedValue(push({ status: "failed" }))

    renderModal()

    expect(
      await screen.findByText(/the last export didn't finish/i),
    ).toBeInTheDocument()
    // A failed push never claims "already exported".
    expect(screen.queryByText(/already exported/i)).not.toBeInTheDocument()
    expect(
      screen.getByRole("button", { name: /update export/i }),
    ).toBeInTheDocument()
  })

  it("keeps first-export copy for an unbound workspace", async () => {
    mockInstalls.mockResolvedValue(installed())

    renderModal()

    expect(
      await screen.findByText(/4 issues will be created/i),
    ).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /^export$/i })).toBeInTheDocument()
    expect(screen.queryByText(/already exported/i)).not.toBeInTheDocument()
  })

  it("keeps polling past the still-working hand-off and lands on done by itself", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      mockInstalls.mockResolvedValue(installed())
      mockExport.mockResolvedValue(push({ status: "pending", repo_url: null }))
      // Mount read: unbound. Every poll after submit: still pending — a slow,
      // many-issue export that outlives the fast-poll window.
      mockPush
        .mockResolvedValueOnce(null)
        .mockResolvedValue(push({ status: "pending", repo_url: null }))

      renderModal()
      // Wait for the repo section to settle (submit is disabled while the
      // repo list loads) before submitting.
      await screen.findByDisplayValue("my-spec")
      fireEvent.click(screen.getByRole("button", { name: /^export$/i }))

      // Let the 202 resolve and polling begin, then run out the 40 fast polls
      // (~60s) → the calm hand-off, never a red error.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5_000)
      })
      await act(async () => {
        await vi.advanceTimersByTimeAsync(65_000)
      })
      expect(await screen.findByText(/still working/i)).toBeInTheDocument()

      // The worker finishes AFTER the hand-off: the continued slow poll must
      // surface it as a visible done state — the user never has to go verify
      // on GitHub by hand.
      mockPush.mockResolvedValue(push({ status: "completed" }))
      await act(async () => {
        await vi.advanceTimersByTimeAsync(11_000)
      })
      expect(await screen.findByText(/exported to github/i)).toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })

  it("lists the installation's repositories and submits the picked one", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockRepos.mockResolvedValue(repoList())
    mockExport.mockResolvedValue(push({ status: "pending", repo_url: null }))

    renderModal()

    // Existing-repo mode is the default when the list is non-empty.
    // RepoPicker renders the listbox unconditionally and `repoChoiceMode`
    // starts as "existing", so an EMPTY listbox is in the DOM on first paint —
    // findByRole("listbox") resolves before the repo fetch settles. Await the
    // row itself rather than querying it synchronously off that early match.
    const listbox = await screen.findByRole("listbox", { name: /repositories/i })
    const row = await within(listbox).findByRole("button", { name: /api-server/i })
    fireEvent.click(row)

    // Picking an existing repo never shows the visibility choice (it only
    // applies when the export may create the repo).
    expect(screen.queryByText(/visibility/i)).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: /^export$/i }))
    await waitFor(() =>
      expect(mockExport).toHaveBeenCalledWith(
        "ws-1",
        expect.objectContaining({ repo_name: "api-server" }),
        expect.any(AbortSignal),
      ),
    )
  })

  it("disables Export in existing mode until a repository is picked", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockRepos.mockResolvedValue(repoList())

    renderModal()

    await screen.findByRole("listbox", { name: /repositories/i })
    expect(screen.getByRole("button", { name: /^export$/i })).toBeDisabled()
  })

  it("filters the repository list case-insensitively", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockRepos.mockResolvedValue(repoList())

    renderModal()

    // The configure phase briefly renders before the repository request flips
    // its loading flag. Wait for fetched data, not that transient picker, so
    // the test cannot race the loading placeholder under a busy CI runner.
    await screen.findByRole("button", { name: /docs-site/i })
    const filter = screen.getByLabelText(/filter repositories/i)
    fireEvent.change(filter, { target: { value: "DOCS" } })

    const listbox = screen.getByRole("listbox", { name: /repositories/i })
    expect(within(listbox).getByText("docs-site")).toBeInTheDocument()
    expect(within(listbox).queryByText("api-server")).not.toBeInTheDocument()
  })

  it("keeps the create-new framing only for an org install on all repositories", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockRepos.mockResolvedValue(repoList({ can_create: true }))

    renderModal()

    // Org + all-repos: the manual-mode toggle is framed as creation…
    const createLink = await screen.findByRole("button", {
      name: /create a new repository instead/i,
    })
    fireEvent.click(createLink)
    expect(
      await screen.findByText(/creating it first if it doesn't exist/i),
    ).toBeInTheDocument()
    // …and the visibility choice appears (creation is its only consumer).
    expect(screen.getByText(/visibility/i)).toBeInTheDocument()
  })

  it("personal-account install never offers repo creation", async () => {
    mockInstalls.mockResolvedValue({
      installations: [
        installation({ account_type: "User", repository_selection: "selected" }),
      ],
      on_legacy_oauth: false,
    })
    mockRepos.mockResolvedValue(repoList({ can_create: false }))

    renderModal()

    // The initial picker renders before the repository request settles. Its
    // response chooses the default mode, so clicking the transient toggle can
    // be overwritten. Wait for fetched data before exercising manual mode.
    await screen.findByRole("button", { name: /api-server/i })
    expect(
      screen.queryByRole("button", { name: /create a new repository/i }),
    ).not.toBeInTheDocument()
    const manualLink = screen.getByRole("button", {
      name: /type a repository name instead/i,
    })
    fireEvent.click(manualLink)
    expect(
      await screen.findByText(/must be an existing repository/i),
    ).toBeInTheDocument()
    expect(screen.queryByText(/visibility/i)).not.toBeInTheDocument()
  })

  it("shows the add-repositories empty state for a personal install with no repos", async () => {
    mockInstalls.mockResolvedValue({
      installations: [
        installation({ account_type: "User", repository_selection: "selected" }),
      ],
      on_legacy_oauth: false,
    })
    mockRepos.mockResolvedValue(
      repoList({ repositories: [], can_create: false }),
    )

    renderModal()

    const manageLink = await screen.findByRole("link", {
      name: /add repositories on github/i,
    })
    // Personal account → the user-scoped installations settings page.
    expect(manageLink).toHaveAttribute(
      "href",
      "https://github.com/settings/installations/4242",
    )
  })

  it("falls back to manual name entry with a retry when the repo list fails to load", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockRepos
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValue(repoList())

    renderModal()

    // Failure is NOT an empty list: a retry notice + the name input remain.
    expect(
      await screen.findByText(/couldn't load your repositories/i),
    ).toBeInTheDocument()
    expect(screen.getByDisplayValue("my-spec")).toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: /retry/i }))

    // The retry refetches and lands in the picker.
    expect(
      await screen.findByRole("listbox", { name: /repositories/i }),
    ).toBeInTheDocument()
    expect(mockRepos).toHaveBeenCalledTimes(2)
  })

  it("offers an account selector for multi-install users and refetches on switch", async () => {
    const second = installation({
      id: "inst-row-2",
      installation_id: 7777,
      account_login: "acme-org",
    })
    mockInstalls.mockResolvedValue({
      installations: [installation(), second],
      on_legacy_oauth: false,
    })
    mockRepos.mockResolvedValue(repoList())

    renderModal()

    const select = await screen.findByLabelText(/github account/i)
    await waitFor(() => expect(mockRepos).toHaveBeenCalledWith("inst-row-1"))

    fireEvent.change(select, { target: { value: "inst-row-2" } })
    await waitFor(() => expect(mockRepos).toHaveBeenCalledWith("inst-row-2"))
  })

  it("maps a 403 on submit back to the install prompt (no red error)", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockExport.mockRejectedValue({ response: { status: 403 } })

    renderModal()

    // Export is disabled while `reposLoading` is true, and a click on a
    // disabled button is a silent no-op — so wait for it to actually enable
    // rather than clicking the moment it appears in the DOM.
    const exportButton = await screen.findByRole("button", { name: /^export$/i })
    await waitFor(() => expect(exportButton).toBeEnabled())
    fireEvent.click(exportButton)

    expect(
      await screen.findByText(/no longer available|reconnect it in settings/i),
    ).toBeInTheDocument()
    expect(screen.queryByText(/^export failed/i)).not.toBeInTheDocument()
  })

  it("surfaces a failed export and offers retry", async () => {
    mockInstalls.mockResolvedValue(installed())
    mockExport.mockResolvedValue(push({ status: "pending", repo_url: null }))
    mockPush
      .mockResolvedValueOnce(null)
      .mockResolvedValue(push({ status: "failed" }))

    renderModal()

    // Same disabled-until-`reposLoading`-clears race as the 403 test above.
    const exportButton = await screen.findByRole("button", { name: /^export$/i })
    await waitFor(() => expect(exportButton).toBeEnabled())
    fireEvent.click(exportButton)

    expect(
      await screen.findByText(/couldn't finish this export/i, undefined, {
        timeout: 4000,
      }),
    ).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /try again/i })).toBeInTheDocument()
  })
})
