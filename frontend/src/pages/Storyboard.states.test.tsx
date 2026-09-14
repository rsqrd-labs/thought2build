import { act, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { MemoryRouter, Route, Routes } from "react-router"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { DEFAULT_TEST_PERMISSIONS, makeStoryboardPayload } from "../components/storyboard/testPayload"
import type { StoryboardDetail } from "../types/storyboard"

const api = vi.hoisted(() => ({
  getStoryboard: vi.fn(),
  getApiErrorMessage: vi.fn((_error: unknown) => "Storyboard service unavailable"),
}))

vi.mock("axios", () => ({
  default: {
    isAxiosError: (error: unknown) => Boolean(
      error && typeof error === "object" && "isAxiosError" in error,
    ),
  },
}))
vi.mock("../services/api", () => api)
vi.mock("../components/storyboard/StoryboardDeck", () => ({
  StoryboardDeck: (props: { isLoading?: boolean; isNotFound?: boolean; onExit?: () => void }) => (
    <div>
      <span>{props.isLoading ? "deck loading" : props.isNotFound ? "deck missing" : "deck ready"}</span>
      {props.onExit && <button onClick={props.onExit}>Exit deck</button>}
    </div>
  ),
}))
vi.mock("../components/storyboard/StoryboardLaunchPage", () => ({
  StoryboardLaunchPage: (props: {
    onPresent: () => void
    onDownload: () => void
    onNotes: () => void
    onShare: () => void
  }) => (
    <div>
      <button onClick={props.onPresent}>Present deck</button>
      <button onClick={props.onDownload}>Toggle downloads</button>
      <button onClick={props.onNotes}>Toggle notes</button>
      <button onClick={props.onShare}>Share deck</button>
    </div>
  ),
}))
vi.mock("../components/storyboard/StoryboardDownloadMenu", () => ({
  StoryboardDownloadMenu: ({ onClose }: { onClose: () => void }) => (
    <div>Download menu<button onClick={onClose}>Close downloads</button></div>
  ),
}))
vi.mock("../components/storyboard/PresenterMode", () => ({
  PresenterMode: ({ onClose }: { onClose: () => void }) => (
    <div>Presenter notes<button onClick={onClose}>Close notes</button></div>
  ),
}))
vi.mock("../components/storyboard/StoryboardShareModal", () => ({
  StoryboardShareModal: (props: {
    initialEnabled: boolean
    initialSlug: string | null
    onClose: () => void
    onShareChanged: (next: {
      enabled: boolean
      slug: string | null
      permissions: typeof DEFAULT_TEST_PERMISSIONS
    }) => void
  }) => (
    <div>
      Share modal {String(props.initialEnabled)} {props.initialSlug ?? "no-slug"}
      <button onClick={() => props.onShareChanged({
        enabled: true,
        slug: "published-slug",
        permissions: DEFAULT_TEST_PERMISSIONS,
      })}>Publish share</button>
      <button onClick={props.onClose}>Close share</button>
    </div>
  ),
}))

import Storyboard from "./Storyboard"

function storyboard(overrides: Partial<StoryboardDetail> = {}): StoryboardDetail {
  return {
    id: "story-1",
    workspace_id: "workspace-1",
    version: 2,
    status: "ready",
    title: "Launch Storyboard",
    theme: "Copper",
    content: makeStoryboardPayload(),
    source_map: {},
    permissions: DEFAULT_TEST_PERMISSIONS,
    public_share_enabled: false,
    public_share_slug: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    ...overrides,
  }
}

function renderPage(
  initialEntry: string | { pathname: string; state?: unknown } = "/storyboards/story-1",
  routePath = "/storyboards/:id",
) {
  return render(
    <MemoryRouter initialEntries={[initialEntry]}>
      <Routes>
        <Route path={routePath} element={<Storyboard />} />
        <Route path="/dashboard" element={<div>Dashboard destination</div>} />
        <Route path="/workspace/:id" element={<div>Workspace destination</div>} />
      </Routes>
    </MemoryRouter>,
  )
}

describe("Storyboard owner states", () => {
  beforeEach(() => {
    api.getStoryboard.mockReset()
    api.getApiErrorMessage.mockClear()
    document.title = "Thought2Build"
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it("renders a missing-id state and returns to the dashboard", async () => {
    renderPage("/storyboards", "/storyboards")
    expect(await screen.findByText("deck missing")).toBeInTheDocument()
    fireEvent.click(screen.getAllByRole("button", { name: "Back to dashboard" })[0])
    expect(screen.getByText("Dashboard destination")).toBeInTheDocument()
    expect(api.getStoryboard).not.toHaveBeenCalled()
  })

  it("treats an owner-scoped 404 as not found and preserves the workspace return target", async () => {
    api.getStoryboard.mockRejectedValue({ isAxiosError: true, response: { status: 404 } })
    renderPage({ pathname: "/storyboards/missing", state: { workspaceId: "workspace-1" } })
    expect(await screen.findByText("deck missing")).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "Back to workspace" }))
    expect(screen.getByText("Workspace destination")).toBeInTheDocument()
  })

  it("shows a retryable load error and supports its dashboard escape", async () => {
    api.getStoryboard.mockRejectedValue(new Error("offline"))
    renderPage()
    expect(await screen.findByText("Storyboard service unavailable")).toBeInTheDocument()
    expect(api.getApiErrorMessage).toHaveBeenCalled()
    fireEvent.click(screen.getAllByRole("button", { name: "Back to dashboard" })[0])
    expect(screen.getByText("Dashboard destination")).toBeInTheDocument()
  })

  it("exercises present, download, notes, and share ownership controls", async () => {
    api.getStoryboard.mockResolvedValue(storyboard())
    renderPage()
    expect(await screen.findByRole("button", { name: "Present deck" })).toBeInTheDocument()
    // The title is set from a passive effect, which is not guaranteed to have
    // flushed at the moment `findByRole` resolves — the ready DOM can commit
    // first, leaving the loading-state title ("Storyboard — Thought2Build")
    // still installed. Observed as an intermittent CI failure; locally the
    // effect happened to win the race every time.
    await waitFor(() =>
      expect(document.title).toBe("Launch Storyboard — Thought2Build Storyboard"),
    )

    fireEvent.click(screen.getByRole("button", { name: "Toggle downloads" }))
    expect(screen.getByText("Download menu")).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "Close downloads" }))
    expect(screen.queryByText("Download menu")).toBeNull()

    fireEvent.click(screen.getByRole("button", { name: "Toggle notes" }))
    expect(screen.getByText("Presenter notes")).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "Close notes" }))

    fireEvent.click(screen.getByRole("button", { name: "Share deck" }))
    expect(screen.getByText(/Share modal false no-slug/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "Publish share" }))
    fireEvent.click(screen.getByRole("button", { name: "Close share" }))
    fireEvent.click(screen.getByRole("button", { name: "Share deck" }))
    expect(screen.getByText(/Share modal true published-slug/)).toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: "Present deck" }))
    expect(screen.getByRole("button", { name: "Exit deck" })).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "Exit deck" }))
    expect(screen.getByRole("button", { name: "Present deck" })).toBeInTheDocument()
  })

  it("does not enter presenter view while generation is incomplete, then polls to ready", async () => {
    vi.useFakeTimers()
    api.getStoryboard
      .mockResolvedValueOnce(storyboard({ status: "generating" }))
      .mockRejectedValueOnce(new Error("transient poll failure"))
      .mockResolvedValueOnce(storyboard({ status: "ready" }))
    await act(async () => {
      renderPage()
      await Promise.resolve()
      await Promise.resolve()
    })
    fireEvent.click(screen.getByRole("button", { name: "Present deck" }))
    expect(screen.queryByRole("button", { name: "Exit deck" })).toBeNull()

    await act(async () => { await vi.advanceTimersByTimeAsync(2500) })
    expect(api.getStoryboard).toHaveBeenCalledTimes(2)
    await act(async () => { await vi.advanceTimersByTimeAsync(2500) })
    expect(api.getStoryboard).toHaveBeenCalledTimes(3)
    fireEvent.click(screen.getByRole("button", { name: "Present deck" }))
    expect(screen.getByRole("button", { name: "Exit deck" })).toBeInTheDocument()
  })

  it("ignores a late load completion after unmount", async () => {
    let resolve!: (value: StoryboardDetail) => void
    api.getStoryboard.mockReturnValue(new Promise((done) => { resolve = done }))
    const view = renderPage()
    view.unmount()
    await act(async () => resolve(storyboard()))
    expect(document.title).toBe("Thought2Build")
  })
})
