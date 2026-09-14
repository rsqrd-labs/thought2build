import { render, screen } from "@testing-library/react"
import { describe, expect, it } from "vitest"
import { MemoryRouter, Route, Routes } from "react-router"

import LegalPrivacy from "./LegalPrivacy"

function renderPage() {
  return render(
    <MemoryRouter initialEntries={["/legal/privacy"]}>
      <Routes>
        <Route path="/legal/privacy" element={<LegalPrivacy />} />
      </Routes>
    </MemoryRouter>,
  )
}

describe("LegalPrivacy", () => {
  it("renders every section of the Privacy Policy", () => {
    renderPage()

    expect(
      screen.getByRole("heading", { name: "Privacy Policy" }),
    ).toBeInTheDocument()

    // Every section renders — a missing section is a policy change, not a
    // styling tweak.
    for (const section of [
      /information we collect/i,
      /how we use your information/i,
      /legal basis for processing/i,
      /who we share your information with/i,
      /public sharing/i,
      /cookies/i,
      /data retention/i,
      /your rights/i,
      /children.s privacy/i,
      /international data transfers/i,
      /security/i,
      /changes to this policy/i,
      /\bcontact\b/i,
    ]) {
      expect(screen.getByRole("heading", { name: section })).toBeInTheDocument()
    }
  })

  it("discloses the refresh_token cookie and no other cookies", () => {
    renderPage()
    expect(screen.getByText(/refresh_token/)).toBeInTheDocument()
    expect(
      screen.getByText(/don.t use advertising, analytics, or third-party tracking cookies/i),
    ).toBeInTheDocument()
  })

  it("discloses every third-party data recipient", () => {
    const { container } = renderPage()
    const text = container.textContent ?? ""
    for (const provider of [
      "Anthropic",
      "OpenAI",
      "Razorpay",
      "GitHub",
      "Brave Search",
      "Sentry",
      "Langfuse",
    ]) {
      expect(text).toContain(provider)
    }
  })

  it("states account/data deletion is a manual, support-mediated process", () => {
    renderPage()
    expect(
      screen.getByText(/rather than through automated self-service tooling/i),
    ).toBeInTheDocument()
  })

  it("links back to the app root", () => {
    renderPage()
    expect(screen.getByRole("link", { name: /back to thought2build/i })).toHaveAttribute(
      "href",
      "/",
    )
  })
})
