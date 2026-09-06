import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter } from "react-router"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import Billing, { formatBillingPrice, safeCheckoutUrl } from "../pages/Billing"
import {
  createCheckoutSession,
  fetchBillingHistory,
  fetchBillingPackage,
  fetchBillingStatus,
  getCredits,
} from "../services/api"
import type { BillingCreditPack, BillingPackage } from "../types/billing"

vi.mock("../services/api", () => ({
  createCheckoutSession: vi.fn(),
  fetchBillingHistory: vi.fn(),
  fetchBillingPackage: vi.fn(),
  fetchBillingStatus: vi.fn(),
  getApiErrorMessage: (_error: unknown, fallback: string) => fallback,
  getCredits: vi.fn(),
}))

const mockCreateCheckout = vi.mocked(createCheckoutSession)
const mockHistory = vi.mocked(fetchBillingHistory)
const mockPackage = vi.mocked(fetchBillingPackage)
const mockStatus = vi.mocked(fetchBillingStatus)
const mockCredits = vi.mocked(getCredits)

const PACKAGE: BillingPackage = {
  credits: 200,
  price_cents: 900,
  validity_days: 30,
  currency: "USD",
  enabled: true,
  provider: "lemonsqueezy",
}

function pack(overrides: Partial<BillingCreditPack>): BillingCreditPack {
  return {
    id: "pack-1",
    credits_purchased: 200,
    credits_remaining: 200,
    price_cents: 900,
    currency: "USD",
    status: "active",
    purchased_at: "2026-01-01T00:00:00Z",
    expires_at: "2026-02-01T00:00:00Z",
    ...overrides,
  }
}

function balance(over: Partial<Awaited<ReturnType<typeof getCredits>>> = {}) {
  return { balance: 200, generation_cost: 10, billing_debt_credits: 0, ...over }
}

function renderBilling(path = "/billing") {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Billing />
    </MemoryRouter>,
  )
}

beforeEach(() => {
  mockPackage.mockResolvedValue(PACKAGE)
  mockHistory.mockResolvedValue([])
  mockCredits.mockResolvedValue(balance())
  mockStatus.mockResolvedValue(null)
  mockCreateCheckout.mockResolvedValue({
    checkout_url: "https://pay.lemonsqueezy.com/x",
    checkout_ref: "ref-1",
  })
})

afterEach(() => {
  vi.clearAllMocks()
})

describe("Billing — history rendering", () => {
  it("renders exact purchase economics and distinct lifecycle labels", async () => {
    mockHistory.mockResolvedValue([
      pack({ id: "p-active", status: "active" }),
      pack({
        id: "p-disputed",
        status: "disputed",
        credits_remaining: 0,
        price_cents: 1_050,
      }),
    ])

    const { container } = renderBilling()

    await waitFor(() => expect(screen.getByText("Active")).toBeInTheDocument())
    expect(screen.getByText("Disputed")).toBeInTheDocument()
    const creditCells = container.querySelectorAll(".billing-history-table td:nth-child(3)")
    expect(creditCells[1]?.textContent).toMatch(/0 of 200 remaining/i)
    expect(screen.getByText(/\$10\.50/)).toBeInTheDocument()
    const disputedChip = container.querySelector(".billing-status-chip.disputed")
    expect(disputedChip).not.toBeNull()
    expect(container.querySelector(".billing-nav-brand .brand-logo-image")).not.toBeNull()
    expect(container.textContent).not.toMatch(/Stripe/)
    expect(container.textContent).not.toMatch(/\bSF\b/)
  })

  it("uses a single page heading and presents checkout terms before the CTA", async () => {
    renderBilling()

    await waitFor(() => expect(screen.getByRole("heading", { level: 1 })).toBeVisible())
    expect(screen.getAllByRole("heading", { level: 1 })).toHaveLength(1)
    expect(screen.getByRole("heading", { name: /billing & credits/i })).toBeVisible()
    expect(screen.getAllByText(/no subscription/i).length).toBeGreaterThan(0)
    expect(screen.getByText(/secure hosted checkout/i)).toBeVisible()
  })
})

describe("Billing — checkout_ref polling", () => {
  it("polls by checkout_ref and resolves to the credits-added confirmation", async () => {
    mockStatus.mockResolvedValue({
      status: "completed",
      credits_added: 200,
      expires_at: "2026-02-01T00:00:00Z",
    })

    renderBilling("/billing?checkout_ref=ref-1")

    await waitFor(() =>
      expect(mockStatus).toHaveBeenCalledWith("ref-1"),
    )
    await waitFor(() =>
      expect(screen.getByText(/200 credits available from this purchase/i)).toBeInTheDocument(),
    )
  })

  it("shows the calm settling state while the grant is still pending (404)", async () => {
    mockStatus.mockResolvedValue(null) // 404 → pending, not an error

    renderBilling("/billing?checkout_ref=ref-pending")

    await waitFor(() =>
      expect(screen.getByText(/confirming your payment/i)).toBeInTheDocument(),
    )
    expect(screen.queryByText(/unable to verify/i)).not.toBeInTheDocument()
  })

  it("surfaces a real error distinctly from a pending 404", async () => {
    mockStatus.mockRejectedValue(new Error("boom"))

    renderBilling("/billing?checkout_ref=ref-err")

    await waitFor(() =>
      expect(screen.getByText(/payment status could not be verified/i)).toBeInTheDocument(),
    )
  })

  it("restarts status polling when the recovery action is selected", async () => {
    const user = userEvent.setup()
    mockStatus
      .mockRejectedValueOnce(new Error("temporary network failure"))
      .mockResolvedValue({
        status: "completed",
        credits_added: 200,
        expires_at: "2026-02-01T00:00:00Z",
      })

    renderBilling("/billing?checkout_ref=ref-retry")

    const retry = await screen.findByRole("button", { name: /check payment status/i })
    await user.click(retry)

    await waitFor(() => expect(mockStatus).toHaveBeenCalledTimes(2))
    expect(await screen.findByText(/200 credits available from this purchase/i)).toBeVisible()
  })

  it("lets the user dismiss a completed receipt without losing billing data", async () => {
    const user = userEvent.setup()
    mockStatus.mockResolvedValue({
      status: "completed",
      credits_added: 200,
      expires_at: "2026-02-01T00:00:00Z",
    })

    renderBilling("/billing?checkout_ref=ref-dismiss")

    await user.click(await screen.findByRole("button", { name: /stay on billing/i }))
    expect(screen.queryByText(/200 credits available from this purchase/i)).not.toBeInTheDocument()
    expect(screen.getByRole("heading", { name: /available balance/i })).toBeVisible()
  })
})

describe("Billing — payment-reversal debt", () => {
  it("shows debt as a distinct note and never folds it into the usable balance", async () => {
    mockCredits.mockResolvedValue(balance({ balance: 200, billing_debt_credits: 50 }))

    const { container } = renderBilling()

    await waitFor(() =>
      expect(screen.getByText(/reversed payment/i)).toBeInTheDocument(),
    )
    // The note is role=note (informational), never a red alert.
    const note = container.querySelector(".billing-debt-note")
    expect(note).not.toBeNull()
    expect(note?.getAttribute("role")).toBe("note")
    // The usable balance shows 200 — NOT 250 (200 + 50 debt).
    const balanceValue = container.querySelector(".billing-balance-value")
    expect(balanceValue?.textContent).toBe("200")
  })

  it("renders no debt note when there is no pending reversal debt", async () => {
    mockCredits.mockResolvedValue(balance({ billing_debt_credits: 0 }))

    const { container } = renderBilling()

    await waitFor(() => expect(screen.getByText("200")).toBeInTheDocument())
    expect(container.querySelector(".billing-debt-note")).toBeNull()
  })
})

describe("Billing — checkout availability gate (issue #44)", () => {
  it("hides Buy, keeps the package card, and shows the quiet slate note when disabled", async () => {
    mockPackage.mockResolvedValue({ ...PACKAGE, enabled: false })

    const { container } = renderBilling()

    // The package economics still render — only the button is swapped out.
    await waitFor(() =>
      expect(screen.getByText("200 credits")).toBeInTheDocument(),
    )
    expect(container.querySelector(".billing-package-card")).not.toBeNull()
    expect(screen.getByText("30 days")).toBeInTheDocument()
    expect(screen.getByText(/to use this pack/i)).toBeInTheDocument()

    // No Buy button; a calm role=note note in its place (coherent when arriving
    // from an out-of-credits alert).
    expect(
      screen.queryByRole("button", { name: /buy .*credits/i }),
    ).not.toBeInTheDocument()
    const note = container.querySelector(".billing-unavailable-note")
    expect(note).not.toBeNull()
    expect(note?.getAttribute("role")).toBe("note")
    expect(note?.textContent).toMatch(/temporarily unavailable/i)
  })

  it("shows the Buy button when checkout is enabled", async () => {
    mockPackage.mockResolvedValue({ ...PACKAGE, enabled: true })

    const { container } = renderBilling()

    await waitFor(() =>
      expect(screen.getByRole("button", { name: /buy .*credits/i })).toBeInTheDocument(),
    )
    expect(container.querySelector(".billing-unavailable-note")).toBeNull()
  })

  it("still polls a returning ?checkout_ref to completion while disabled (kill switch mid-flight)", async () => {
    // PAYMENTS_ENABLED flipped false after a user started checkout — the webhook
    // still grants (D3) and PaymentStatusPanel must poll regardless of `enabled`.
    mockPackage.mockResolvedValue({ ...PACKAGE, enabled: false })
    mockStatus.mockResolvedValue({
      status: "completed",
      credits_added: 200,
      expires_at: "2026-02-01T00:00:00Z",
    })

    renderBilling("/billing?checkout_ref=ref-inflight")

    await waitFor(() =>
      expect(mockStatus).toHaveBeenCalledWith("ref-inflight"),
    )
    await waitFor(() =>
      expect(screen.getByText(/200 credits available from this purchase/i)).toBeInTheDocument(),
    )
  })
})

describe("Billing — currency formatting (issue #44)", () => {
  it("preserves fractional minor units instead of rounding the displayed price", () => {
    expect(formatBillingPrice(1_050, "USD")).toMatch(/\$10\.50/)
  })

  it("renders INR as ₹ via the Intl path", async () => {
    mockPackage.mockResolvedValue({
      ...PACKAGE,
      price_cents: 79900,
      currency: "INR",
    })

    const { container } = renderBilling()

    await waitFor(() =>
      expect(container.querySelector(".billing-package-price")).not.toBeNull(),
    )
    const price = container.querySelector(".billing-package-price")?.textContent ?? ""
    // Intl renders INR with the ₹ symbol; never a "$" fallback.
    expect(price).toMatch(/₹\s?799/)
    expect(price).not.toMatch(/\$/)
  })

  it("falls back to the currency code, not '$', when Intl cannot format it", async () => {
    // Force the catch branch: a malformed (non-3-letter) code makes
    // Intl.NumberFormat throw RangeError. The fallback must prefix the code, not "$".
    mockPackage.mockResolvedValue({
      ...PACKAGE,
      price_cents: 79900,
      currency: "INRX",
    })

    const { container } = renderBilling()

    await waitFor(() =>
      expect(container.querySelector(".billing-package-price")).not.toBeNull(),
    )
    const price = container.querySelector(".billing-package-price")?.textContent ?? ""
    expect(price).toMatch(/INRX\s?799/)
    expect(price).not.toMatch(/\$/)
  })
})

describe("Billing — checkout navigation hardening", () => {
  it("accepts only credential-free HTTPS checkout URLs", () => {
    expect(safeCheckoutUrl("https://pay.lemonsqueezy.com/checkout/abc")).toMatch(
      /^https:\/\/pay\.lemonsqueezy\.com/,
    )
    expect(safeCheckoutUrl("http://pay.example.test/checkout")).toBeNull()
    expect(safeCheckoutUrl("javascript:alert(1)")).toBeNull()
    expect(safeCheckoutUrl("https://user:secret@pay.example.test/checkout")).toBeNull()
    expect(safeCheckoutUrl("not a URL")).toBeNull()
  })

  it("does not navigate when the API returns an unsafe checkout URL", async () => {
    const user = userEvent.setup()
    mockCreateCheckout.mockResolvedValue({
      checkout_url: "javascript:alert(1)",
      checkout_ref: "ref-unsafe",
    })

    renderBilling()

    await user.click(await screen.findByRole("button", { name: /buy 200 credits/i }))
    expect(await screen.findByText(/checkout could not open/i)).toBeVisible()
    expect(screen.getByText(/could not open secure checkout/i)).toBeVisible()
  })
})

describe("Billing — settled payment receipt", () => {
  it.each([
    ["refunded", "Payment refunded"],
    ["disputed", "Payment reversed after a dispute"],
    ["expired", "This credit pack has expired"],
  ] as const)("shows %s with zero usable credits", async (settlement_status, heading) => {
    mockStatus.mockResolvedValue({
      status: "completed", credits_added: 0, credits_purchased: 200,
      settlement_status, expires_at: "2026-02-01T00:00:00Z",
    })
    renderBilling("/billing?checkout_ref=settled")
    expect(await screen.findByRole("heading", { name: heading })).toBeVisible()
    expect(screen.getByText(/0 credits available from this purchase/i)).toBeVisible()
    expect(screen.queryByText(/200 credits added/i)).not.toBeInTheDocument()
  })

  it("explains debt recovery separately from usable purchase credits", async () => {
    mockStatus.mockResolvedValue({
      status: "completed", credits_added: 150, credits_purchased: 200,
      debt_recovered: 50, settlement_status: "active",
      expires_at: "2026-02-01T00:00:00Z",
    })
    renderBilling("/billing?checkout_ref=debt-repaid")
    expect(await screen.findByText(/150 credits available from this purchase/i)).toBeVisible()
    expect(screen.getByText(/50 credits were used to settle billing debt/i)).toBeVisible()
  })

  it("explains a partial payment reversal", async () => {
    mockStatus.mockResolvedValue({
      status: "completed", credits_added: 100,
      settlement_status: "partially_refunded", expires_at: "2026-02-01T00:00:00Z",
    })
    renderBilling("/billing?checkout_ref=partially-reversed")
    expect(await screen.findByText(/this purchase has been partially reversed/i)).toBeVisible()
    expect(screen.getByText(/100 credits available from this purchase/i)).toBeVisible()
  })
})
