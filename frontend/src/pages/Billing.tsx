import { useCallback, useEffect, useMemo, useState } from "react"
import { Link, useSearchParams } from "react-router"

import { ActionAlertPanel } from "../components/shared/ActionAlert"
import { BrandLogo } from "../components/shared/BrandLogo"
import {
  createCheckoutSession,
  fetchBillingHistory,
  fetchBillingPackage,
  fetchBillingStatus,
  getApiErrorMessage,
  getCredits,
} from "../services/api"
import type { BillingCreditPack, BillingPackage, BillingStatusResponse } from "../types/billing"
import { billingAlert } from "../utils/errorPresentation"

type LoadState = "loading" | "ready" | "error"
type PollingStatus = "idle" | "processing" | "completed" | "timeout" | "error"

const PAYMENT_POLL_INTERVAL_MS = 2_000
const PAYMENT_POLL_TIMEOUT_MS = 30_000
const EXPIRY_WARNING_DAYS = 7

const statusLabels: Record<BillingCreditPack["status"], string> = {
  active: "Active",
  consumed: "Used",
  expired: "Expired",
  refunded: "Refunded",
  disputed: "Disputed",
}

function formatDate(value: string | null | undefined, withYear = true): string {
  if (!value) return "—"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "—"
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    ...(withYear ? { year: "numeric" as const } : {}),
  }).format(date)
}

export function formatBillingPrice(priceMinorUnits: number, currency: string): string {
  const normalizedCurrency = (currency || "USD").trim().toUpperCase()
  try {
    const formatter = new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: normalizedCurrency,
    })
    const fractionDigits = formatter.resolvedOptions().maximumFractionDigits ?? 2
    return formatter.format(priceMinorUnits / 10 ** fractionDigits)
  } catch {
    const amount = priceMinorUnits / 100
    return `${normalizedCurrency} ${amount.toFixed(priceMinorUnits % 100 === 0 ? 0 : 2)}`
  }
}

export function safeCheckoutUrl(value: string): string | null {
  try {
    const url = new URL(value)
    return url.protocol === "https:" && url.username === "" && url.password === ""
      ? url.toString()
      : null
  } catch {
    return null
  }
}

interface ActivePackSummary {
  credits: number
  count: number
  nextExpiry: string
  expiresSoon: boolean
}

function activePackSummary(packs: BillingCreditPack[]): ActivePackSummary | null {
  const activePacks = packs.filter(
    (pack) => pack.status === "active" && pack.credits_remaining > 0,
  )
  if (activePacks.length === 0) return null

  const credits = activePacks.reduce(
    (total, pack) => total + pack.credits_remaining,
    0,
  )
  const nextExpiryTimestamp = Math.min(
    ...activePacks
      .map((pack) => new Date(pack.expires_at).getTime())
      .filter((value) => Number.isFinite(value)),
  )
  const hasValidExpiry = Number.isFinite(nextExpiryTimestamp)
  const daysUntilExpiry = hasValidExpiry
    ? Math.ceil((nextExpiryTimestamp - Date.now()) / 86_400_000)
    : Number.POSITIVE_INFINITY

  return {
    credits,
    count: activePacks.length,
    nextExpiry: hasValidExpiry
      ? formatDate(new Date(nextExpiryTimestamp).toISOString(), false)
      : "—",
    expiresSoon: daysUntilExpiry >= 0 && daysUntilExpiry <= EXPIRY_WARNING_DAYS,
  }
}

function BillingNav() {
  return (
    <nav className="billing-nav" aria-label="Billing navigation">
      <div className="billing-nav-inner">
        <Link to="/dashboard" className="billing-nav-back" aria-label="Back to dashboard">
          <span aria-hidden="true">←</span>
          <span className="billing-nav-back-label">Dashboard</span>
        </Link>
        <div className="billing-nav-brand">
          <BrandLogo size="small" decorative />
          <span className="settings-nav-divider">/</span>
          <span className="settings-nav-section">Billing</span>
        </div>
        <span className="billing-nav-end" aria-hidden="true" />
      </div>
    </nav>
  )
}

interface PaymentStatusPanelProps {
  status: PollingStatus
  creditsAdded: number | null
  expiresAt: string | null
  settlement: BillingStatusResponse | null
  onRetry: () => void
  onDismiss: () => void
}

function PaymentStatusPanel({
  status,
  creditsAdded,
  expiresAt,
  settlement,
  onRetry,
  onDismiss,
}: PaymentStatusPanelProps) {
  if (status === "idle") return null

  if (status === "timeout" || status === "error") {
    return (
      <section className={`billing-payment-status ${status}`} aria-live="polite">
        <ActionAlertPanel
          {...billingAlert(status === "timeout" ? "payment-timeout" : "payment-error", {
            primaryAction: {
              label: "Check payment status",
              onSelect: onRetry,
              autoDismiss: false,
            },
          })}
        />
      </section>
    )
  }

  return (
    <section
      className={`billing-payment-status ${status}`}
      aria-live="polite"
      aria-atomic="true"
    >
      <div className="billing-payment-mark" aria-hidden="true">
        {status === "completed" ? (
          <svg viewBox="0 0 24 24" fill="none">
            <path d="m6.5 12.5 3.4 3.4 7.6-8" />
          </svg>
        ) : (
          <BrandLogo size="small" decorative />
        )}
      </div>
      <div className="billing-payment-copy">
        {status === "processing" ? (
          <>
            <h2>Confirming your payment</h2>
            <p>Credits will appear automatically as soon as the payment is verified.</p>
          </>
        ) : (
          <>
            <h2>{settlement?.settlement_status === "refunded"
              ? "Payment refunded"
              : settlement?.settlement_status === "disputed"
                ? "Payment reversed after a dispute"
                : settlement?.settlement_status === "expired"
                  ? "This credit pack has expired"
                  : "Payment confirmed"}</h2>
            <p>{creditsAdded ?? 0} credits available from this purchase.
              {(creditsAdded ?? 0) > 0 && ` These credits expire ${formatDate(expiresAt)}.`}
            </p>
            {(settlement?.debt_recovered ?? 0) > 0 && (
              <p>{settlement?.debt_recovered} credits were used to settle billing debt.</p>
            )}
            {settlement?.settlement_status === "partially_refunded" && (
              <p>This purchase has been partially reversed. Your available credits reflect that reversal.</p>
            )}
          </>
        )}
      </div>
      {status === "completed" && (
        <div className="billing-payment-actions">
          <Link to="/dashboard" className="billing-status-link">
            Continue to dashboard
          </Link>
          <button type="button" className="billing-secondary-btn" onClick={onDismiss}>
            Stay on billing
          </button>
        </div>
      )}
    </section>
  )
}

function BillingSkeleton() {
  return (
    <div className="billing-skeleton" aria-busy="true" aria-label="Loading billing details">
      <div className="billing-skeleton-card billing-skeleton-balance" aria-hidden="true">
        <span className="billing-skeleton-line short" />
        <span className="billing-skeleton-line value" />
        <span className="billing-skeleton-line medium" />
      </div>
      <div className="billing-skeleton-card billing-skeleton-offer" aria-hidden="true">
        <span className="billing-skeleton-line short" />
        <span className="billing-skeleton-line title" />
        <span className="billing-skeleton-line medium" />
        <span className="billing-skeleton-button" />
      </div>
      <div className="billing-skeleton-card billing-skeleton-history" aria-hidden="true">
        <span className="billing-skeleton-line title" />
        <span className="billing-skeleton-line wide" />
        <span className="billing-skeleton-line wide" />
      </div>
    </div>
  )
}

export default function Billing() {
  const [searchParams, setSearchParams] = useSearchParams()
  const checkoutRef = searchParams.get("checkout_ref")

  const [loadState, setLoadState] = useState<LoadState>("loading")
  const [billingPackage, setBillingPackage] = useState<BillingPackage | null>(null)
  const [packs, setPacks] = useState<BillingCreditPack[]>([])
  const [balance, setBalance] = useState<number | null>(null)
  const [generationCost, setGenerationCost] = useState<number | null>(null)
  const [debtCredits, setDebtCredits] = useState(0)
  const [checkoutError, setCheckoutError] = useState<string | null>(null)
  const [isStartingCheckout, setIsStartingCheckout] = useState(false)
  const [pollAttempt, setPollAttempt] = useState(0)
  const [pollingStatus, setPollingStatus] = useState<PollingStatus>(
    checkoutRef ? "processing" : "idle",
  )
  const [creditsAdded, setCreditsAdded] = useState<number | null>(null)
  const [completedExpiresAt, setCompletedExpiresAt] = useState<string | null>(null)
  const [settlement, setSettlement] = useState<BillingStatusResponse | null>(null)
  const [balanceJustLanded, setBalanceJustLanded] = useState(false)

  const loadBillingData = useCallback(async (showLoading = true) => {
    if (showLoading) setLoadState("loading")
    try {
      const [packageResponse, historyResponse, creditsResponse] = await Promise.all([
        fetchBillingPackage(),
        fetchBillingHistory(),
        getCredits(),
      ])
      setBillingPackage(packageResponse)
      setPacks(historyResponse)
      setBalance(creditsResponse.balance)
      setGenerationCost(creditsResponse.generation_cost)
      setDebtCredits(creditsResponse.billing_debt_credits)
      setLoadState("ready")
    } catch {
      setLoadState("error")
    }
  }, [])

  useEffect(() => {
    void loadBillingData()
  }, [loadBillingData])

  useEffect(() => {
    if (!checkoutRef) {
      setPollingStatus("idle")
      setCreditsAdded(null)
      setCompletedExpiresAt(null)
      setSettlement(null)
      return
    }

    const ref = checkoutRef
    let elapsed = 0
    let inFlight = false
    let stopped = false
    let intervalId = 0
    let timeoutId = 0
    setPollingStatus("processing")
    setCreditsAdded(null)
    setCompletedExpiresAt(null)
    setSettlement(null)

    function stopPolling() {
      window.clearInterval(intervalId)
      window.clearTimeout(timeoutId)
    }

    async function pollStatus() {
      if (inFlight || stopped) return
      inFlight = true
      try {
        const result = await fetchBillingStatus(ref)
        if (stopped) return

        if (result?.status === "completed") {
          stopped = true
          stopPolling()
          setPollingStatus("completed")
          setCreditsAdded(result.credits_added)
          setCompletedExpiresAt(result.expires_at)
          setSettlement(result)
          setBalanceJustLanded(true)
          void loadBillingData(false)
        } else if (elapsed >= PAYMENT_POLL_TIMEOUT_MS) {
          stopped = true
          stopPolling()
          setPollingStatus("timeout")
        }
      } catch {
        if (!stopped) {
          stopped = true
          stopPolling()
          setPollingStatus("error")
        }
      } finally {
        inFlight = false
      }
    }

    intervalId = window.setInterval(() => {
      elapsed += PAYMENT_POLL_INTERVAL_MS
      void pollStatus()
    }, PAYMENT_POLL_INTERVAL_MS)
    timeoutId = window.setTimeout(() => {
      if (stopped) return
      stopped = true
      stopPolling()
      setPollingStatus("timeout")
    }, PAYMENT_POLL_TIMEOUT_MS)
    void pollStatus()

    return () => {
      stopped = true
      stopPolling()
    }
  }, [checkoutRef, loadBillingData, pollAttempt])

  const packSummary = useMemo(() => activePackSummary(packs), [packs])
  const standardActions = useMemo(() => {
    if (!billingPackage || !generationCost || generationCost <= 0) return null
    return Math.floor(billingPackage.credits / generationCost)
  }, [billingPackage, generationCost])

  const dismissPaymentStatus = useCallback(() => {
    const nextParams = new URLSearchParams(searchParams)
    nextParams.delete("checkout_ref")
    setSearchParams(nextParams, { replace: true })
    setPollingStatus("idle")
  }, [searchParams, setSearchParams])

  const retryPaymentStatus = useCallback(() => {
    setPollingStatus("processing")
    setPollAttempt((attempt) => attempt + 1)
  }, [])

  async function handleBuyCredits() {
    if (isStartingCheckout || !billingPackage) return
    setCheckoutError(null)
    setIsStartingCheckout(true)
    try {
      const response = await createCheckoutSession()
      const checkoutUrl = safeCheckoutUrl(response.checkout_url)
      if (!checkoutUrl) throw new Error("Unsafe checkout URL")
      window.location.assign(checkoutUrl)
    } catch (error) {
      setCheckoutError(
        getApiErrorMessage(error, "Could not open secure checkout. Please try again."),
      )
      setIsStartingCheckout(false)
    }
  }

  const packagePrice = billingPackage
    ? formatBillingPrice(billingPackage.price_cents, billingPackage.currency)
    : null

  return (
    <div className="billing-page">
      <div className="ambient-field" aria-hidden="true">
        <div className="ambient-band band-saffron" />
        <div className="ambient-band band-lotus" />
        <div className="ambient-band band-slate" />
      </div>

      <BillingNav />

      <main className="billing-main">
        <header className="billing-hero">
          <div>
            <p className="billing-eyebrow">Account</p>
            <h1>Billing &amp; credits</h1>
            <p className="billing-hero-copy">
              Review your balance, add a one-time credit pack, and see when every
              purchase expires.
            </p>
          </div>
          <div className="billing-trust-list" aria-label="Purchase terms">
            <span>One-time packs</span>
            <span>Secure hosted checkout</span>
            <span>No subscription</span>
          </div>
        </header>

        <PaymentStatusPanel
          status={pollingStatus}
          creditsAdded={creditsAdded}
          expiresAt={completedExpiresAt}
          settlement={settlement}
          onRetry={retryPaymentStatus}
          onDismiss={dismissPaymentStatus}
        />

        {loadState === "loading" && !billingPackage ? (
          <BillingSkeleton />
        ) : loadState === "error" && !billingPackage ? (
          <section className="billing-state-panel error">
            <ActionAlertPanel
              {...billingAlert("load", {
                primaryAction: {
                  label: "Try again",
                  onSelect: () => void loadBillingData(),
                  autoDismiss: false,
                },
              })}
            />
          </section>
        ) : billingPackage ? (
          <>
            <div className="billing-overview-grid">
              <section className="billing-balance-panel" aria-labelledby="billing-balance-title">
                <div className="billing-card-heading">
                  <p className="billing-eyebrow">Wallet</p>
                  <h2 id="billing-balance-title">Available balance</h2>
                </div>
                <div className="billing-balance-display" aria-live="polite">
                  <output
                    className={`billing-balance-value${balanceJustLanded ? " landed" : ""}`}
                    onAnimationEnd={() => setBalanceJustLanded(false)}
                  >
                    {balance ?? "—"}
                  </output>
                  <span>credits</span>
                </div>
                {packSummary ? (
                  <div className="billing-balance-meta">
                    <p>
                      <strong>{packSummary.credits}</strong> purchased credits across{" "}
                      {packSummary.count} active {packSummary.count === 1 ? "pack" : "packs"}
                    </p>
                    <p className={packSummary.expiresSoon ? "urgent" : ""}>
                      Next expiry <strong>{packSummary.nextExpiry}</strong>
                    </p>
                  </div>
                ) : (
                  <p className="billing-balance-empty">
                    Purchased credits will appear here after checkout.
                  </p>
                )}
                {generationCost !== null && generationCost > 0 && (
                  <p className="billing-generation-cost">
                    A standard stage generation uses {generationCost} credits.
                  </p>
                )}
                {debtCredits > 0 && (
                  <p className="billing-debt-note" role="note">
                    {debtCredits} {debtCredits === 1 ? "credit" : "credits"} from a
                    reversed payment will be recovered from your next top-up.
                  </p>
                )}
              </section>

              <article className="billing-package-card" aria-labelledby="billing-package-title">
                <div className="billing-package-topline">
                  <span className="billing-eyebrow">One-time credit pack</span>
                  <span className="billing-package-badge">No subscription</span>
                </div>
                <div className="billing-package-hero">
                  <div>
                    <h2 id="billing-package-title">{billingPackage.credits} credits</h2>
                    <p className="billing-package-price">{packagePrice}</p>
                  </div>
                  <dl className="billing-package-facts">
                    {standardActions !== null && standardActions > 0 && (
                      <div>
                        <dt>{standardActions}</dt>
                        <dd>standard stage generations</dd>
                      </div>
                    )}
                    <div>
                      <dt>{billingPackage.validity_days} days</dt>
                      <dd>to use this pack</dd>
                    </div>
                  </dl>
                </div>
                {billingPackage.enabled ? (
                  <div className="billing-purchase-action">
                    <button
                      type="button"
                      className="billing-buy-btn"
                      onClick={() => void handleBuyCredits()}
                      disabled={isStartingCheckout}
                    >
                      {isStartingCheckout
                        ? "Opening secure checkout…"
                        : `Buy ${billingPackage.credits} credits · ${packagePrice}`}
                    </button>
                    <p>You’ll review the final total before payment.</p>
                  </div>
                ) : (
                  <p className="billing-unavailable-note" role="note">
                    Credit purchases are temporarily unavailable. Credits you already
                    own remain valid.
                  </p>
                )}
                {billingPackage.enabled && checkoutError && (
                  <ActionAlertPanel
                    severity="error"
                    title="Checkout could not open"
                    message={checkoutError}
                    recovery="Your credits were not charged. Try again from Billing."
                    source="Billing"
                    primaryAction={{
                      label: "Try again",
                      onSelect: () => void handleBuyCredits(),
                      autoDismiss: false,
                    }}
                    onDismiss={() => setCheckoutError(null)}
                    className="billing-checkout-error"
                  />
                )}
              </article>
            </div>

            <section className="billing-history-section" aria-labelledby="billing-history-title">
              <div className="billing-section-header">
                <div>
                  <p className="billing-eyebrow">Purchase history</p>
                  <h2 id="billing-history-title">Credit packs</h2>
                  <p>Purchased amounts, remaining balances, and expiry dates.</p>
                </div>
              </div>

              {packs.length === 0 ? (
                <div className="billing-empty-history">
                  <p>No purchases yet</p>
                  <p>
                    New packs remain available for {billingPackage.validity_days} days
                    from purchase.
                  </p>
                </div>
              ) : (
                <div className="billing-history-table-wrap">
                  <table className="billing-history-table">
                    <thead>
                      <tr>
                        <th scope="col">Purchased</th>
                        <th scope="col">Amount</th>
                        <th scope="col">Credit balance</th>
                        <th scope="col">Status</th>
                        <th scope="col">Expires</th>
                      </tr>
                    </thead>
                    <tbody>
                      {packs.map((pack) => (
                        <tr key={pack.id}>
                          <td data-label="Purchased">{formatDate(pack.purchased_at)}</td>
                          <td data-label="Amount">
                            {formatBillingPrice(pack.price_cents, pack.currency)}
                          </td>
                          <td data-label="Credit balance">
                            <strong>{pack.credits_remaining}</strong> of{" "}
                            {pack.credits_purchased} remaining
                          </td>
                          <td data-label="Status">
                            <span className={`billing-status-chip ${pack.status}`}>
                              {statusLabels[pack.status]}
                            </span>
                          </td>
                          <td data-label="Expires">{formatDate(pack.expires_at)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </section>
          </>
        ) : null}
      </main>
    </div>
  )
}
