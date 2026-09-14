export interface BillingPackage {
  credits: number
  price_cents: number
  validity_days: number
  currency: string
  // Issue #44: false when PAYMENTS_ENABLED is off or the active provider is
  // unconfigured — the frontend gates the Buy button on it instead of clicking
  // into a 503. `provider` names the active gateway these economics belong to.
  enabled: boolean
  provider: string
}

export interface BillingCreditPack {
  id: string
  credits_purchased: number
  credits_remaining: number
  price_cents: number
  currency: string
  status: "active" | "consumed" | "expired" | "refunded" | "disputed"
  purchased_at: string
  expires_at: string
}

export interface BillingStatusResponse {
  status: "pending" | "completed"
  credits_added: number
  expires_at: string | null
  credits_purchased?: number
  debt_recovered?: number
  credits_revoked?: number
  settlement_status?: "active" | "consumed" | "expired" | "refunded" | "partially_refunded" | "disputed"
}

export interface CheckoutResponse {
  checkout_url: string
  checkout_ref: string
}
