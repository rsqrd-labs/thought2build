/**
 * Vercel Web Analytics for the SPA.
 *
 * The SPA is served on the apex through the marketing zone's rewrites, so the
 * script and beacons resolve to `/_vercel/insights/*` on thought2build.com —
 * same-origin (CSP `'self'`-clean) and recorded in the marketing project's
 * dashboard alongside the Astro pages. That gives one funnel across both zones:
 * `/` → `/auth/callback` → `/dashboard` → `/workspace/[id]`. The script tracks
 * client-side route changes itself, so there is no per-route wiring.
 *
 * Off unless `VITE_ANALYTICS_ENABLED` is exactly `"true"` (see featureFlags),
 * so dev, CI and e2e builds ship no analytics script.
 */
import { inject, type BeforeSendEvent } from "@vercel/analytics"

import { featureFlags } from "../config/featureFlags"

// Campaign attribution is the only query data worth keeping. Everything else
// is dropped: `/auth/callback` carries the OAuth `code` and `state`, and none
// of the app's other query parameters are useful as analytics dimensions.
const KEPT_QUERY_PARAMS = new Set([
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_term",
  "utm_content",
  "ref",
])

// Path segments that identify a user's data. `/p/:slug` and `/sb/:slug` are
// capability URLs — holding the slug grants read access — so they must never
// leave the browser verbatim.
const REDACTED_ROUTES: ReadonlyArray<readonly [RegExp, string]> = [
  [/^\/p\/[^/]+/, "/p/[slug]"],
  [/^\/sb\/[^/]+/, "/sb/[slug]"],
  [/^\/workspace\/[^/]+/, "/workspace/[id]"],
  [/^\/storyboards\/[^/]+/, "/storyboards/[id]"],
]

/**
 * Strip identifiers and secrets from a page URL before it is sent to Vercel.
 * Returns null (drop the event) when the URL cannot be parsed.
 */
export function redactAnalyticsUrl(raw: string): string | null {
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    return null
  }

  let path = url.pathname
  for (const [pattern, replacement] of REDACTED_ROUTES) {
    if (pattern.test(path)) {
      path = path.replace(pattern, replacement)
      break
    }
  }

  const kept = new URLSearchParams()
  for (const [key, value] of url.searchParams) {
    if (KEPT_QUERY_PARAMS.has(key.toLowerCase())) kept.append(key, value)
  }
  const query = kept.toString()

  return `${url.origin}${path}${query ? `?${query}` : ""}`
}

export function redactAnalyticsEvent(event: BeforeSendEvent): BeforeSendEvent | null {
  const url = redactAnalyticsUrl(event.url)
  return url === null ? null : { ...event, url }
}

export function initAnalytics(): void {
  if (!featureFlags.webAnalytics) return
  inject({
    // Explicit, because `auto` sniffs `process.env.NODE_ENV`, which Vite does
    // not define; development mode would load a third-party debug script that
    // the CSP blocks anyway.
    mode: "production",
    framework: "react",
    beforeSend: redactAnalyticsEvent,
  })
}
