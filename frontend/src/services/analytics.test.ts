import { describe, expect, it } from "vitest"

import { redactAnalyticsEvent, redactAnalyticsUrl } from "./analytics"

const ORIGIN = "https://thought2build.com"

describe("redactAnalyticsUrl", () => {
  it("drops the OAuth code and state from the auth callback", () => {
    expect(
      redactAnalyticsUrl(`${ORIGIN}/auth/callback?state=abc&code=4%2F0AXsecret&scope=email`),
    ).toBe(`${ORIGIN}/auth/callback`)
  })

  it("collapses capability slugs on public share routes", () => {
    expect(redactAnalyticsUrl(`${ORIGIN}/p/Zx9kQ2mLp4Rt7Wv1`)).toBe(`${ORIGIN}/p/[slug]`)
    expect(redactAnalyticsUrl(`${ORIGIN}/sb/Zx9kQ2mLp4Rt7Wv1`)).toBe(`${ORIGIN}/sb/[slug]`)
  })

  it("collapses workspace and storyboard ids but keeps the sub-route", () => {
    expect(redactAnalyticsUrl(`${ORIGIN}/workspace/7f3c1e2a-0000-4000-8000-000000000000`)).toBe(
      `${ORIGIN}/workspace/[id]`,
    )
    expect(redactAnalyticsUrl(`${ORIGIN}/workspace/7f3c1e2a/github`)).toBe(
      `${ORIGIN}/workspace/[id]/github`,
    )
    expect(redactAnalyticsUrl(`${ORIGIN}/storyboards/42`)).toBe(`${ORIGIN}/storyboards/[id]`)
  })

  it("keeps campaign attribution params and nothing else", () => {
    expect(
      redactAnalyticsUrl(
        `${ORIGIN}/dashboard?utm_source=hn&checkout_ref=chk_123&utm_campaign=launch&ref=x#frag`,
      ),
    ).toBe(`${ORIGIN}/dashboard?utm_source=hn&utm_campaign=launch&ref=x`)
  })

  it("leaves plain app routes untouched", () => {
    expect(redactAnalyticsUrl(`${ORIGIN}/dashboard`)).toBe(`${ORIGIN}/dashboard`)
    expect(redactAnalyticsUrl(`${ORIGIN}/`)).toBe(`${ORIGIN}/`)
  })

  it("returns null for an unparseable URL", () => {
    expect(redactAnalyticsUrl("not a url")).toBeNull()
  })
})

describe("redactAnalyticsEvent", () => {
  it("redacts the event url and preserves its type", () => {
    expect(
      redactAnalyticsEvent({ type: "pageview", url: `${ORIGIN}/p/secret-slug?x=1` }),
    ).toEqual({ type: "pageview", url: `${ORIGIN}/p/[slug]` })
  })

  it("drops events whose url cannot be parsed", () => {
    expect(redactAnalyticsEvent({ type: "pageview", url: "::" })).toBeNull()
  })
})
