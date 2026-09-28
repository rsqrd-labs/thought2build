/**
 * Build-time feature flags (issue #21).
 *
 * Flags are read once from Vite's statically-replaced `import.meta.env`, so the
 * bundler can tree-shake disabled branches and there is zero per-render cost.
 * Values are strings in the environment; only the exact literal `"true"` opts
 * in, so any unset/typo'd value fails closed (off).
 *
 * `branded_loaders` enables the unified `<BrandLoader>` system (the animated
 * squirrel) across every loading surface — including all generation overlays.
 * It now ships **on** by default after verification; set
 * `VITE_BRANDED_LOADERS=false` to opt back out to the legacy per-surface
 * loaders. Only the exact literal `"false"` disables it, so any unset/typo'd
 * value keeps the branded loader on.
 *
 * `demoDayMode` gates the **creation** of Demo Day workspaces (the mode +
 * target-agent selectors in `CreateWorkspaceModal`) per the Demo Day plan
 * (docs/DEMO_DAY_MODE_IMPLEMENTATION_PLAN.md §10 Phase 4). It ships **off**:
 * unlike `brandedLoaders`, only the exact literal `"true"` opts in, so any
 * unset/typo'd value fails closed (off) until the live golden-corpus gate
 * promotes the feature. NOTE: this flag governs the *creation* surface only —
 * the construction-verified badge and handoff panel render data-driven on
 * `workspace.mode === "demo_day"`, so a workspace already created in Demo Day
 * mode keeps displaying its handoff UI even if this build flag is off.
 *
 * `webAnalytics` injects Vercel Web Analytics (see services/analytics.ts). Only
 * the exact literal `"true"` opts in, so dev, CI and e2e builds ship no script.
 */
export const featureFlags = {
  brandedLoaders: import.meta.env.VITE_BRANDED_LOADERS !== "false",
  demoDayMode: import.meta.env.VITE_DEMO_DAY_MODE === "true",
  webAnalytics: import.meta.env.VITE_ANALYTICS_ENABLED === "true",
} as const

export type FeatureFlag = keyof typeof featureFlags
