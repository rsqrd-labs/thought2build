# Thought2Build Go-Live Runbook (First-Time Launch, Beginner Edition)

You've never taken an app live before — this doc assumes that and explains
each step, not just the command. It is written for **this exact launch**:
backend on **Railway**, frontend on **Vercel**, domain **thought2build.com**
bought at **Hostinger**, and payments **turned off for the first week**.

This is a companion to two docs that already exist — don't duplicate work,
use all three together:

- **This file** — the order to do things in, on launch day, with the
  Hostinger-specific domain steps that aren't written down anywhere else.
- `docs/INTEGRATION_API_SETUP_HANDBOOK.md` — the detailed click-by-click
  instructions for each service (Railway, Vercel, Google OAuth, etc.). When
  this runbook says "do Step 1 from the handbook," go read that section.
- `docs/PRODUCTION_RELEASE_GATE.md` — the full pre-flight checklist a team
  with a staging environment would run. You don't have staging, so Phase 6
  below is a trimmed-down version of it that fits a solo, single-environment
  launch.

Print this out, or keep it open next to your terminal, and go top to bottom.
Each phase ends with a "✅ You should now have" checkpoint — don't move on
until it's true.

---

## Before you start: accounts checklist

Create these accounts now, before you begin (all have free tiers):

- [ ] [Railway](https://railway.app) — sign up with GitHub
- [ ] [Vercel](https://vercel.com) — sign up with GitHub
- [ ] Hostinger — domain already being purchased
- [ ] [Google Cloud Console](https://console.cloud.google.com) — for "Sign in
      with Google"
- [ ] At least one LLM provider account with billing enabled — e.g.
      [console.anthropic.com](https://console.anthropic.com). Thought2Build needs
      at least one working provider key to generate anything.

You do **not** need, for this launch: Razorpay or Lemon Squeezy (payments are
off), a GitHub OAuth App (export integration, optional), Langfuse, Sentry, or
the marketing zone (`apps/marketing`). All of those can be added later
without downtime. Leave their env vars blank.

---

## Your domain layout

Thought2Build is two separate deployments that need two separate hostnames
under your domain, `thought2build.com` (bought at Hostinger):

| Hostname | Points to | Serves |
|---|---|---|
| `thought2build.com` and `www.thought2build.com` | Vercel | The React app (frontend) |
| `api.thought2build.com` | Railway | The backend API |

You are **not** setting up the separate marketing/SEO site
(`apps/marketing`) for this launch — the frontend app itself lives on the
apex domain (`thought2build.com`). That keeps this launch to two moving parts
instead of three. You can add the marketing zone later; it's a separate
Vercel project and doesn't touch this setup.

---

## What this will cost on Railway (and how this runbook keeps it low)

Railway's **Hobby plan** is $5/month, and that $5 is also your first $5 of
usage — it's not an extra fee on top. Everything above $5 is billed on
**actual usage**, and the biggest driver by far is **resident memory** (~$10
per GB-month), *not* CPU — an idle async app burns almost no CPU between
requests, so CPU is a small slice of the bill.

For this launch (backend + `worker` + `worker-fast` + Postgres + Redis, no real
traffic yet), durable generation runs on the existing bulk worker and adds no
separate service. Apply the three low-risk levers this runbook bakes in:

| Lever | Where it's applied | Saves |
|---|---|---|
| `WEB_CONCURRENCY=1` on `backend` | Phase 1, step 3 | ~$3–4/mo |
| Postgres volume provisioned at 1 GB (not padded) | Phase 1, step 7 | ~$0.60/mo + headroom later |
| Postgres memory config trimmed for a tiny DB | Phase 1, step 7 | ~$1–2/mo |

All three are **reversible with zero reliability cost at pre-launch scale** —
that's why they're the default here. Two bigger levers (merging `worker` and
`worker-fast`; lazy-loading heavy libs in the workers) are deliberately **not**
taken: the first reverts the worker-lane isolation that protects paid credit
grants, and the second
is a code change worth doing only after real memory numbers justify it.

**These are modeled estimates, not a quote.** Railway shows real per-service
memory within a day of running — watch it (Phase 12) and let the actual graph,
not this table, drive any further tuning.

> Note: the LLM provider bill (Anthropic/OpenAI/Google, per Phase 7) is
> **separate** and usage-based per token. At real generation volume it can
> exceed the Railway hosting cost — it is not included in the numbers above.

---

## Phase 0 — Generate production secrets (on your own laptop)

Do this first, before touching Railway. These are one-time values you'll
paste into Railway's environment variables in Phase 1.

**⚠️ Important:** this repo has `jwt_private.pem` / `jwt_public.pem` files at
the repo root and a `backend/.env.example` full of `placeholder-*` values —
those are for **local development only**. Never paste those into Railway.
Generate fresh ones now, one command at a time:

**1. A new JWT signing key pair** (used to sign login tokens):
```bash
openssl genrsa -out /tmp/jwt_private.pem 2048
```
```bash
openssl rsa -in /tmp/jwt_private.pem -pubout -out /tmp/jwt_public.pem
```

**2. Convert both keys to single-line strings** you can paste into Railway:
```bash
python3 - <<'PY'
from pathlib import Path
for name, file in [("JWT_PRIVATE_KEY", "/tmp/jwt_private.pem"), ("JWT_PUBLIC_KEY", "/tmp/jwt_public.pem")]:
    value = Path(file).read_text().replace("\n", "\\n")
    print(f'{name}="{value}"')
PY
```

**3. `ENCRYPTION_MASTER_KEY`** (encrypts stored user API keys):
```bash
cd backend && uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**4. `CSRF_SECRET`:**
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

**5. `METRICS_TOKEN`** (protects the `/metrics` endpoint):
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Paste each output into a scratch text file — you'll need all five in Phase 1.
Delete `/tmp/jwt_private.pem` after you've copied it into Railway; don't leave
a real private key lying around on disk longer than needed.

**✅ You should now have:** five secret values saved somewhere safe (a
password manager, not a plain text file you'll forget about).

---

## Phase 1 — Stand up the backend on Railway

Follow **Step 1 ("Set up Railway (backend)")** in
`docs/INTEGRATION_API_SETUP_HANDBOOK.md` in full — it walks through creating
the project, adding PostgreSQL and Redis, and creating the backend service.
Use the secrets from Phase 0 for `JWT_PRIVATE_KEY`, `JWT_PUBLIC_KEY`,
`ENCRYPTION_MASTER_KEY`, `CSRF_SECRET`, and `METRICS_TOKEN`.

A few things specific to **this** launch, on top of the handbook:

1. **Payments stay off.** Leave every `RAZORPAY_*` and `LEMONSQUEEZY_*`
   variable blank, and explicitly set:
   ```
   PAYMENTS_ENABLED=false
   PAYMENT_PROVIDER=razorpay
   ```
   (`PAYMENT_PROVIDER` still needs a valid value even while disabled — the
   app fails to start in production if it's blank or misspelled — but with
   `PAYMENTS_ENABLED=false` no checkout is reachable and the Buy button stays
   hidden. See Phase 8 below for how to turn it on next week.)

2. **Leave these blank too** (all optional, all safe to skip for launch):
   `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET`, `GITHUB_APP_ID`,
   `GITHUB_APP_SLUG`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_APP_WEBHOOK_SECRET`,
   `LANGFUSE_SECRET_KEY`, `SENTRY_DSN`, `GRAFANA_OTLP_ENDPOINT`.

3. **`ALLOWED_HOSTS`** — you can't fill this in correctly yet (it needs your
   real domain, which isn't wired up until Phase 4). For now set it to just
   the Railway domain so the backend can boot and you can test it:
   ```
   ALLOWED_HOSTS=*.up.railway.app
   ```
   You'll come back and add your real domain in Phase 5.

   **Cost lever — set `WEB_CONCURRENCY=1` on the `backend` service.** Railway
   bills mostly on **resident memory**, and each API worker is a full copy of
   the app in RAM. At pre-launch traffic one async worker is plenty (it serves
   many concurrent requests on one event loop), so this roughly halves the
   `backend` service's memory bill for free. Set on `backend` only (workers
   ignore it):
   ```
   WEB_CONCURRENCY=1
   ```
   Trade-off to know: with one worker there's no in-process redundancy — a
   crash is a brief outage until it respawns, and a deploy restart has no second
   worker to cover the swap. That's fine solo/pre-launch. **Bump it back to `2`
   the day real traffic arrives** (see Phase 12).

4. **`FRONTEND_URL`** — same story, set a placeholder for now
   (`https://placeholder.example.com` is fine), you'll fix it in Phase 5.

5. This app also needs **two worker processes**, not just the web service —
   `worker` (bulk jobs + durable paid LLM generation) and `worker-fast`
   (billing + PR-check jobs). Create both now:
   - Create the backend service as the handbook describes, then duplicate it
     twice (or add two more "GitHub Repo" services from the same repo),
     rooted at `backend` the same way.
   - On each of the three services, set **Settings → Config-as-code → Config
     File Path** explicitly:

     | Service name | Config File Path |
     |---|---|
     | `backend` | `/backend/railway.json` |
     | `worker` | `/backend/railway.worker.json` |
     | `worker-fast` | `/backend/railway.worker-fast.json` |

   - Copy the **same environment variables** onto both worker services
     as you set on `backend` (Railway's "Variable References" / "Shared
     Variables" feature can do this in one place — use it if offered, it
     saves you from keeping three copies in sync).
   - Only the `backend` service needs **Generate Domain** (Phase 1, step 14
     in the handbook) — workers don't serve HTTP traffic.

6. After all three services deploy, confirm all three show a healthy
   deployment in the **Deployments** tab (not just `backend`) — a missing
   `worker-fast` or `worker` is easy to overlook. Without `worker`, paid
   generation jobs queue but never run.

7. **Cost levers on the Postgres database** (both safe, do them now while the
   DB is empty):
   - **Volume size:** when you add the PostgreSQL plugin, provision its volume
     at **1 GB**, not a padded size. Railway bills the *provisioned* volume,
     not just the bytes used, and you can grow it later with **zero downtime** —
     so there's no reason to reserve space you don't need yet.
   - **Memory config:** the DB defaults are tuned for a much larger dataset
     than a fresh launch has. If Railway's Postgres lets you set config (or via
     a one-off `ALTER SYSTEM`), trimming `shared_buffers` / `work_mem` toward a
     small footprint shaves resident memory. This is optional and marginal
     (~$1–2/mo) — skip it if the plugin doesn't expose tuning; **never** trim it
     so far the DB can't serve the smoke test in Phase 10.

**✅ You should now have:** three Railway services (`backend`, `worker`, and
`worker-fast`) all deployed successfully,
`WEB_CONCURRENCY=1` set on `backend`,
the Postgres volume at 1 GB, and
`https://<your-railway-backend-url>/health` returning
`{"status":"ok",...}` in your browser.

---

## Phase 2 — Point `api.thought2build.com` at Railway

1. In Railway, open the `backend` service → **Settings** → **Networking** →
   **Custom Domain** → enter `api.thought2build.com` → **Add Domain**. Railway
   shows you a **CNAME target** (something like
   `xxxxx.up.railway.app` or similar — copy exactly what Railway shows you).
2. Log into **Hostinger** → **Domains** → your domain → **DNS / Name
   Servers** → **DNS Zone Editor** (naming may vary slightly by Hostinger's
   current UI, but "DNS Zone" or "Manage DNS Records" is the section you
   want).
3. Add a new record:
   - **Type:** `CNAME`
   - **Name/Host:** `api`
   - **Points to/Target:** the value Railway gave you in step 1
   - **TTL:** leave default (or 3600/1 hour)
4. Save. DNS changes can take anywhere from a few minutes to a few hours to
   propagate — don't panic if it's not instant.
5. Back in Railway, wait for the domain status to flip from "Pending" to a
   green checkmark (it issues an SSL certificate automatically once DNS
   resolves — you don't do anything for HTTPS, Railway handles it).

**Check propagation** from your terminal while you wait:
```bash
dig api.thought2build.com CNAME +short
```
Once that prints Railway's target, it's live.

**✅ You should now have:** `https://api.thought2build.com/health` returning
`{"status":"ok",...}` with a valid HTTPS padlock.

---

## Phase 3 — Stand up the frontend on Vercel

Follow **Step 2 ("Set up Vercel")** in the handbook, with one change: set
`VITE_API_URL` to your **new custom domain**, not the raw Railway URL:

```
VITE_API_URL=https://api.thought2build.com
```

Deploy. Vercel gives you a temporary `*.vercel.app` URL — that's expected,
you'll attach the real domain in the next phase.

**✅ You should now have:** a working `https://<something>.vercel.app` URL
that loads the Thought2Build landing page (sign-in will not work yet — that's
Phase 6).

---

## Phase 4 — Point `thought2build.com` at Vercel

1. In Vercel, open your frontend project → **Settings** → **Domains** → type
   `thought2build.com` → **Add**.
2. Vercel will show you DNS instructions. For an apex domain it's usually:
   - **Type:** `A`, **Name:** `@`, **Value:** `76.76.21.21`

     (Vercel occasionally changes this IP — always use the exact value shown
     in your Vercel dashboard, not this document, in case it's changed.)
   - Also add `www.thought2build.com` in the same Vercel dialog, and Vercel will
     show a `CNAME` record for it (usually pointing to
     `cname.vercel-dns.com`).
3. In Hostinger's DNS Zone Editor, add both records Vercel showed you:
   - `A` record: Name `@`, Value the IP from Vercel
   - `CNAME` record: Name `www`, Value `cname.vercel-dns.com` (or whatever
     Vercel showed)
4. Save, and set Vercel's **Redirect** option so `www` redirects to the apex
   (or the reverse — pick one canonical URL, Vercel's domain settings UI has
   a toggle for this).
5. Wait for Vercel's dashboard to show both domains as "Valid Configuration"
   (green). This is usually faster than Railway's check but can still take a
   while depending on Hostinger's propagation.

**✅ You should now have:** `https://thought2build.com` loading the app over
HTTPS with a valid certificate, and `https://www.thought2build.com` redirecting
to it.

---

## Phase 5 — Wire the two domains together

Now that both real domains resolve, go back and fix the placeholders from
Phase 1:

1. Railway → `backend` service → **Variables**:
   ```
   FRONTEND_URL=https://thought2build.com
   ALLOWED_HOSTS=api.thought2build.com,*.up.railway.app
   ```
   (Keep the `*.up.railway.app` entry — Railway's own internal health checks
   use that hostname even after you attach a custom domain.)
2. Copy the same two variables onto `worker` and `worker-fast` if you didn't
   set up shared variables in Phase 1.
3. Redeploy the `backend` service so it picks up the change (Railway →
   **Deployments** → **Deploy**, or just push any commit).
4. Vercel: no change needed here, `VITE_API_URL` is already
   `https://api.thought2build.com` from Phase 3.

**✅ You should now have:** frontend and backend both on their final domains,
backend aware of the frontend's real URL (needed for CORS and OAuth
redirects).

---

## Phase 6 — Google OAuth for the real domain

Sign-in won't work until Google knows about your real domain. Follow the
**"Google OAuth" → "How to set up credentials"** section of the handbook,
using your real domain this time:

- **Authorized JavaScript origins:** `https://thought2build.com`
- **Authorized redirect URIs:** `https://thought2build.com/auth/callback`

Then in Railway, set:
```
GOOGLE_CLIENT_ID=<from Google Cloud Console>
GOOGLE_CLIENT_SECRET=<from Google Cloud Console>
```
Redeploy the backend.

> Common mistake (called out in the handbook too): the redirect URI must be
> the **frontend** domain (`thought2build.com/auth/callback`), not
> `api.thought2build.com`. Google sends the user back to the frontend page,
> which then talks to the backend itself.

**✅ You should now have:** clicking "Sign in with Google" on
`https://thought2build.com` completes a real login and lands you on the
dashboard.

---

## Phase 7 — Wire up at least one LLM provider

Without this, every generation will fail. Follow the "LLM Providers" section
of the handbook for whichever provider(s) you're using (Anthropic is the
simplest to start with). Set the key(s) in Railway, redeploy.

**✅ You should now have:** an API key set for at least one of
`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`. (There is also an
optional fourth provider, `OPENROUTER_API_KEY` — an open-weight-model
fallback tier, issue #152. Skip it for go-live; it needs its own
promotion gate before it belongs in `LLM_PROVIDER_PRIORITY`, see
`docs/RUNBOOK.md` §8.5.)

---

## Phase 8 — Automate future deploys (optional but recommended)

Right now every change requires you to manually redeploy in Railway/Vercel.
Follow **Step 3 ("Set up GitHub Secrets")** in the handbook to wire up
`.github/workflows/ci.yml` so pushing to `main` deploys automatically. Once
the secrets are in place, also set the repository **variable**
`PRODUCTION_DEPLOY_ENABLED=true` (GitHub repo → **Settings** → **Secrets and
variables** → **Actions** → **Variables** tab) — the deploy job is
intentionally gated off until you flip this, so it can't fire during earlier
testing.

Only the Vercel secrets are needed. Railway deploys itself: each of `backend`,
`worker`, and `worker-fast` is connected to `main` with **Auto deploys when
pushed to GitHub** + **Wait for CI** (service → **Settings** → **Source**), so it
builds the commit once CI is green. CI has no `railway up` step and needs no
`RAILWAY_TOKEN` — re-adding one would double-build every push and fail on the
services' repo-root-relative config paths.

This step can be skipped for launch day itself and done the next day once
things are calm — manual deploys work fine in the meantime.

---

## Phase 9 — Pre-launch checklist (solo-launch version)

`docs/PRODUCTION_RELEASE_GATE.md` is written for a team with a staging
environment; you have one environment, so treat this as the trimmed
equivalent. Go through it right before you announce the site is live:

**Environment sanity**
- [ ] `ENVIRONMENT=production` is set on all three application services
- [ ] `FRONTEND_URL` starts with `https://` and matches your real domain
      exactly (no trailing slash)
- [ ] `ALLOWED_HOSTS` includes `api.thought2build.com`
- [ ] `JWT_PRIVATE_KEY` / `JWT_PUBLIC_KEY` are the ones you generated in
      Phase 0, **not** the placeholder or the repo's local `.pem` files
- [ ] `ENCRYPTION_MASTER_KEY` is the Fernet key you generated, not a
      placeholder
- [ ] `PAYMENTS_ENABLED=false` (confirmed off, per plan)
- [ ] `METRICS_TOKEN` is set to a real random value
- [ ] `WEB_CONCURRENCY=1` is set on `backend` (launch cost lever, Phase 1) —
      workers don't need it. Remember to raise it to `2` when traffic arrives
      (Phase 12)

**It actually boots correctly**
- [ ] `https://api.thought2build.com/health` returns `{"status":"ok",...}`
- [ ] Railway's **Deployments** tab shows `backend`, `worker`, and `worker-fast`
      all green/running, not just `backend`
- [ ] Database migrations ran without error — this happens automatically on
      every backend deploy (`entrypoint.sh` runs `alembic upgrade head`
      before starting the server); check the `backend` service's deploy logs
      for `alembic` output with no errors

**Security basics**
- [ ] No `.env` files were committed or deployed as build artifacts — Railway
      and Vercel only use what's typed into their dashboards
- [ ] `https://api.thought2build.com/metrics` requires the `METRICS_TOKEN` (try
      it without one — it should reject you, not show data)
- [ ] `curl -i https://api.thought2build.com` from any origin other than your
      frontend gets blocked by CORS if you try a cross-origin fetch from the
      browser console at another site (optional spot-check, not required)

If you have time, it's also worth skimming the **Environment Gate** and
**Security Gate** tables in `docs/PRODUCTION_RELEASE_GATE.md` — most rows
won't apply (Razorpay, Langfuse, Storyboard-specific items) since
those are off, but the general env-var hygiene items still do.

**Your data is backed up**
- [ ] Railway's managed Postgres backups are enabled (Layer 1) — one click in
      the database service, confirm the retention shown.
- [ ] The off-platform encrypted backup is live (Layer 2): the **DB Backup**
      GitHub Action (`.github/workflows/db-backup.yml`) has run once via
      *Run workflow* and uploaded an artifact to your bucket. Full setup (bucket,
      `age` keypair, Actions secrets) and one restore drill are in
      `docs/BACKUP_RESTORE.md` — do the drill at least once so you know a restore
      actually works before you need it.

---

## Phase 10 — Go-live smoke test (do this in your actual browser)

Walk this golden path end to end on `https://thought2build.com` — this is the
minimum from `docs/SMOKE_TEST_CHECKLIST.md` that proves the whole pipeline
works for a real user:

1. [ ] Load `https://thought2build.com` — no console errors (open DevTools →
       Console)
2. [ ] Click "Sign in with Google" → complete login → land on the dashboard
3. [ ] Confirm you received starting credits (shown in the UI)
4. [ ] Create a new workspace
5. [ ] Enter an idea and generate the **Spec** stage — confirm it streams
       text in real time and a credit is deducted
6. [ ] Approve/finalise the Spec stage — confirm the Plan stage unlocks
7. [ ] Sign out, sign back in — confirm the session persists correctly
8. [ ] `https://api.thought2build.com/health` still returns `ok`

If all 8 pass, the core product works end to end in production. Don't chase
every item in the full smoke checklist tonight — do that over the next few
days as you have time; this subset is what actually blocks telling people
the site is live.

---

## Phase 11 — Payments: off until verified

You're launching with:
```
PAYMENTS_ENABLED=false
```
This means: the app runs normally, users get their starting free credits,
generation works — but the "Buy credits" button and checkout are inert.
`GET /billing/package` reports `enabled: false` and the frontend hides
checkout accordingly. Nothing about this is fragile — you can leave it off
indefinitely with no side effects.

Before enabling Razorpay purchases, complete the
[payment launch verification](PAYMENT_LAUNCH_VERIFICATION.md). This includes
migration 0046, real provider test-mode payment and refund exercises, worker and
reconciliation checks, and the required dispute webhook subscriptions. An HTTP
200 for a sample webhook alone does not verify a credit grant.

Keep `PAYMENT_PROVIDER=razorpay` and `PAYMENTS_ENABLED=false` during deployment.
Production requires live Razorpay credentials; use a separate staging environment
for test keys. Enable purchases only after reviewing those verification results.

---

## Phase 12 — What to watch during week one

With no dedicated monitoring stack set up yet (Sentry/Langfuse/Grafana are
all optional and skipped for this launch), your visibility is:

- **Railway → each service → Logs tab** — glance at this daily. Errors show
  up here first.
- **`https://api.thought2build.com/health`** — bookmark it, check when in doubt.
- **`https://api.thought2build.com/metrics`** (with your `METRICS_TOKEN`) — has
  request counts, error rates, and generation counters if you want to look
  closer; not required daily.
- **Vercel → Deployments** — shows build/runtime status for the frontend.
- **Your own product usage** — the best smoke test each day is generating one
  spec yourself.
- **Railway → Usage / each service → Metrics** — glance at the **memory** graph
  and the running month-to-date cost. This is what confirms (or corrects) the
  ~$18–23/month estimate in the cost section above; the modeled numbers are a
  starting point, the graph is the truth. Two things to act on:
  - If real traffic starts arriving and the `backend` service is busy, **set
    `WEB_CONCURRENCY=2`** again (you dropped it to 1 for launch in Phase 1) so
    you have in-process redundancy — the small memory increase is worth it once
    people depend on the site.
  - If a service's memory is far above the estimate, that's the one to
    investigate first (it's the cost driver), before considering the bigger
    levers noted in the cost section.

If you want a lightweight early-warning system without full observability
setup, Sentry's free tier (`SENTRY_DSN` in both Railway and Vercel) is the
single highest-value thing to add next — it emails you on unhandled errors
without any dashboard-watching required. Not needed for day one.

---

## Phase 13 — If something breaks

**Frontend won't load / blank page:** check Vercel's deploy logs first (a
failed build is the most common cause), then check the browser console for
CORS or `VITE_API_URL` errors.

**"redirect_uri_mismatch" on sign-in:** the URI registered in Google Cloud
Console doesn't exactly match `FRONTEND_URL + /auth/callback` — check for
`http` vs `https` and trailing slashes.

**Backend deploy fails on Railway:** open **Deployments** → the failed
build → read the logs. Almost always a missing/misspelled environment
variable, or `DATABASE_URL` not converted to the `postgresql+asyncpg://`
scheme.

**Generation fails immediately:** check that at least one LLM provider key is
set and valid (test the key directly against the provider's API if unsure).

**Billing-related errors even though payments are off:** shouldn't happen —
`PAYMENTS_ENABLED=false` disables checkout entirely — but if you see any,
confirm `PAYMENT_PROVIDER` is set to a valid value (`razorpay` or
`razorpay`); production refuses to boot on an empty/invalid value even
while disabled.

**Rolling back:** Railway keeps previous deployments — go to **Deployments**
→ pick the last known-good one → **Redeploy**. Vercel works the same way
under its **Deployments** tab → **Promote to Production** on an older build.
Neither requires touching DNS or the domain setup, so a rollback is quick and
safe.

**Genuinely stuck:** `docs/RUNBOOK.md` has a much deeper operational
reference (key rotation, Redis/DB issues, worker lane problems, billing
reconciliation) once you're past the first-week basics covered here.

---

## Quick reference: full checklist in one place

- [ ] Phase 0 — secrets generated
- [ ] Phase 1 — Railway: Postgres + Redis + `backend`/`worker`/`worker-fast`
      deployed, `/health` OK
- [ ] Phase 2 — `api.thought2build.com` → Railway, HTTPS valid
- [ ] Phase 3 — Vercel frontend deployed with `VITE_API_URL` set
- [ ] Phase 4 — `thought2build.com` + `www` → Vercel, HTTPS valid
- [ ] Phase 5 — `FRONTEND_URL` / `ALLOWED_HOSTS` updated to real domains,
      backend redeployed
- [ ] Phase 6 — Google OAuth updated for real domain, sign-in works
- [ ] Phase 7 — at least one LLM provider key set
- [ ] Phase 8 — GitHub Actions auto-deploy wired up (optional, can defer)
- [ ] Phase 9 — pre-launch checklist passed
- [ ] Phase 10 — golden-path smoke test passed on the real domain
- [ ] Phase 11 — payments confirmed off, plan noted for next week
- [ ] Phase 12 — you know where to look if something breaks
