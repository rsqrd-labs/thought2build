import { useEffect } from "react"
import { Link } from "react-router"
import { BrandLockup } from "../components/shared/BrandLogo"
import { MarkdownRenderer } from "../components/workspace/MarkdownRenderer"
import {
  LEGAL_CONTACT_EMAIL,
  LEGAL_ENTITY_DESCRIPTION,
  LEGAL_JURISDICTION,
  LEGAL_LAST_UPDATED,
} from "./legalConstants"

// Privacy Policy. Registered OUTSIDE the auth guard (must be readable
// without signing in) and must not import any authenticated client-side
// store — same constraint as LegalRetention.tsx and PublicWorkspaceView.
//
// Cookie disclosure lives in this page rather than a separate Cookie Policy:
// the backend sets exactly one cookie (refresh_token, strictly necessary —
// see backend/routers/auth.py), so a dedicated page would be unjustified.
//
// Every factual claim below is grounded in the app's actual behavior as of
// the date in legalConstants.ts. The India-specific legal framing (DPDPA) and
// the GDPR/CCPA section are informed by common practice, not
// jurisdiction-specific legal review — get real counsel review before real
// users sign up, and fill in the placeholders in legalConstants.ts first.

const PRIVACY_MARKDOWN = `
**Last updated: ${LEGAL_LAST_UPDATED}**

This Privacy Policy explains what personal data Thought2Build collects, how it is
used, and with whom it is shared. ${LEGAL_ENTITY_DESCRIPTION}

## 1. Information We Collect

- **Account information** — your email address, Google account identifier,
  name, and profile photo, obtained through Google Sign-In.
- **Content** — the problem statements, clarification answers, and generated
  specification, plan, harness, and task artifacts associated with your
  workspaces.
- **Billing information** — your email address, shared with our payment
  processor to complete a purchase. We do not collect or store payment card
  details; checkout is hosted entirely by our payment processor.
- **Integration information** — an encrypted GitHub access credential or App
  installation record, if you connect a GitHub repository.

We do not store your IP address in our application database. It is
processed transiently for rate limiting and may appear in error-monitoring
diagnostics or standard infrastructure access logs (see **Who We Share Your
Information With**, below).

## 2. How We Use Your Information

We use this information to operate Thought2Build's generation pipeline, process
credit purchases, synchronize content with GitHub when connected, maintain
the security and reliability of the Service, and respond to your inquiries.

## 3. Legal Basis for Processing

${LEGAL_ENTITY_DESCRIPTION} As an India-based operator, we process personal
data primarily under the Digital Personal Data Protection Act, 2023 (DPDPA)
and the Information Technology (Reasonable Security Practices and Sensitive
Personal Data or Information) Rules, 2011. Processing is based on your
consent as a Data Principal, given when you sign in and use the Service; you
may withdraw consent at any time by discontinuing use of the Service and
requesting account deletion (see **Your Rights**, below). For any
privacy-related grievance, contact us at ${LEGAL_CONTACT_EMAIL}.

Where applicable, we also recognize the rights afforded under the General
Data Protection Regulation (GDPR) and the California Consumer Privacy Act
(CCPA). The rights described in **Your Rights**, below, are designed to
satisfy both frameworks; contact us if you wish to invoke a specific right by
name.

## 4. Who We Share Your Information With

We do not sell your personal data. We share it only as necessary to operate
the Service, with the following categories of recipients:

- **AI model providers.** To generate the artifacts you request, we transmit
  your idea, clarification answers, and prior stage content to the AI
  providers selected by Thought2Build — Anthropic, OpenAI, or Google — as context
  for generation. Provider selection is an internal service operation.
- **Payment processor.** Razorpay hosts checkout and processes credit purchases.
  We send your email address, purchase amount, currency, and account and
  checkout identifiers needed to associate the payment with your account.
  We do not collect or store your payment card details.
- **GitHub.** If you connect a repository, we synchronize the content you
  export — specification, plan, harness, and task files, along with
  generated issues and pull requests — to the repository you designate.
- **Search providers.** When research grounding is enabled for a workspace,
  we send a query derived from your idea to Brave Search to ground
  generation in current information.
- **Monitoring and observability providers.** We use error-monitoring and
  LLM-observability tooling, including Sentry and Langfuse, to maintain,
  secure, and improve the Service. These providers may receive diagnostic
  information — including your IP address, browser user agent, and portions
  of your content — with credentials and secrets automatically redacted
  before transmission.
- **Hosting and infrastructure providers.** We use third-party cloud
  infrastructure to host and operate the Service.

## 5. Public Sharing

You may choose to make a workspace or Storyboard publicly viewable through a
shareable link that does not require signing in to access. Public sharing is
optional and fully within your control — you may disable it at any time,
which removes public access, subject to a brief caching delay.

## 6. Cookies

We use a single, strictly necessary cookie:

| Cookie | Purpose | Type | Lifetime |
|---|---|---|---|
| \`refresh_token\` | Keeps you signed in between visits | Strictly necessary (first-party) | 7 days |

This cookie is essential for authentication, is protected with
industry-standard security attributes, and cannot be used to track you
across other sites. We don't use advertising, analytics, or third-party
tracking cookies.

## 7. Data Retention

We retain your active work indefinitely and remove only redundant history and
internal telemetry on a defined schedule. Deleting a workspace moves it to
trash for a window before permanent deletion, during which you may restore or
export it. See our full [Data Retention Policy](/legal/retention) for the
applicable windows, which are versioned and may change over time.

## 8. Your Rights

You may, at any time:

- **Export** any workspace as a ZIP or PDF, or view and restore anything in
  trash.
- **Request access, correction, or erasure** of your personal data, or object
  to a specific use of it, by contacting us at ${LEGAL_CONTACT_EMAIL}. We
  handle these requests individually rather than through automated
  self-service tooling; we will verify your identity and respond within a
  reasonable time.
- **Delete your account.** Contact us to request account deletion; this may
  be delayed if you have unsettled billing debt on your account (see our
  Terms of Service).

## 9. Children's Privacy

Thought2Build is not directed at, and is not intended for use by, individuals
under 18 years of age. We do not knowingly collect personal data from anyone
under 18.

## 10. International Data Transfers

Certain recipients described above — including our AI model providers,
payment processors, and GitHub — process data outside ${LEGAL_JURISDICTION}.
By using the Service, you acknowledge that your data may be processed in
jurisdictions with data protection laws that differ from your own.

## 11. Security

We encrypt sensitive stored values, including connected-integration
credentials, at rest; protect state-changing requests with anti-forgery
tokens; and rate-limit requests to reduce abuse. No system is completely
secure, but we take reasonable measures to protect your data.

## 12. Changes to This Policy

We may update this Privacy Policy from time to time. We will revise the
"Last updated" date above when we do. Continued use of the Service after a
change constitutes acceptance of the updated policy.

## 13. Contact

For questions about this Privacy Policy, or to exercise the rights described
above, contact us at ${LEGAL_CONTACT_EMAIL}.
`.trim()

export default function LegalPrivacy() {
  useEffect(() => {
    document.title = "Privacy Policy — Thought2Build"
    return () => {
      document.title = "Thought2Build"
    }
  }, [])

  return (
    <div className="public-view-shell">
      <header className="public-view-cover">
        <BrandLockup variant="compact" className="public-view-brand" />
        <h1 className="public-view-title">Privacy Policy</h1>
      </header>

      <main className="public-view-content legal-prose">
        <MarkdownRenderer content={PRIVACY_MARKDOWN} />
      </main>

      <footer className="public-view-footer">
        <p>
          <Link to="/legal/terms" className="public-view-footer-link">
            Terms of Service
          </Link>
          {" · "}
          <Link to="/legal/retention" className="public-view-footer-link">
            Data Retention Policy
          </Link>
          {" · "}
          <Link to="/" className="public-view-footer-link">
            ← Back to Thought2Build
          </Link>
        </p>
      </footer>
    </div>
  )
}
