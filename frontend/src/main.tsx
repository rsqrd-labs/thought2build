import * as Sentry from "@sentry/react"
import ReactDOM from "react-dom/client"

import App from "./App"
import { initAnalytics } from "./services/analytics"
import "./index.css"

if (import.meta.env.VITE_SENTRY_DSN) {
  Sentry.init({
    dsn: import.meta.env.VITE_SENTRY_DSN,
    integrations: [Sentry.browserTracingIntegration()],
    tracesSampleRate: 0.1,
  })
}

initAnalytics()

const root = document.getElementById("root")
if (!root) {
  throw new Error("Thought2Build root element is missing")
}
ReactDOM.createRoot(root).render(<App />)
