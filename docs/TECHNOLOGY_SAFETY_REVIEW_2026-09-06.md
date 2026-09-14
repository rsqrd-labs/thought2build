# Technology safety policy review — 6 September 2026

The backend contract suite rejected the policy because its 5 August review was
older than the configured 30-day window. The existing denylist and verification
mechanisms were re-reviewed before refreshing `last_reviewed`; the freshness
window, blocking severities, runtime checks and package checks are unchanged.

These denylists are conservative product restrictions for newly generated stacks,
not an exhaustive list of every retired version or a claim that every blocked
version lacks all vendor support. Versions outside the static list still require
the runtime lifecycle and package vulnerability checks.

| Existing restriction | Review evidence and decision |
| --- | --- |
| Python through 3.10 | The [Python release table](https://www.python.org/downloads/) lists 3.10 support ending in October 2026. Retain the conservative floor for new stacks; 3.12 security support continues through October 2028. |
| Node.js through 18 | The [official EOL list](https://nodejs.org/en/about/eol) confirms these releases are EOL. Retain the static restriction. Node 20 is also EOL and must be rejected by the live lifecycle check; absence from the static list is not approval. |
| Java through 11 | The [Oracle support roadmap](https://www.oracle.com/java/technologies/java-se-support-roadmap.html) confirms that Java 11 still has extended support for entitled customers. Retain the existing new-stack product restriction, not a universal EOL claim. Vendor-specific support and licensing remain relevant. |
| GPT-3/3.5 family | OpenAI identifies [GPT-3.5 Turbo Instruct](https://developers.openai.com/api/docs/models/gpt-3.5-turbo-instruct) as an older model on the legacy Completions endpoint. Retain the restriction against this family in new generated stacks; it does not claim every GPT-3.5 endpoint has shut down. |
| Gemini 1.x family | The [Gemini changelog](https://ai.google.dev/gemini-api/docs/changelog) records the retirement of the 1.5 generation. Retain the legacy-family restriction. |
| Claude 1.x/2.x family | Anthropic's [deprecation history](https://platform.claude.com/docs/en/about-claude/model-deprecations) lists the older Claude generations as retired. Retain the restriction. |

The package ecosystem and runtime product mappings remain applicable. They route
individual selections to OSV and runtime lifecycle verification rather than
certifying installed dependencies once per policy review. Plan-prompt denylist
text is generated from this JSON. Its separate `DENYLIST_LAST_REVIEWED` anchor
was refreshed to the same date. The prompt version was bumped to asdd-v2.11.1
as required by the prompt-eval gate, although prompt content is unchanged.
