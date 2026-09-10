# Pipeline reliability remediation — 2026-09-10

This implements the eight findings in [the original review](PIPELINE_RELIABILITY_REVIEW_2026-09-06.md). It does not claim that live model quality or production fault recovery has been benchmarked.

| Finding | Implemented behavior |
|---|---|
| 1. Incomplete shared cache identity | Output-cache namespace v2 includes private workspace/user identity, clarification answers, mode parameters, dependency content/versions, compression settings, pinned remote prompts, and a hash of shipped generation code/policy. Refinements also identify their exact rendered prompts. |
| 2. Mutable generation inputs | Durable runs store a deep input snapshot and resolved prompts before chunk checkpoints. Retries use those inputs. A resume rejects changed input/release identity. Artifact/history rows retain source provenance; finalization detects drift. Workspace/source edits and finalization serialize through a workspace row lock. Clarification edits invalidate existing artifacts atomically. |
| 3. Evaluation of old fixtures only | Offline fixture grading is explicitly labeled. A separate CI gate generates baseline and candidate packages through production prompt construction, routing, and chunk generation, then compares actual outputs. It records prompts, code/corpus identities, attempted routes, token usage, estimated cost, errors, and elapsed time. Missing credentials, failed generation, or missing candidate evidence fails the live gate. |
| 4. Spoofable/advisory structural checks | Heading detection excludes fenced, quoted, indented, and commented examples. Objective structure and reference failures block readiness independently of refund eligibility. Fresh/cached generation, finalization, and stale acceptance enforce readiness. An explicit version-specific owner override remains supported. ZIP and GitHub exports include VERIFICATION.json with provenance, structural/local technology findings, and overrides. |
| 5. Local safety lost on external failure | Local policy findings are persisted before external verification and merged into timeout/error results. Recovery and finalization retain deterministic blockers. Export metadata reevaluates local policy too. |
| 6. Silent context loss | Oversized critical sections abort prompt construction instead of being dropped. Compression must retain normative source blocks. Each fully assembled chunk request checks estimated input plus reserved output against a conservative fraction of the routed model context limit. |
| 7. Unpinned remote prompts | Remote bodies require an explicit version and matching SHA-256. Unpinned names use reviewed local prompts; a broken pin fails rather than silently changing the body. Resolved prompts are durable. Demo Day time/environment directives remain locally enforced. |
| 8. Expired policy/date-sensitive tests | The evidence-backed policy refresh already committed in c8189e0 is preserved. A daily CI check warns/fails seven days before expiry. Policy behavior tests use an explicit fixed clock; maintenance checks use actual UTC time. |

## Deployment

1. Apply migration `0047_generation_input_identity` before deploying code that reads the new columns. This PR is stacked on #156, which includes the verified policy refresh and prerequisite dependency fixes.
2. Deploy matching API and generation-worker builds. Drain old workers during rollout. New workers reject legacy queued runs without snapshots and settle them through the existing failure/refund path; legacy partial runs without reproducible prompts cannot advertise a free resume. A release change can require regeneration rather than combining chunks from different releases.
3. Output cache invalidation is automatic through the v2 namespace. Existing v1 entries expire normally. Legacy artifact provenance stays unknown; exports report this rather than inventing lineage. Regeneration establishes provenance; explicit stale acceptance records the owner's acceptance of current source inputs.
4. For each remote prompt, configure `LANGFUSE_PROMPT_PINS` with the exact prompt name and an object containing a positive integer `version` and a lowercase, 64-character `sha256` of the fetched prompt body. Use `{}` for local prompts only. Roll out pin changes as a reviewed release, then restart API/workers together.
5. Configure provider secrets for the `generated-prompt-gate` job. Its default ceiling is 100 provider attempts per revision (200 total), including retries and compressor calls. This is a call ceiling, not a dollar ceiling. Evidence artifacts expire after 14 days; retain the approved release evidence separately. Fork PRs need an appropriately trusted evaluation run because they do not receive secrets.
6. Frozen inputs and rendered prompts contain user content. They remain in the existing private database under workspace/run deletion relationships; use the same access controls, backup handling, and retention policy as artifact content. They are not included verbatim in export verification metadata.

## Validation scope

Regression coverage includes cache isolation, nested snapshot immutability, heading spoofing, critical-section overflow, total context reservation, remote pin mismatch, mode directives, local safety timeouts, manual-edit/stale-acceptance gates, and policy expiry boundaries. A fake-provider evaluation test runs production chunking and verifies call limits and recorded failures without spending provider funds.

Disposable PostgreSQL/Redis checks cover the migration chain, actual concurrent finalization/source edits, snapshot JSONB roundtrips, and exports. Pre-isolation workspace results (also included separate GitHub integration changes):

- Backend CI test selection: **3,015 passed, 3 skipped**. This uses CI's exclusions for schema-owning billing/finalization/Storyboard suites; finalization was run separately below.
- Backend harness contracts: **783 passed**.
- Isolated PostgreSQL finalization/source-lineage integration suite: **10 passed**.
- The workspace migration chain, including the separate GitHub migration, applied successfully to a fresh disposable database. Only migration `0047` belongs to this PR.
- `ruff check backend` and the changed evaluation scripts: passed.
- `black --check backend` and the changed evaluation scripts: passed (414 files).
- Offline committed-fixture grading: passed for all three workspaces; this is explicitly not candidate quality evidence.
- Actual-date policy freshness, workflow YAML parsing, and `git diff --check`: passed.

The test runners reported 10 backend and 3 harness warnings, including dependency deprecation and unawaited connection-cancellation warnings. These did not fail tests; the result is not a warning-free or production fault-injection certification.

No paid candidate/baseline generation was run locally. Before production promotion, run the generated gate and review complete packages for requirement fidelity, cross-stage consistency, and usefulness. Deterministic structure checks cannot establish semantic correctness. Context accounting uses a conservative estimate rather than an exact tokenizer. The existing worker lease/deadline tradeoff and broader fault-injection benchmarks described in the original review remain operational validation work, not guarantees added by these changes.


## Isolated PR verification

The pipeline changes were copied into a separate worktree based on #156, excluding the uncommitted GitHub integration work. On that exact code:

- 560 focused pipeline tests passed.
- 10 PostgreSQL finalization/source-lineage integration tests passed.
- 8 GitHub export service tests passed.
- The migration chain through this PR's `0047` applied successfully.
- Ruff and Black passed for the backend and changed evaluation scripts.
