# Pipeline and system-prompt reliability review

Reviewed September 6, 2026, against working tree based on `887bbdd`.

The system has substantial execution safeguards, but does not yet guarantee that a delivered package reflects one consistent set of user inputs or that passing its gates means the package is structurally usable. The highest-value work is to make input identity, source lineage, validation, and evaluation authoritative. Adding more instructions to the prompts will not resolve those problems.

Scope: core Spec → Plan → Harness → Tasks generation, prompt assembly and loading, output caching, worker handoff, checkpoints/resume, finalization and edits, quality checks, export verification, frontend stream state, and evaluation/release configuration. Existing unrelated working-tree changes were left intact. This is a code review and focused local verification, not a production availability assessment or live model benchmark.

## Findings, in priority order

### 1. High: generation cache identity omits inputs that change the answer

Evidence: `backend/services/pipeline/stage_manager.py:3826`, `backend/services/llm/cost_cache.py:22`, `backend/services/pipeline/prompt_builder.py:232`, `backend/prompts/spec.py:37`, `backend/prompts/demo_day.py:796`.

The output cache includes the problem statement, upstream artifact hashes, route, and declared prompt version. It omits clarification answers, Demo Day time budget and restricted-environment settings, and the actual remote system-prompt revision/body. Spec has no upstream artifact hashes to distinguish requests.

Two workspaces with the same initial statement and route but different clarification answers can therefore receive the same cached Spec. The cache is also shared across workspaces: an artifact incorporating one user's private clarification details can potentially be delivered to another user with the same initial statement. This is a code-path risk; no cross-user request was executed during this review. Different Demo Day constraints can likewise collide despite changing the rendered prompts.

Fix: define a canonical generation-input manifest and derive output-cache identity from all effective inputs. Include tenant scope for private generated outputs, clarification content, mode parameters, prompt content/revision, compression policy, output contract, and source versions/hashes. Prefer hashing the exact effective prompt plus generation settings after snapshot creation. Move any early cache lookup to an identity-equivalent manifest. Invalidate the existing cache namespace on rollout.

Acceptance: same statement + different answers, tenant, time budget, environment restriction, or remote prompt revision must miss; identical authorized inputs must hit. Test the real generation call site's key construction, not just the generic key helper.

### 2. High: runs and checkpoints do not pin their upstream input snapshot

Evidence: `backend/services/pipeline/stage_manager.py:4172`, `:4352`, `:4466`, `:4592`, `:5064`, `:5690`, `:6057`; `backend/services/pipeline/generation_runs.py:333`; `backend/models/stage_generation.py:125`.

The API computes a cache key before enqueue. The worker later reloads workspace content to assemble prompts, then reloads it again for generation-time validation dependencies. Retries rebuild prompts and reuse checkpointed chunks. Runs persist chunk keys and target-stage versions, but not an immutable upstream-version/prompt manifest.

Editing a finalized Spec while Plan is in progress does not mark that Plan stale: downstream invalidation selects only finalized stages. Plan can finish with old inputs, become draft, and be finalized without a dependency-version comparison. An edit between queueing and prompt assembly can also cause new-input output to be cached under an old-input key. Resume after a source edit or prompt deployment can combine chunks from different input contracts as long as the chunk keys still match.

Fix: capture source StageVersion IDs, problem/clarification/settings hashes, effective prompt identity, and chunk-plan version once. Use that snapshot for generation, validators, critics, caching, and resume. Compare source revisions transactionally before publishing/finalizing; preserve useful output but mark it stale when dependencies moved. Resume only when the full manifest matches. Do not hold database row locks across provider calls.

Acceptance: edit Spec during Plan generation, between enqueue/preflight, and between checkpoint/resume. No resulting artifact may be represented as current against changed inputs; no output may populate a cache entry for different inputs.

### 3. High: the prompt-eval release gate does not evaluate changed prompts

Evidence: `harness/prompt_eval/run.py:77`, `:125`, `:250`; `.github/workflows/prompt-eval.yml`; `docs/PRODUCTION_RELEASE_GATE.md`; acknowledged in comments in `backend/prompts/base.py:40`.

The runner loads committed Markdown artifacts and applies deterministic graders. It never generates the four stages from the proposed prompts. `--version` and `--baseline` label the report; the baseline scores actually come from each fixture's `baseline_scores.json`. Cost is hardcoded to zero. The workflow describes live billed calls and supplies provider secrets, but this runner is an offline fixture-grading path.

A harmful prompt change can pass while grading unchanged historical answers. The workflow also omits important generation-affecting paths such as stage_manager chunk scaffolding and adapter behavior from its trigger list.

Fix: retain this job as a clearly named grader regression suite. Add a separate candidate-generation evaluation that executes the production assembly/chunking/validation pipeline, persists exact prompt/route/input provenance and artifacts, and compares candidate versus pinned baseline on the same corpus. Broaden triggers to the entire generation contract. Baseline updates must require an explicit reviewed artifact change; changing a version label is insufficient.

Acceptance: deliberately change a prompt to omit a mandatory section. The candidate-output gate must fail even when committed golden artifacts are unchanged. Report actual stage and package pass rates, cost, latency, fallback use, and human acceptance.

### 4. High: structural validity is conflated with refundable failure

Evidence: `backend/services/pipeline/artifact_validator.py:234`, `:311`; `backend/services/pipeline/stage_manager.py:4990`; `backend/services/pipeline/construction_verdict_service.py:1`.

The blocking section check uses substring membership (`heading in artifact_md`). It accepts required headings inside a code fence or quoted example. A local reproduction passed it with every Spec heading inside one code block and no section bodies.

Separately, all completeness issues other than empty output and provider-reported token truncation become advisory. That includes incomplete harness file blocks and unbalanced code fences, as well as subjective depth findings. Cross-artifact construction verification is advisory and does not fail export. Consequently, an ordinary model stop is treated as evidence sufficient to deliver structurally damaged content with warnings.

Fix: separate three decisions: whether the artifact is structurally valid, whether it is ready for downstream use, and whether a refund is owed. Use a fence-aware Markdown parser and typed artifact checks. Objectively malformed required file blocks, duplicate identifiers, unresolved required references, and invalid task graphs should prevent an ordinary ready state, without automatically triggering refunds or unlimited regeneration. Keep stylistic depth and subjective critic scores advisory. Preserve the explicit owner override, with its limitations visible on export.

Acceptance: fenced/quoted headings cannot satisfy sections; missing file bodies and invalid references cannot produce a clean ready package; structural failures do not automatically buy repeated full generations.

### 5. High: external-check timeouts can erase already-known local safety failures

Evidence: `backend/services/pipeline/tech_safety.py:482`, `backend/services/pipeline/stage_manager.py:6403`, `backend/services/pipeline/recovery_service.py:122`.

Technology analysis computes local freshness and denylist findings, then awaits external lifecycle/advisory checks. The caller wraps the entire analysis in a timeout. If that timeout or another exception occurs, it replaces the whole result with an unverified advisory and sets `blocked=False`. Local findings computed before the failed lookup are not returned. Detached check loss similarly becomes advisory after the stale-check window.

This makes the finalization outcome depend on whether external checks complete: a known local blocker can be lost when an unrelated lookup stalls. This finding is based on control flow, not a live timeout injection.

Fix: compute and persist deterministic local findings independently first. Merge bounded external results afterward; lookup unavailability must never downgrade a known blocker. Make finalization-relevant checks durable jobs or recompute their deterministic portion at the finalization boundary. Keep unavailable external evidence explicitly unknown.

Acceptance: inject an external timeout for an artifact with a known local denylist hit. The local blocking finding must survive. Kill the checking worker and verify recovery preserves the same decision.

### 6. Medium: oversized context silently loses critical contracts

Evidence: `backend/services/pipeline/prompt_builder.py:145`, `:258`; `backend/services/pipeline/problem_compressor.py:1`; `backend/services/pipeline/stage_manager.py:4524`.

When a critical upstream section does not fit the 200,000-character budget, `_section_aware_injection` drops it and increments a metric. The request continues. A local reproduction with an oversized Functional Requirements section removed its `FR-999` contract entirely. The final slice can also truncate the appended summary. The problem-statement clamp explicitly permits lossy reduction and surfaces only an advisory.

The upstream budget is per artifact and character-based; the assembled prompt token count is recorded but this path does not impose a full request-window admission check. Tasks may receive three large dependencies plus instructions and chunk context. A metric proves omission happened; it does not make that loss acceptable for requirement-preserving generation.

Fix: preserve a typed requirement/decision/reference ledger independently of narrative. Partition large critical sections rather than dropping them. Budget the complete request, including output reserve and chunk history, for each selected/fallback model. If authoritative contracts still do not fit, stop with an actionable scope decision or hierarchical generation strategy. Explicitly report omitted input IDs rather than claiming preservation.

Acceptance: a critical section exceeding the cap retains every identifier and obligation through partitioning, or fails explicitly before generation; it must never silently disappear.

### 7. Medium: remote system prompts bypass release identity and mode parameters

Evidence: `backend/prompts/base.py:586`, `backend/services/langfuse_service.py:330`, `backend/services/pipeline/stage_manager.py:4793`.

`load_prompt` requests the remote name without a pinned version and receives only a body. Local security prose is appended, but the remote body can change stage structure and behavior outside the repository's prompt-version gate. Cost records still use the local stage prompt version. Worker-local TTLs and fallback behavior can serve different bodies under that identity. A nonempty remote body also replaces a parameterized Demo Day fallback; the local cache-key fix distinguishes parameters, but does not interpolate them into the remote system body. User prompts still carry mode guidance, so this is conflicting/missing system guidance rather than proof all mode constraints disappear.

Fix: deploy an immutable prompt bundle mapping each stage/mode to reviewed prompt content and schema version. Record the actual body hash, remote revision, and fallback source on each run. Keep parameterized safety/environment constraints in a local composition layer applied after remote loading. Reject remote bundles incompatible with the validator/chunk contract.

Acceptance: changing a remote prompt changes release/cache provenance; retries remain on their original bundle; restricted-environment directives survive every remote and local-fallback path.

### 8. Medium: the checked-in technology policy has expired

Evidence: `backend/services/pipeline/tech_safety_policy.json:3`, `backend/services/pipeline/tech_safety.py:190`.

The policy was last reviewed on August 5, 2026 and has a 30-day maximum age. On September 6, the local freshness function returns critical `technology_policy_stale`. This affects otherwise valid artifacts independently of their chosen stack. Focused tests also show date-sensitive expectations no longer hold.

Fix: perform an actual policy review and republish its evidence; do not merely advance the date. Add scheduled freshness monitoring and an owner before expiry, and test policy freshness separately from unrelated generation behavior. Use an injected fixed date in behavioral tests and explicit boundary tests for expiry. Distinguish policy-maintenance incidents from generated-stack defects in the UI and operations metrics.

Acceptance: warning precedes expiry; routine tests do not change outcome solely because today's date advanced; expiry remains explicitly tested. Vendor lifecycle facts were not revalidated during this review.

## System-prompt improvements

The prompts already provide stable IDs, explicit stage contracts, worked examples, clarification precedence, untrusted-input framing, and an internal verification checklist. Preserve those useful parts. The next revision should reduce duplicated policy and move mechanical obligations into code.

1. **Generate structure from one contract.** Required headings, chunk scopes, keep-lists, cross-stage references, and validator rules currently span several modules. Define one versioned stage schema and derive these surfaces from it. This removes the need to manually keep prose and parsers synchronized.
2. **State product-data precedence clearly.** Distinguish system safety/output rules from factual authority: accepted user clarifications resolve ambiguous original intent; accepted upstream artifacts constrain downstream design; research is evidence, not permission to change scope. Material contradictions should become explicit unresolved decisions.
3. **Label fact versus assumption versus proposal.** Require source IDs/evidence for inherited requirements and external capability/version claims. Allow implementation/design proposals with explicit status. A prompt asking for precise support dates or verified contrast does not prove those checks occurred.
4. **Externalize coverage bookkeeping.** The Tasks prompt asks the model to build a complete coverage map internally before writing. Supply a deterministic manifest of requirement IDs, plan contracts, and actual harness paths, then validate the join after generation. Derive task counts and effort summaries from parsed tasks.
5. **Use operation-scoped output contracts.** A chunk call should receive its exact scope, authoritative shared ledger, prior committed decisions, output limits, and termination contract. Require explicit gap records for unresolved contracts. Avoid duplicating whole-document obligations across independent chunks.
6. **Treat fences as model guidance, not an isolation boundary.** Content-bound nonces resist textual delimiter spoofing; they do not mechanically enforce instruction hierarchy inside an LLM. Retain least-privilege generation and export defenses, and add live adversarial cases that measure actual behavior instead of only checking that defense sentences exist.
7. **Make repair bounded and targeted.** Feed machine-readable validator errors into a repair of the affected section/file, with a fixed attempt and cost budget. Revalidate the whole dependency graph after patching. Preserve the original when repair fails, visibly marked incomplete.

## Implementation sequence

**First: correctness and immediate operational maintenance.** Fix cache identity and invalidate its namespace; introduce source manifests and stale-result checks; preserve local technology findings on timeout; actually review the expired policy. Add regression cases for the concrete triggers above.

**Next: make readiness meaningful.** Separate billing, artifact validity, advisory quality, and owner override states. Implement fence-aware structure and required-reference validation. Persist finalization-relevant check jobs. Make exports include source versions and verification/override status.

**Then: establish a trustworthy release gate.** Build candidate-versus-baseline live pipeline evaluation with immutable prompt bundles. Include small and large products, standard and Demo Day, restricted environments, conflicting inputs, adversarial source text, multilingual input, dependency edits, and partial resumes. Evaluate complete packages: individually plausible stages can still contradict one another.

**Afterward: simplify orchestration incrementally.** `stage_manager.py` is over 7,000 lines and combines queue handoff, prompt/chunk construction, billing transitions, recovery, validation, refinement, and advisory scheduling. Extract these behind existing tested seams after fixing invariants; avoid a large rewrite that changes all state transitions at once.

## Reliability evidence to collect before calling the pipeline reliable

- One committed run must produce one terminal result and one idempotent settlement, even with duplicate job delivery, cancellation, or API/worker loss.
- Kill a worker after charge, after a checkpoint, and around final commit. Verify eventual recovery, no duplicate version, correct retained partial output, and no duplicate refund. Use real isolated PostgreSQL/Redis processes for these tests.
- Exercise Redis outage during enqueue and cache writes, provider 429/5xx/truncation, long reasoning without visible text, external-check timeout, and database failure during terminal commit.
- Test browser refresh/reconnect against the durable run result, plus stale or duplicate progress messages. Displayed draft content must converge to the persisted version.
- Measure queue wait, preflight, provider, validation, persistence, time to draft, and time to finalizable separately. Track recovery age, source-drift rejections, output-cache identity misses, structural failure rate, and override rate by route/mode/prompt version.
- Track package acceptance and requirement preservation alongside technical completion and COGS. A `succeeded` run is not equivalent to an implementation-ready package.

The shared bulk worker has a documented hard-kill limitation: its approximately 1,800-second lease outlasts the default 600-second generation deadline (`backend/worker.py:92`). Automatic hard-kill continuation can lose to deadline settlement even though checkpoints survive for later recovery. Decide explicitly whether the promised behavior is automatic continuation or bounded settlement plus user resume; test that promise under the actual queue topology. Current comments already acknowledge this tradeoff.

## Local validation performed

No provider calls, production mutations, or generated-code execution were performed. Unit checks used dummy configuration and deliberately unreachable local database/Redis addresses; fake adapters and sessions handled the tested paths. The initial test invocation failed collection because required configuration was absent; it was rerun with explicit test configuration.

- Prompt builder/base/fragments, stage manager, stage quality parity, and LLM gateway: **265 passed, 3 failed**.
- Resume, recovery service/heartbeat, admission, generation-worker readiness: **58 passed**.
- Total focused completed checks: **323 passed, 3 failed**. This is not the entire test suite.

Failures:

1. `test_generate_missing_sentinel_is_delivered_not_refunded`: expected `clear`, observed `advisory`.
2. `test_generate_mermaid_user_flow_diagram_is_not_refunded`: failed in the same generation suite; retain as an unresolved failure rather than assume it proves a provider regression.
3. `test_background_technology_check_blocks_without_regeneration`: expected the first finding to be runtime/model deprecation; observed `technology_policy_stale`.

Direct offline reproductions confirmed that the section gate accepts all mandatory headings inside a single fence, oversized critical-section injection drops `FR-999`, and the current policy produces `technology_policy_stale` for September 6. Concurrency, cross-tenant cache exposure, and external-timeout findings are source-traced risks awaiting dedicated integration reproductions. No claim is made that live model output quality or production recovery has been benchmarked.
