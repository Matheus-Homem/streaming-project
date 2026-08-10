# GitHub Ingestion Tasks - P2 (Resilience & Deduplication)

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is the source of truth for the full flow.

**Modified by `AD-001` (`.specs/STATE.md`): the user writes all production and test code for every task below. The agent's role during Execute is mentor guidance (explain, point at the approach, review) plus running the gate and creating the commit once the user's code passes - never authoring the implementation itself.**

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

---

**Design**: none - scope skipped Design (no architectural decisions beyond what's noted per-task below)
**Spec**: `.specs/features/streaming-ingestion/spec.md` (P2 stories, ING-09..ING-12)
**Status**: In Progress

> Live progress (which task/step is active, attempts, blockers) now lives in `.specs/checkpoint.yaml` and `.specs/STATE.md`'s `## Handoff` section, not in this file. `tasks.md` stays a static definition of what each task requires; only the `Done when` checkboxes below flip as work is verified and committed.

---

## Test Coverage Matrix

> Generated from codebase sampling (`tests/ingestion/test_client.py`, `test_engine.py`, `test_publisher.py`, `test_use_case.py` - all `unittest.TestCase` + `unittest.mock`) and spec ACs. No `AGENTS.md`/`CONTRIBUTING.md`/testing-standards doc found in the repo, so the strong default applies: domain logic gets 1:1 AC coverage plus every listed edge case.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| `ingestion/app.py` poll loop (orchestration) | unit | Happy path (success resets backoff) + failure path (logs, doesn't crash, backoff grows and is capped) - 1:1 to ING-09 | `tests/ingestion/test_app.py` (new file) | `python3 -m pytest tests/ingestion/test_app.py -q` |
| `ingestion/client.py` rate-limit detection | unit | Rate-limited response raises the dedicated exception; non-rate-limited errors still behave as today - 1:1 to ING-10 | `tests/ingestion/test_client.py` (extend) | `python3 -m pytest tests/ingestion/test_client.py -q` |
| `ingestion/dedup.py` seen-events tracker (new) | unit | All branches: unseen id → not a duplicate + records it; seen id → reported as duplicate; bounded size is enforced - 1:1 to ING-11 | `tests/ingestion/test_dedup.py` (new file) | `python3 -m pytest tests/ingestion/test_dedup.py -q` |
| `ingestion/use_case.py` dedup wiring | unit | Duplicate events are filtered before `producer.publish()` is called; non-duplicates flow through unchanged | `tests/ingestion/test_use_case.py` (extend) | `python3 -m pytest tests/ingestion/test_use_case.py -q` |
| `ingestion/app.py` `--poll-interval` CLI flag | unit | Default (5s) when omitted; explicit value used when passed - 1:1 to ING-12 | `tests/ingestion/test_app.py` (extend) | `python3 -m pytest tests/ingestion/test_app.py -q` |

## Gate Check Commands

> Generated from `Makefile` (`make test` = `pytest -s -vv --log-cli-level=INFO --cov=. --cov-report=term-missing tests` + cleanup; `make neat` = `autoflake` + `isort` + `black` over `shared ingestion tests`).

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After each task below (unit tests only, no integration/e2e layer in this feature) | `python3 -m pytest tests/ingestion -q` |
| Full | Same as Quick for this feature - no integration/e2e layer is introduced | `python3 -m pytest tests/ingestion -q` |
| Build | After the last task in each phase | `make neat && make test` |

---

## Execution Plan

Phases are ordered and run sequentially - each phase completes before the next begins, tasks within a phase execute in order.

### Phase 1 [S1]: Poll-loop resilience (ING-09, ING-10)

```
T1 → T2
```

### Phase 2 [S2]: Deduplication within a run (ING-11)

```
T3 → T4
```

### Phase 3 [S3]: Configurable poll interval (ING-12)

```
T5
```

---

## Task Breakdown

### T1: Harden the poll loop against unhandled exceptions with capped backoff

**What**: `main()`'s `while True` loop catches exceptions from `ingestion_pipeline.execute()`, logs them, and keeps polling instead of crashing the process; backoff grows on repeated failures and resets on success, but is capped at a maximum interval instead of growing unbounded.
**Where**: `ingestion/app.py`
**Depends on**: None
**Reuses**: `shared/logger.py`'s per-module `getLogger` pattern (already used by every other module in `ingestion/`) instead of the bare `logging` module
**Requirement**: ING-09

**Context - you already have a draft here.** Your current uncommitted diff adds the `try/except` + doubling backoff, which is the right shape. Three things to tighten before this is done, from the review:
1. **No ceiling.** `sleep_time * 2` forever means after ~10 consecutive failures you're sleeping over an hour, growing unbounded. Pick and justify a max (e.g. 60s) - what's the trade-off between "recover fast once GitHub is back" and "don't hammer a struggling dependency"?
2. **Logger consistency.** Every other module gets its logger via `getLogger(self.__class__.__name__)` through `shared/logger.py`'s `setup_logging`. Your diff uses the bare `logging.error(...)`, which logs under the root logger - it won't carry the same name-based context the rest of the codebase relies on for filtering. Get a module-level logger instead (`getLogger(__name__)` or similar, consistent with the existing convention).
3. **Lost traceback.** `logging.error(f"Error in execution: {e}")` only captures the exception's string, not the stack trace. Compare with `client.py`/`publisher.py`, which use `self.logger.exception(...)` - that captures the full traceback automatically. Ask yourself: six months from now, staring at a log line with just `Error in execution: 'NoneType' object has no attribute...`, would you be able to find the bug without the traceback?

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [x] Loop survives a simulated `execute()` exception without terminating the process
- [x] Backoff doubles on consecutive failures and resets to the base interval after a success
- [x] Backoff is capped at a maximum value (your choice - document it in a comment or docstring) - `RetryTimer._max_time = 300` (`ingestion/utils/timer.py`)
- [x] Failure logs go through a named logger consistent with the rest of `ingestion/`, and preserve the traceback - minor gap noted: the loop's own `except` still logs via `.info(...)`, traceback survives today only because `client.py`/`publisher.py` call `.exception(...)` before re-raising
- [x] Gate check passes: `python3 -m pytest tests/ingestion/test_app.py -q`
- [x] Test count: at least 2 new tests (success path resets backoff; failure path logs + continues + caps) - `TestMain` in `tests/ingestion/test_app.py`

**Tests**: unit
**Gate**: quick

**Commit**: `fix(ingestion): recover from unhandled exceptions in the poll loop`

---

### T2: Detect GitHub rate-limit responses in the client

**What**: `IngestionClient.get_events()` distinguishes a rate-limited GitHub response (status `403`/`429` with `X-RateLimit-Remaining: 0`, or whatever combination GitHub's docs specify for the events endpoint) from other HTTP errors, and raises a dedicated, source-agnostic exception (e.g. `RateLimitError`, not `GitHubRateLimitError` - this codebase is multi-source by design, see `RawEvent.source`/`SourceType` in `models.py`; a GitLab client will hit the same condition later and should raise the same exception type) instead of the generic `HTTPError`.
**Where**: `ingestion/adapters/client.py` (path updated post-refactor; originally `ingestion/client.py`)
**Depends on**: T1
**Reuses**: existing `try/except` structure in `get_events()`

**Design note (why this shape):** the client's job stays "fetch and signal what happened" - it does not sleep or retry itself. The poll loop from T1 already catches any exception from the pipeline and backs off; a dedicated exception type just lets that same generic backoff path treat a rate limit correctly (no immediate retry) without the client and the loop both trying to own retry policy. The exception name stays source-agnostic (not `GitHubRateLimitError`) for the same reason `RawEvent` carries a `source` field instead of per-source subclasses - a `SourceType`/source attribute on the exception instance differentiates GitHub from GitLab, not the class name.

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] A rate-limited response raises the dedicated exception, not `HTTPError`
- [ ] A non-rate-limited error response still raises `HTTPError` as before (no regression)
- [ ] The exception reads the `X-RateLimit-Reset` header and exposes the recommended wait time as an attribute (e.g. `RateLimitError.reset_at`) - **mandatory**, not an optional extension
- [ ] Gate check passes: `python3 -m pytest tests/ingestion/test_client.py -q`
- [ ] Test count: existing client tests still pass + at least 1 new test for the rate-limit branch

**Tests**: unit
**Gate**: quick

**Commit**: `feat(ingestion): detect GitHub rate-limit responses in the client`

---

### T3: Add a bounded in-memory seen-events tracker

**What**: A small, standalone component that remembers recently-seen `source_event_id`s and reports whether an id has already been seen in this run. Bounded (not an unbounded `set()` that grows for the lifetime of a long-running process) - e.g. a `collections.deque(maxlen=N)` paired with a `set`, or an equivalent bounded structure.
**Where**: `ingestion/dedup.py` (new file)
**Depends on**: None
**Reuses**: none - this is a new, dependency-free component

**Done when**:

- [ ] An id seen for the first time is reported as not-a-duplicate and is remembered
- [ ] The same id seen again is reported as a duplicate
- [ ] The tracker has a bound (document the chosen size and what happens at the boundary - oldest entry evicted)
- [ ] Gate check passes: `python3 -m pytest tests/ingestion/test_dedup.py -q`
- [ ] Test count: at least 3 tests (new id, duplicate id, eviction at the bound)

**Tests**: unit
**Gate**: quick

**Commit**: `feat(ingestion): add bounded seen-events tracker for dedup`

---

### T4: Wire the seen-events tracker into the ingestion pipeline

**What**: `IngestionPipeline.execute()` filters out events whose `source_event_id` the tracker from T3 reports as already-seen, before calling `producer.publish()`. Non-duplicate events flow through unchanged.
**Where**: `ingestion/use_case.py`
**Depends on**: T3
**Reuses**: `ingestion/dedup.py` tracker from T3

**Think about this before coding it:** where in the pipeline should the duplicate check happen - before or after `engine.process()`? `engine.process()` turns raw dicts into `RawEvent`s (with `source_event_id` on them); filtering before that means comparing raw dict ids, filtering after means comparing `RawEvent.source_event_id`. Which keeps `IngestionPipeline.execute()`'s existing three-stage shape (`get_events → process → publish`) cleanest, and why?

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] A duplicate event (same `source_event_id` as one already processed this run) is not passed to `producer.publish()`
- [ ] A non-duplicate event still reaches `producer.publish()` exactly as before
- [ ] Existing `IngestionPipeline` tests (order of `client → engine → producer` calls) still pass unmodified in spirit - update them only if the dedup step genuinely changes the call sequence
- [ ] Gate check passes: `python3 -m pytest tests/ingestion/test_use_case.py -q`
- [ ] Test count: existing use_case tests still pass + at least 2 new tests (duplicate filtered, non-duplicate passes through)

**Tests**: unit
**Gate**: quick

**Commit**: `feat(ingestion): skip already-seen events before publish`

---

### T5: Add a configurable `--poll-interval` CLI flag

**What**: `build_arguments()` gains an optional `--poll-interval` flag (seconds, float or int - your call); `main()` uses it as the base interval for the poll loop instead of the hardcoded `5`. Omitting the flag keeps today's behavior (5s default).
**Where**: `ingestion/app.py`
**Depends on**: None (Phase 3 runs after Phase 1 completes, so T1 is already merged by the time this starts - no explicit code dependency beyond that ordering)
**Reuses**: `argparse` patterns already in `build_arguments()`

**Done when**:

- [x] Running without `--poll-interval` behaves exactly as today (5s base interval)
- [x] Running with `--poll-interval N` uses `N` as the base interval instead of `5`
- [x] The backoff cap and doubling logic from T1 still work relative to the configured base, not a hardcoded `5` - `RetryTimer` stores `self._default_time` in the constructor; `reset()` restores that instead of a hardcoded `5`
- [x] Gate check passes: `python3 -m pytest tests/ingestion/test_app.py -q`
- [x] Test count: at least 2 new tests (default value, explicit override) - `TestBuildArguments` in `tests/ingestion/test_app.py` + `tests/ingestion/utils/test_timer.py::test_timer_reset`

**Tests**: unit
**Gate**: quick

**Commit**: `feat(ingestion): make poll interval configurable via --poll-interval`

---

## Phase Execution Map

```
Phase 1 → Phase 2 → Phase 3

Phase 1:  T1 ------→ T2
Phase 2:  T3 ------→ T4
Phase 3:  T5
```

Execution is strictly sequential - one task at a time, in order. 5 tasks total fits a single batch (≤ ~8 tasks) - no sub-agent delegation needed, everything runs inline.

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: Harden poll loop (exceptions + capped backoff) | 1 file, 1 loop | ✅ Granular |
| T2: Detect rate-limit responses in client | 1 file, 1 method | ✅ Granular |
| T3: Bounded seen-events tracker | 1 new file, 1 component | ✅ Granular |
| T4: Wire tracker into pipeline | 1 file, 1 method | ✅ Granular |
| T5: Configurable poll interval | 1 file, 2 cohesive changes (arg + loop use) | ✅ Granular (2-3 related things in same file, cohesive) |

---

## Diagram-Definition Cross-Check

| Task | Depends On (task body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | (start of Phase 1) | ✅ Match |
| T2 | T1 | T1 → T2 | ✅ Match |
| T3 | None | (start of Phase 2) | ✅ Match |
| T4 | T3 | T3 → T4 | ✅ Match |
| T5 | None | (start of Phase 3, no arrow) | ✅ Match |

---

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1: Harden poll loop | `ingestion/app.py` poll loop | unit | unit | ✅ OK |
| T2: Rate-limit detection | `ingestion/client.py` | unit | unit | ✅ OK |
| T3: Seen-events tracker | `ingestion/dedup.py` (new) | unit | unit | ✅ OK |
| T4: Wire tracker into pipeline | `ingestion/use_case.py` | unit | unit | ✅ OK |
| T5: Configurable poll interval | `ingestion/app.py` | unit | unit | ✅ OK |

All ✅ - no restructuring needed before presenting.
