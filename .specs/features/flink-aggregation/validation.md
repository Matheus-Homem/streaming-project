# Flink Aggregation Validation

## Validation: Flink Aggregation - PASS ✅

**Date**: 2026-08-25
**Spec**: `.specs/features/flink-aggregation/spec.md`
**Diff range**: `c1b2a70..HEAD` on `feat/flink_aggregation`, plus uncommitted working-tree changes (see Diff Range Detail below)
**Verifier**: independent sub-agent (author ≠ verifier)

---

## Diff Range Detail

`git log --oneline c1b2a70..HEAD` shows 18 commits. Of these, the feature's own surface is:

- `977c43f`, `9ee2b2c`, `cc5d50b`, `0f4aa56`, `46ede2d`, `3e2d66a` - T1-T5 (topic, SQL, app.py, Dockerfile, compose)
- `d51e243`, `f16db1f`, `dac3cf0` - map.md, design.md, tasks.md

Also present in the same commit range but **not** part of this feature's own task list (a related, separately-made fix to Kafka sink key/value serialization, touching `flink/common` and `flink/normalization`):

- `7193caa` (Refactor Kafka sink serialization to use `RowFieldExtractorSchema`), `3fc95b8`, `2dcd779`, `98a343b`

And unrelated to this feature entirely (ingestion auth/config work): `ea0389c`, `cc243b1`, `21cb912`, `d8ff93b`.

**Verified**: `flink/aggregation/app.py` imports only `os`, `logging`, `pathlib`, `dotenv`, `pyflink.datastream`, `pyflink.table`, `shared.logger` - it does **not** import `flink/common` at all. The sink/serialization fix (`RowFieldExtractorSchema`) is therefore unrelated to this feature's runtime surface; it's included below only because the task instructions asked the sensor to consider it as touched, in-scope Python code.

**Uncommitted working-tree changes** (per `git status --porcelain` at verification time), reviewed as real, already-live-tested changes:

- `flink/aggregation/queries/repo_counts_5m.sql` - modified since the last commit (table/column names changed mid-task per T2's gate note: `normalized_event_envelope`/`aggregated_event` instead of `events_normalized`/`events_aggregated`)
- `infra/docker/docker-compose.yml` - modified (aggregation/taskmanager-aggregation service tuning)
- `infra/docker/scripts/create-topics.sh` - modified (see T1 discrepancy note below)
- `.specs/features/flink-aggregation/tasks.md` - task status updates (all T1-T6 checked)
- `.mentor/nodes.md`, `.mentor/profile.md` - mentor bookkeeping, out of this review's scope
- `.mentor/features/flink-aggregation/classes/` - untracked, mentor class material, out of scope

All code/config files above were read in their current (working-tree) state for this review, not just at their last commit.

---

## Task Completion

| Task | Status  | Notes |
| ---- | ------- | ----- |
| T1   | ✅ Done | `events-aggregated` added to `TOPICS` array. One discrepancy: the task's "Done when" text says `--replication-factor 3`, but the actual script applies `--replication-factor 1` to all three topics in the shared loop (`infra/docker/scripts/create-topics.sh:24`) - this is a pre-existing value shared uniformly across all topics, not something this feature changed, and it does satisfy "matching the other two entries exactly." Doc-vs-code drift only, not a functional gap. |
| T2   | ✅ Done | `repo_counts_5m.sql` present, all 3 statements as specced. |
| T3   | ✅ Done | `app.py` present, generic runner, no hardcoded query file. |
| T4   | ✅ Done | `Dockerfile` matches `flink/normalization/Dockerfile`'s structure; does not copy `flink/common/`. |
| T5   | ✅ Done | `aggregation`/`taskmanager-aggregation` wired in `docker-compose.yml`. |
| T6   | ✅ Done (weaker evidence tier) | All 6 sub-items checked. 5 of 6 are user-attested in chat, not independently observed by any agent. 1 (FLA-09, keyed output) was directly observed by the orchestrating agent via a Kafka UI screenshot. See evidence-tier note below. |

---

## Spec-Anchored Acceptance Criteria (P1: Repo activity counts per window)

| # | Criterion (WHEN X THEN Y) | Spec-defined outcome | `file:line` + assertion / evidence | Result |
| - | -------------------------- | --------------------- | ----------------------------------- | ------ |
| AC1 | Source table reads `events-normalized`, keyed by `repo_name`, `event_time` as event-time attribute | `CREATE TABLE` with `partition_key`, `event_time`, watermark on a derived timestamp | `flink/aggregation/queries/repo_counts_5m.sql:1-8` - `CREATE TABLE normalized_event_envelope (partition_key STRING, event_time BIGINT, event_time_timestamp AS TO_TIMESTAMP_LTZ(event_time, 3), WATERMARK FOR event_time_timestamp AS event_time_timestamp - INTERVAL '30' SECOND)`, `'topic' = 'events-normalized'` at line 8 | ✅ PASS |
| AC2 | Bounded-out-of-orderness watermark of 30s on `event_time` | Exactly `INTERVAL '30' SECOND` | `repo_counts_5m.sql:5` - `WATERMARK FOR event_time_timestamp AS event_time_timestamp - INTERVAL '30' SECOND` | ✅ PASS |
| AC3 | 5-min tumbling window close publishes one row with `repo_name`, `window_start`, `window_end`, `event_count` | `TABLE(TUMBLE(...))`, `INTERVAL '5' MINUTE`, `COUNT(*)`, all 4 output columns | `repo_counts_5m.sql:17-21` (sink columns), `:34-48` (`TUMBLE(..., INTERVAL '5' MINUTE)`, `COUNT(*) AS event_count`); live count verification `tasks.md:174` (user-attested, not independently observed) | ✅ PASS (structural) / evidence-tier caveat on the live/runtime part - see below |
| AC4 | No zero-count row for a repo with no events in a window | `GROUP BY partition_key, window_start, window_end` naturally emits no row for an empty group (standard `TUMBLE`+`GROUP BY` semantics - no explicit `LEFT JOIN`/`FULL OUTER` construct that would synthesize zero rows) | `repo_counts_5m.sql:48` - `GROUP BY partition_key, window_start, window_end`; no evidence of a row being emitted per repo-window pair that had zero events - correct by construction, not independently observed live | ✅ PASS (structural, by SQL semantics) |
| AC5 | Late event excluded from any window's count, no job failure | Standard Flink watermark/window drop behavior, no side-output configured | `repo_counts_5m.sql:5` (watermark), no side-output clause anywhere in the file (absence confirmed by reading the whole file); live confirmation `tasks.md:175` (user-attested) | ✅ PASS |
| AC6 | Malformed row skipped, logged, job continues | `json.ignore-parse-errors` type behavior | `repo_counts_5m.sql:13` - `'json.ignore-parse-errors' = 'true'`; whether it *logs* is explicitly flagged unconfirmed-from-docs-alone in `tasks.md:92`, deferred to T6 live logs; live confirmation `tasks.md:176` (user-attested, "skipped, logged, job keeps running") | ✅ PASS - weaker evidence tier (see caveat) |
| AC7 | Checkpointing at regular interval, restart resumes in-flight window count | `env.enable_checkpointing(...)` | `flink/aggregation/app.py:27` - `env.enable_checkpointing(60000)`; live restart-resume confirmation `tasks.md:177` (user-attested) | ✅ PASS |
| AC8 | Exactly-once delivery, no duplicate row after restart | `sink.delivery-guarantee = exactly-once` + transactional id prefix | `repo_counts_5m.sql:29-31` - `'sink.delivery-guarantee' = 'exactly-once'`, `'sink.transactional-id-prefix' = 'flink-query-aggregated-events-sink'`, `'properties.transaction.timeout.ms' = '600000'`; live no-duplicate confirmation `tasks.md:178` (user-attested) | ✅ PASS |
| AC9 | `events-aggregated` records keyed by `repo_name` (physical Kafka key, not just JSON field) | `key.fields`/`key.format` set | `repo_counts_5m.sql:26-27` - `'key.fields' = 'repo_name'`, `'key.format' = 'json'`; live confirmation `tasks.md:179` - **directly observed by the orchestrating agent** (Kafka UI screenshot, Key column = `{"repo_name":"..."}`) | ✅ PASS - strongest evidence tier of the T6 items |

**Status**: ✅ All 9 ACs covered structurally in code; the runtime/behavioral half of AC3-AC8 rests on T6's live verification, which is real but carries a weaker evidence tier for 5 of its 6 items (user-attested, not agent-observed) - see Evidence-Tier Note below. No spec-precision gaps: every AC in `spec.md` states a precise outcome, and each one is matched by an exact SQL option/value.

### Evidence-Tier Note (T6)

Per the task brief, this is called out explicitly rather than treated as equal to an automated assertion:

- **FLA-09 (keyed output)**: agent-observed - a Kafka UI screenshot showing the physical Key column was directly seen in the conversation. This is the strongest evidence tier available in this feature (short of an automated integration test).
- **FLA-03/04/05 (correct counts, late-event exclusion), FLA-06 (malformed skip+log), FLA-07 (checkpoint restart resume), FLA-08 (exactly-once no-duplicate)**: user-attested only - the user reported passing results in chat on 2026-08-25; no screenshot, log excerpt, or independently-run command was captured by any agent for these 5 items. This is real evidence (a human ran the procedure and reported the outcome) but it is not independently verifiable from this session and is weaker than either an automated test assertion or an agent-observed artifact. It is not treated as a gap - the Test Coverage Matrix accepted this test strategy up front - but it should not be read as equivalent-strength evidence to a checked automated test.

---

## Edge Cases

- [x] Empty `events-normalized` at job start → starts cleanly, no error: consistent with the SQL having no code path that would fail on zero rows (structural); not separately live-verified in T6's checklist (T6's messages were non-empty) - **not directly evidenced**, flagged as a minor residual gap, not blocking (low risk given the connector's default streaming-source behavior).
- [x] Two events, same `repo_name`, same `event_time` → both counted: `COUNT(*)` has no dedup-by-timestamp semantics, correct by construction (`repo_counts_5m.sql:39`). Not separately live-verified but not the kind of edge case likely to silently break given the `COUNT(*)` semantics.
- [x] Restart mid-window resumes count, not zero: `tasks.md:177` (user-attested).
- [x] Sink's `transaction.timeout.ms` vs. broker's `transaction.max.timeout.ms`: `repo_counts_5m.sql:31` sets `600000` (10 min) explicitly; `tasks.md:93` records this as confirmed under the broker's unmodified default (15 min) - reviewed as a config-comparison claim, not something this verifier could re-derive without inspecting live broker startup, consistent with the T2 gate note.

---

## Discrimination Sensor

Run in an isolated `git worktree` scratch (`git worktree add <path> HEAD`), never in the real tree. Baseline `git status --porcelain` captured before, and re-checked identical after, each scratch run.

| # | File:line | Description | Tests run | Killed? |
| - | --------- | ------------ | --------- | ------- |
| 1 | `flink/common/adapters/sink.py:25-26` | Swapped which literal index (`0`/`1`) is passed to the key vs. value `RowFieldExtractorSchema(...)` call (key got `1`, value got `0`) | `tests/flink/common/adapters/test_sink.py` | ❌ **Survived** - `3 passed`. The test mocks `RowFieldExtractorSchema` entirely and asserts by *call order* (`side_effect=[key_schema, value_schema]`), not by which literal argument reaches which setter - it never checks that `0` specifically maps to the key and `1` to the value. |
| 2 | `flink/normalization/adapters/function.py:42` | Flipped `yield Row(key, value)` → `yield Row(value, key)` | `tests/flink/normalization/adapters/test_function.py` | ✅ Killed - `1 failed, 5 passed` (`Row(b'{"source": "widget"}', b'my-org/my-repo')` ≠ expected) |
| 3 | `flink/aggregation/app.py:16` | `split_statements`: removed the truthiness filter (`if s.strip()` dropped from the list comprehension), so a trailing/empty statement is no longer discarded | `tests/flink/aggregation/` (does not exist) | N/A - **no test exists to kill or survive this mutant**. `tests/flink/aggregation/` is not present; running `tests/flink` (92 tests) showed no change. This is not a gap: the Test Coverage Matrix explicitly made `split_statements` tests conditional ("only if non-trivial") and the implementer judged the straightforward `;`-split as not meeting that bar - a documented decision, not an oversight. |

**Sensor depth**: lightweight (3 mutations attempted, per the default tier)
**Result**: 1/2 testable mutations killed; mutation #3 correctly has no test surface by design.

**Finding from mutation #1**: `tests/flink/common/adapters/test_sink.py::test_can_wire_the_key_and_value_serializers_to_extract_by_row_index` does not actually pin *which* row index feeds the key vs. the value - it only confirms both `0` and `1` were called somewhere and that call order maps schema objects into the two setters. A swap of the literal arguments passed to each call escapes detection. This is real code in scope of the sensor's mandate but **belongs to the separate, already-decided sink-serialization fix** (`7193caa` et al.), not to this feature's own T1-T6 task list - flagged here per the task's instruction to run the sensor on whatever Python is in scope, but not treated as a `flink-aggregation` gap for the PASS/FAIL verdict below. Worth a follow-up fix task on that other work if/when it's revisited.

Isolation verified: `git status --porcelain` matched the pre-sensor baseline after each of the 3 scratch runs (worktrees removed with `--force`, no working-tree file touched).

---

## Code Quality

| Principle | Status |
| --------- | ------ |
| No features beyond what was asked | ✅ - `app.py` is a generic runner, no extra query-selection framework built (matches the explicit Out-of-Scope item) |
| No abstractions for single-use code | ✅ |
| No unnecessary "flexibility" added | ✅ |
| Only touched files required for task | ✅ for this feature's own T1-T6; the sink/serialization commits in the same range are a separately-scoped fix, not scope creep by this feature (confirmed `app.py` doesn't import `flink/common`) |
| Didn't "improve" unrelated code | ✅ |
| Matches existing patterns/style | ✅ - Dockerfile/compose mirror `flink/normalization`'s shape; `app.py` mirrors its dotenv/logger pattern |
| Would senior engineer approve? | ✅, with the caveat that mutation #1's weak assertion (in the unrelated but co-touched sink test) would likely draw a review comment if surfaced |
| Tests map to acceptance criteria, non-shallow | N/A for this feature's own SQL/checkpointing behavior - correctly zero automated tests, per the accepted live-only test strategy |
| Spec-anchored outcome check | ✅ - every AC's SQL option/value matches the spec's precise outcome exactly (see AC table) |
| Per-layer Coverage Expectation met | ✅ - `app.py`'s only non-trivial-if-tested logic (`split_statements`) has a documented, deliberate no-test decision consistent with the matrix's conditional rule |
| Every test in scope maps to a spec AC / edge case / Done-when | ✅ - no unclaimed tests found in `tests/flink/aggregation` (doesn't exist) or in the co-touched `tests/flink/common`/`tests/flink/normalization` files (all map to the sink-serialization fix's own behavior) |
| Documented project quality/testing guidelines followed | `Makefile`'s `make test` (`pytest --cov=ingestion --cov=flink --cov-report=term-missing tests`), `.claude/rules/python-conventions.md` (unittest.TestCase style, one logger per class) - both followed |

---

## Gate Check

- **Gate command**: `make test` (Quick gate per `tasks.md`'s Gate Check Commands, applicable since T6 is a live/manual gate, not part of the automated suite)
- **Result**: 189 passed, 11 subtests passed, 6 failed
- **Failures**: all 6 in `tests/ingestion/test_app.py::TestMain::*` - `KeyError: 'KAFKA_BOOTSTRAP_SERVERS'`. Confirmed **pre-existing**: reran the exact same suite from a scratch worktree checked out at `c1b2a70` (the commit immediately before this feature's work started) - same 6 failures, same error, present there too. Unrelated to `flink-aggregation`'s surface (`tests/ingestion/`, not `flink/` or `infra/`). Excluded from this feature's gate verdict.
- **Test count before feature** (`c1b2a70`, full suite): 182 passed, 11 subtests, 6 failed (same pre-existing failures)
- **Test count after feature** (`HEAD` + working tree): 189 passed, 11 subtests, 6 failed
- **Delta**: +7 passing tests. All 7 belong to the co-touched sink-serialization fix (`tests/flink/common/adapters/test_sink.py`, `tests/flink/normalization/adapters/test_function.py`, `tests/flink/normalization/test_app.py`), not to `flink-aggregation` itself - consistent with the Test Coverage Matrix's decision that this feature adds zero automated tests of its own.
- **Skipped tests**: none found in scope.
- **Feature-scoped gate verdict**: ✅ PASS (0 failures attributable to `flink-aggregation`'s own surface; the 6 failing tests are pre-existing and unrelated)

---

## Requirement Traceability (recommended update - not applied to `spec.md`, per instructions)

| Requirement | Previous Status (spec.md) | Recommended New Status |
| ----------- | -------------------------- | ------------------------ |
| FLA-01 | Pending | ✅ Verified |
| FLA-02 | Pending | ✅ Verified |
| FLA-03 | Pending | ✅ Verified |
| FLA-04 | Pending | ✅ Verified |
| FLA-05 | Pending | ✅ Verified |
| FLA-06 | Pending | ✅ Verified (weaker evidence tier - user-attested log confirmation) |
| FLA-07 | Pending | ✅ Verified (weaker evidence tier - user-attested) |
| FLA-08 | Pending | ✅ Verified (weaker evidence tier - user-attested) |
| FLA-09 | Pending | ✅ Verified (strongest evidence tier - agent-observed) |

---

## Summary

**Overall**: ✅ Ready

**Spec-anchored check**: 9/9 ACs matched spec outcome structurally in code; 0 spec-precision gaps. Runtime behavior for AC3, AC5-AC8 additionally rests on T6's live verification, carrying a documented weaker evidence tier for 5 of 6 items (user-attested, not independently observed) - not a gap under this feature's accepted test strategy, but noted for honesty.

**Gate**: 189 passed, 6 failed (all pre-existing/unrelated, confirmed via baseline rerun), 11 subtests passed. Feature-scoped verdict: 0 failures.

**Sensor**: 3 mutations attempted, 1 killed, 1 survived (in the unrelated co-touched sink test, not this feature's own task list), 1 N/A by design (no test surface exists for `split_statements`, per a documented, matrix-sanctioned decision).

**What works**: Topic provisioning, source/sink DDL, watermark, windowed aggregation query, checkpointing config, exactly-once sink config, and keyed output are all present and structurally correct against every FLA requirement. Docker/compose wiring mirrors the existing `flink/normalization` deploy pattern with no drift. `flink/aggregation/app.py` has no coupling to the unrelated sink-serialization fix landed in the same commit range.

**Issues found**:
1. (Not a `flink-aggregation` gap, informational) `tests/flink/common/adapters/test_sink.py`'s key/value-serializer-wiring test doesn't pin which row index feeds the key vs. the value - a mutant swapping the two literal arguments survives. Belongs to the separately-scoped sink-serialization fix; worth a follow-up fix task there if that work is revisited.
2. (Minor, informational) The "empty topic at job start" edge case from `spec.md` was not explicitly exercised in T6's live procedure (T6's messages were all non-empty). Structurally sound (no code path assumes non-empty input) but not independently evidenced.
3. (Doc-only) T1's "Done when" text says `--replication-factor 3`; the actual script applies `--replication-factor 1` uniformly to all three topics (pre-existing behavior, unchanged by this feature). Cosmetic task-doc drift, not a functional issue.

**Next steps**: None required to close this feature - all 9 FLA requirements have code-level evidence and T6's live procedure covered all Success Criteria. If the team wants a stronger evidence bar in the future for live-only features, consider a lightweight capture step (log excerpt or screenshot) for each T6 sub-item beyond FLA-09.
