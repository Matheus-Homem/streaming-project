# Flink Aggregation Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is the source of truth for the full flow (per-task cycle, sub-agent delegation, adequacy review, Verifier, discrimination sensor).

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

**Authorship levels** (`AD-008`, `.claude/rules/interaction-protocol.md`): every task below carries a verdict from `.mentor/features/flink-aggregation/map.md` - `own`, `paired`, or `delegated` (the v3 mentor skill's term for what `AD-008`/`interaction-protocol.md` still call `deliver`; same meaning). Read `map.md`'s Trace section before starting a task, not this file alone - `map.md` is the authoritative record, this file only mirrors the verdict for visibility. **3 own (T2, T3, T6), 3 delegated (T1, T4, T5), 0 paired standalone.**

**No agent commits** (`AD-003`): the agent stages, runs gates, and proposes a commit message per task - the user commits.

---

**Design**: `.specs/features/flink-aggregation/design.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase (no `AGENTS.md`/contributing guide found; `Makefile`'s `make test` target is the only documented test convention: `pytest --cov=ingestion --cov=flink --cov-report=term-missing tests`, no enforced coverage threshold). Confirmed with the user during Tasks: this feature's real behavior lives in SQL, not Python, and is verified live (T6), not by an automated PyFlink test harness - see `design.md`'s Tech Decisions.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| `flink/aggregation/app.py` (SQL statement-split + execute glue) | unit, only if the statement-splitting logic is non-trivial (e.g. must ignore `;` inside string literals) | If tests are written: the split function's edge cases (empty file, trailing semicolon, statement containing a string literal with `;`) | `tests/flink/aggregation/test_app.py` | `pytest tests/flink/aggregation` |
| `flink/aggregation/queries/repo_counts_5m.sql` (the SQL query itself - windowing, watermark, exactly-once) | none - live-verified only | N/A. Windowing/watermark/late-drop/checkpoint-restore/exactly-once behavior is verified live via `docker compose` (T6), matching `flink-normalization`'s T19 precedent for the same class of behavior | n/a | T6's manual procedure (docker compose up, publish sample events, inspect `events-aggregated` in Kafka UI) |
| `flink/aggregation/Dockerfile`, `infra/docker/docker-compose.yml`, `infra/docker/scripts/create-topics.sh` | none | Build/config gate only | n/a | `docker compose -f infra/docker/docker-compose.yml config` (syntax) + T6 (live) |

## Gate Check Commands

> Generated from codebase - confirm before Execute.

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After T1-T5 (code/config-only tasks, no PyFlink runtime involved) | `make test` (repo-wide suite - catches import/collection errors even with no new tests added; `--cov=flink` will show `flink/aggregation/` at low/zero coverage, which is expected and accepted per the Test Coverage Matrix above) |
| Full | After T6 | The T6 procedure itself: `docker compose -f infra/docker/docker-compose.yml up`, publish the hand-crafted `events-normalized` messages from `spec.md`'s Independent Test, inspect `events-aggregated` in Kafka UI, restart the `aggregation` service mid-stream and confirm no duplicate |
| Build | Phase completion | `docker compose -f infra/docker/docker-compose.yml build aggregation taskmanager-aggregation` |

---

## Execution Plan

Single phase - 6 tasks, well under the ~7-task sub-agent budget; no batching offered.

### Phase 1: Aggregation job, end to end

```
T1 -> T5
T4 -> T5
T2 -> T6
T3 -> T6
T5 -> T6
```

T1, T2, T3, T4 have no dependency on each other - each is a standalone file. T5 (compose wiring) genuinely needs T1's topic and T4's image to reference. T6 (live verification) needs everything: T1's topic, T2's query and T3's app running inside T4's image, wired by T5. Executed in the order T1 → T2 → T3 → T4 → T5 → T6 (a single worker, one task at a time - the order among the independent T1-T4 is arbitrary, chosen here to match the file list in `design.md`).

---

## Task Breakdown

### T1: Provision `events-aggregated` topic

**What**: Add `"events-aggregated"` to the `TOPICS` array in `infra/docker/scripts/create-topics.sh`, same partitions/replication-factor/retention as the other two topics.
**Where**: `infra/docker/scripts/create-topics.sh`
**Depends on**: None
**Reuses**: The existing `TOPICS` array pattern (already holds `events-raw`, `events-normalized`)
**Requirement**: FLA-01

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `events-aggregated` is in the `TOPICS` array with `--partitions 3 --replication-factor 3 --config retention.ms=604800000`, matching the other two entries exactly

**Tests**: none (config-only, per Test Coverage Matrix)
**Gate**: quick - `make test`: 188 passed, 11 subtests, no regression (2026-08-25)

---

### T2: Write `repo_counts_5m.sql`

**What**: The P1 contract (`AD-009`) - one file, three statements: source `CREATE TABLE` for `events-normalized` (columns `partition_key STRING`, `event_time BIGINT`, a computed `TIMESTAMP_LTZ` column, and a `WATERMARK FOR` clause with a 30-second bound), sink `CREATE TABLE` for `events-aggregated` (columns `repo_name`, `window_start`, `window_end`, `event_count`, with exactly-once sink options), and the windowed `INSERT INTO ... SELECT` using `TABLE(TUMBLE(...))` grouped by `repo_name`/`window_start`/`window_end`.
**Where**: `flink/aggregation/queries/repo_counts_5m.sql`
**Depends on**: None (T1 must exist before this is *run*, not before it is *written*)
**Reuses**: `flink/normalization/models.py`'s `NormalizedEvent` as the reference for the source table's 2 columns (consulted by hand, not read programmatically - see `design.md`'s Code Reuse Analysis)
**Requirement**: FLA-02, FLA-03, FLA-04, FLA-05, FLA-06, FLA-08, FLA-09

**Tools**: MCP: NONE (Context7 if available, to verify exact Flink 2.3 SQL connector option names - `json.ignore-parse-errors`, `sink.delivery-guarantee`, `sink.transactional-id-prefix` - per the Knowledge Verification Chain; web search as fallback) / Skill: NONE

**Done when**:
- [x] Source table declares `partition_key`, `event_time`, a `TIMESTAMP_LTZ` computed column, and `WATERMARK FOR ... - INTERVAL '30' SECOND`
- [x] Source table's format option skips a malformed row instead of failing the job (FLA-06) - `json.ignore-parse-errors = 'true'`, confirmed against the official Flink Kafka connector docs; whether it also *logs* the skip (not just silently drops it) is not confirmed from docs alone - verify by reading job logs in T6, not re-opened here
- [x] Sink table declares `repo_name`, `window_start`, `window_end`, `event_count`, keyed by `repo_name` (FLA-09), with exactly-once delivery options set (FLA-08 - `sink.delivery-guarantee`/`sink.transactional-id-prefix`) and an explicit `properties.transaction.timeout.ms = '600000'` (10 min), confirmed under the broker's default `transaction.max.timeout.ms` (15 min, unmodified in `docker-compose.yml`) - see `design.md`'s Risks, now resolved
- [x] `INSERT INTO ... SELECT` uses `TABLE(TUMBLE(TABLE normalized_event_envelope, DESCRIPTOR(event_time_timestamp), INTERVAL '5' MINUTE))`, `GROUP BY partition_key, window_start, window_end`, `COUNT(*) AS event_count` (FLA-02, FLA-03, FLA-04, FLA-05) - positional syntax confirmed against official docs (the named-argument form first attempted, `DATA => / TIMECOL => / SIZE =>`, was not found documented anywhere and was dropped)

**Tests**: none (live-verified in T6, per Test Coverage Matrix)
**Gate**: quick - no PyFlink runtime check possible in isolation; SQL correctness reviewed statement-by-statement against `spec.md`'s ACs and the official Flink Kafka/Windowing-TVF docs across several review rounds (2026-08-25) - real runtime verification is T6. Table/column names ended up as `normalized_event_envelope` (not `events_normalized` as design.md's Done-When draft said) and `aggregated_event` (not `events_aggregated`) - deliberate naming decision made mid-task (the source table is a narrow envelope-only projection, not a full mirror of the topic; see chat), doesn't change the topic names it maps to.

---

### T3: Write `app.py`

**What**: Generic Flink SQL script runner - bootstraps a `StreamExecutionEnvironment`/`StreamTableEnvironment`, enables checkpointing (interval + state backend), reads `os.environ["AGGREGATION_QUERY_FILE"]` relative to `flink/aggregation/queries/`, splits the file into statements, executes each via `execute_sql()`. Never names a specific query file in code.
**Where**: `flink/aggregation/app.py`
**Depends on**: None to write (imports nothing from T2); needs T2's file present to run meaningfully
**Reuses**: `shared/logger.py`'s `setup_logging`, the `.env`/dotenv-load shape of `flink/normalization/app.py`
**Requirement**: FLA-07

**Tools**: MCP: Context7 if available (verify the exact PyFlink `TableEnvironment` API for running multiple statements from one file - per Knowledge Verification Chain, flagged uncertain in `design.md`'s Risks) / Skill: NONE

**Done when**:
- [x] Checkpointing enabled with a 60-second interval (`env.enable_checkpointing(60000)`, per `spec.md`'s Assumptions); default state backend - no code needed, confirmed intentional per `design.md`'s Tech Decisions, not an omission
- [x] Query file path resolved from `AGGREGATION_QUERY_FILE`, no hardcoded filename anywhere in the file
- [x] Statements split (`;`-delimited, stripped, empty pieces filtered) and executed in source-file order via `execute_sql()`; the last statement's `TableResult.wait()` (no timeout - confirmed via docs that a timeout raises `TimeoutException` once it elapses, which a never-finishing streaming job would always hit) keeps the process alive, replacing the DataStream-only `env.execute()` pattern that doesn't apply to a Table API job

**Tests**: none - the statement-splitting logic stayed a straightforward `;`-split + strip + filter, matching the Test Coverage Matrix's skip condition
**Gate**: quick - `make test`: 188 passed, 11 subtests, no regression (2026-08-25); `flink/aggregation/` correctly shows at low coverage (no PyFlink runtime test possible in isolation, per the matrix) - real verification is T6. Several review rounds caught real bugs before landing: `env.execute()` (DataStream API, doesn't apply here) instead of `result.wait()`; `.wait(30000)` then `.wait(600000000)` (a streaming job never "finishes", so any timeout eventually raises `TimeoutException` - needs no argument); a `split_statements` regression to per-line splitting that would have fed single lines to `execute_sql()`; the `AGGREGATION_QUERY_FILE` env var name drifting to `query_name` for a few rounds; a leftover `print(stmt)` debug line; a `"Executinf"` typo, missed twice before being fixed.

---

### T4: Write `Dockerfile`

**What**: Copy `flink/normalization/Dockerfile`'s structure (base image, Kafka SQL connector JAR download, `usrlib` layout) into `flink/aggregation/Dockerfile`, copying `flink/aggregation/` and `shared/` only - not `flink/common/`.
**Where**: `flink/aggregation/Dockerfile`
**Depends on**: None
**Reuses**: `flink/normalization/Dockerfile`
**Requirement**: infra for FLA-01..09 (no single FLA maps to the container image itself)

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Same base image and JAR download step as `flink/normalization/Dockerfile`
- [x] `COPY` includes `flink/aggregation/` and `shared/`; does not include `flink/common/`
- [x] `CMD` runs `standalone-job -py /opt/flink/usrlib/flink/aggregation/app.py`

**Tests**: none (build gate only)
**Gate**: build - `docker` unavailable in this session's environment (no WSL integration; same gap `STATE.md` already flagged for a prior `flink-normalization` session) - real `docker build` deferred to T6, which is where it gets exercised for real anyway. File reviewed by hand against `flink/normalization/Dockerfile` line by line, no drift found.

---

### T5: Wire `docker-compose.yml`

**What**: Add `aggregation` (jobmanager, `standalone-job -py flink/aggregation/app.py`, env `AGGREGATION_QUERY_FILE=repo_counts_5m.sql` - no `KAFKA_BOOTSTRAP_SERVERS`, since T2's `.sql` hardcodes the broker addresses directly, a decision made during T2's review - `depends_on: topic-init: condition: service_completed_successfully`) and `taskmanager-aggregation` (`depends_on: aggregation`), mirroring the existing `normalization`/`taskmanager` service pair.
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T1 (topic must exist for the job to attach to), T4 (image must build)
**Reuses**: The `normalization`/`taskmanager` service definitions as the template
**Requirement**: infra for FLA-01..09

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `aggregation` service builds from `flink/aggregation/Dockerfile`, sets `AGGREGATION_QUERY_FILE=repo_counts_5m.sql`, depends on `topic-init` completing successfully
- [x] `taskmanager-aggregation` service depends on `aggregation`
- [x] `docker compose -f infra/docker/docker-compose.yml config` parses with no error - `docker` unavailable in this session (same gap as T4); validated instead with `python3 -c "import yaml; yaml.safe_load(...)"` (well-formed YAML) and `grep`/`wc -l` against the raw file (`aggregation`/`taskmanager-aggregation` keys appear exactly once each, 200 lines total) after a VS Code `compose-language-service` false-positive DUPLICATE_KEY diagnostic turned out to be a stale editor buffer, not a real duplication on disk. Real `docker compose config` deferred to T6.

Two decisions made while writing this task, not previously specced: Web UI port `8082:8081` (8081 is already taken by `normalization`); taskmanager image tagged `apache/flink:2.3-aggregation` rather than reusing `apache/flink:2.3` (reusing it would have overwritten the normalization taskmanager's tagged image on build, since `image:` + `build:` together makes compose tag the built result under that name).

**Tests**: none (config gate only)
**Gate**: quick

---

### T6: Live verification

**What**: `docker compose up`, publish the hand-crafted `events-normalized` messages from `spec.md`'s Independent Test (two windows, two repos, one late event, one malformed message), inspect `events-aggregated` in Kafka UI for correct counts/windows, confirm the late event is absent from any count and the malformed message did not kill the job, then restart the `aggregation` service mid-stream and confirm no duplicate row for an already-published window.
**Where**: n/a (verification task, no new file - findings recorded as status notes in this file per task, matching `flink-normalization`'s T19 precedent)
**Depends on**: T2, T3, T5
**Reuses**: The T19 verification pattern from `flink-normalization` (`STATE.md` Handoff, 2026-08-21 entry)
**Requirement**: FLA-01 through FLA-09 (all of them - this is where every AC gets a real check)

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Correct `event_count` per `repo_name`/window, matching a hand count of the published messages (Success Criteria #1) - user-confirmed 2026-08-25, not directly observed by the agent
- [x] The late-arriving event is absent from any window's count, no job failure (FLA-05) - user-confirmed 2026-08-25, not directly observed by the agent
- [x] The malformed message is skipped, logged, job keeps running (FLA-06) - user-confirmed 2026-08-25, not directly observed by the agent
- [x] Restarting `aggregation` mid-window resumes the partial count, does not reset to zero (FLA-07, Edge Case) - user-confirmed 2026-08-25, not directly observed by the agent
- [x] Restarting `aggregation` after a window was already published does not produce a duplicate row (FLA-08, Success Criteria #2) - user-confirmed 2026-08-25, not directly observed by the agent
- [x] `events-aggregated` records are keyed by `repo_name` in Kafka UI, not only in the JSON payload (FLA-09) - confirmed 2026-08-25: Kafka UI's Key column shows `{"repo_name":"..."}` as the physical record key, not just present inside the value payload

**Tests**: none - this task *is* the test (Test Coverage Matrix)
**Gate**: full

---

## Phase Execution Map

```
Phase 1:
T1 ─┐
    ├→ T5 ─┐
T4 ─┘      │
T2 ────────┼→ T6
T3 ────────┘
```

Execution is strictly sequential - there is no intra-phase parallelism. A single worker still executes one task at a time, in the order T1 → T2 → T3 → T4 → T5 → T6, even though T1-T4 have no dependency on each other.

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: Provision topic | 1 file, 1-line change | ✅ Granular |
| T2: Write query | 1 file (3 statements, one cohesive contract per `AD-009`) | ✅ Granular - kept as one file deliberately, see `design.md` Tech Decisions |
| T3: Write app.py | 1 file | ✅ Granular |
| T4: Write Dockerfile | 1 file | ✅ Granular |
| T5: Wire compose | 1 file, 2 service blocks (cohesive - a jobmanager/taskmanager pair is one deployable unit) | ✅ Granular |
| T6: Live verification | 1 procedure, no file | ✅ Granular (verification task, not a code task) |

## Diagram-Definition Cross-Check

| Task | Depends On (task body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | No incoming arrow | ✅ Match |
| T2 | None | No incoming arrow | ✅ Match |
| T3 | None | No incoming arrow | ✅ Match |
| T4 | None | No incoming arrow | ✅ Match |
| T5 | T1, T4 | T1 → T5, T4 → T5 | ✅ Match |
| T6 | T2, T3, T5 | T2 → T6, T3 → T6, T5 → T6 | ✅ Match (T5 already transitively covers T1/T4, so T6's body lists T2/T3/T5 rather than repeating T1/T4) |

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1 | config (`create-topics.sh`) | none | none | ✅ OK |
| T2 | SQL query (live-verified layer) | none | none | ✅ OK |
| T3 | `app.py` (unit, conditional) | unit if split logic is non-trivial | unit, conditional | ✅ OK |
| T4 | Dockerfile (build gate) | none | none | ✅ OK |
| T5 | compose config (config gate) | none | none | ✅ OK |
| T6 | verification (is the test) | none (this task IS the live test) | none | ✅ OK |
