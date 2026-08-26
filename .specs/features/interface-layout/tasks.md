# Interface Layout Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is the source of truth for the full flow (per-task cycle, sub-agent delegation, adequacy review, Verifier, discrimination sensor).

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

**Authorship levels** (`AD-008`, `.claude/rules/interaction-protocol.md`): assigned via `/mentor-map` before Execute begins - not pre-judged here.

**No agent commits** (`AD-003`): the agent stages, runs gates, and proposes a commit message per task - the user commits.

---

**Design**: skipped - pure relocation, no architectural decision (per `spec.md`'s scope and the already-approved `tmp/platform-refactor-plan-v2.md`)
**Status**: Done

---

## Test Coverage Matrix

> Generated from codebase. Guidelines found: `CLAUDE.md`'s "Code conventions" section (`unittest.TestCase` + `unittest.mock`, one test file per source module, mirrored under `tests/`), `Makefile`'s `make test` target (`pytest --cov=ingestion --cov=flink --cov-report=term-missing tests`, no enforced threshold). Sampled 5 existing test files: `tests/ingestion/test_models.py`, `tests/flink/normalization/adapters/test_contract_repository.py`, `tests/flink/normalization/test_contracts_github.py`, `tests/flink/normalization/test_app.py`, and `flink-aggregation/tasks.md`'s own precedent for Dockerfile/compose (build gate only, no unit test).

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| `ingestion/models.py` (per-source config loader) | unit | 1:1 with existing coverage: valid source load, unknown source `NotImplementedError`, endpoint/param resolution unchanged | `tests/ingestion/test_models.py` | `pytest tests/ingestion/test_models.py` |
| `flink/normalization/adapters/contract_repository.py` (repointed path) | unit | 1:1 with existing coverage: valid source load, unknown source `NotImplementedError` | `tests/flink/normalization/adapters/test_contract_repository.py` | `pytest tests/flink/normalization/adapters/test_contract_repository.py` |
| `flink/normalization/domain/*` behavior against the relocated `github.yml` | unit | Byte-identical `NormalizedEvent` output - existing fixture-based coverage, path updated only | `tests/flink/normalization/test_contracts_github.py` | `pytest tests/flink/normalization/test_contracts_github.py` |
| `flink/analytics/app.py` (renamed, query-path resolution updated) | unit, only if the path-resolution change is non-trivial to assert | Env var name and resolved path covered if a test already exists for this in the `aggregation` predecessor; otherwise none, matching `flink-aggregation`'s own matrix precedent (SQL/runtime behavior is live-verified, not unit-tested) | `tests/flink/aggregation/test_app.py` → renamed `tests/flink/analytics/test_app.py` | `pytest tests/flink/analytics` |
| Dockerfiles (`ingestion/Dockerfile`, `flink/normalization/Dockerfile`, `flink/analytics/Dockerfile`), `docker-compose.yml` | none | Build/config gate only - same precedent as `flink-aggregation` T4/T5 | n/a | `docker compose -f infra/docker/docker-compose.yml config` (syntax) + live verification task |
| `infra/docker/scripts/create-topics.sh` | none | Unchanged in this feature (topic names untouched) - no gate needed | n/a | n/a |

## Gate Check Commands

> Generated from codebase - confirm before Execute.

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After tasks touching only Python/config, no Docker runtime involved | `make test` - baseline noted below |
| Full | After the live-verification task | `docker compose -f infra/docker/docker-compose.yml up`, re-run `flink-aggregation` T6's exact procedure against the renamed stage |
| Build | Phase completion (Docker/compose changes) | `docker compose -f infra/docker/docker-compose.yml config` (syntax) + `docker compose ... build` |

**Baseline (recorded before this feature's first task, 2026-08-25)**: `make test` → **189 passed, 11 subtests passed, 6 failed**. The 6 failures (`tests/ingestion/test_app.py::TestMain::*`, all `KeyError: 'KAFKA_BOOTSTRAP_SERVERS'`) were treated as pre-existing and unrelated at the time - an apparent environment-variable gap in the local test shell. Quick gates through T10 below compare against **189 passed / 11 subtests**, not the 6 known failures.

**Correction (2026-08-26)**: the "environment gap" framing was wrong. Investigated during the `ingestion/models.py` code review that produced the `YamlSourceConfigRepository` refactor (see T2's notes): 6 of `TestMain`'s 8 tests never patched `KAFKA_BOOTSTRAP_SERVERS` into `os.environ` at all - only the 2 tests that specifically exercise that env var's presence/absence did. They only ever passed by coincidence, riding on whatever the ambient shell happened to have set. Fixed by adding the same `@patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "broker-1:19092,broker-2:19092"})` the other 2 tests already used. `make test` now passes **196/196, 11 subtests, 0 failures** - even with `KAFKA_BOOTSTRAP_SERVERS` explicitly absent from the environment (`env -u KAFKA_BOOTSTRAP_SERVERS make test`). This is the true baseline from this point forward; the "189 passed / 6 pre-existing failures" framing earlier in this file is superseded, not deleted.

---

## Execution Plan

Phases are ordered and run sequentially. 11 tasks total - fits a single batch (≤ ~8 is the no-sub-agent threshold; at 11 it is offered, see the ASK step below), packed as whole phases.

### Phase 1: Ingestion reads per-source configuration (ILO-01..04)

Order: T1, then T2, then T3. See the full dependency diagram in "Phase Execution Map" below.

### Phase 2: Normalization reads contracts from the same source directory (ILO-05..07)

Order: T4 (depends on Phase 1's T1), then T5. See the full dependency diagram below.

### Phase 3: The analytics stage is renamed and its queries relocated (ILO-08..12)

Order: T6, T7, T8, T9 (depends on Phase 1's T2 and Phase 2's T5), T10, T11 (depends on T3 and T5
from earlier phases). See the full dependency diagram below.

---

## Task Breakdown

### T1: Split `ingestion/config/sources.yml` into per-source files

**What**: Create `interface/sources/github/ingestion.yml` and `interface/sources/gitlab/ingestion.yml`, each holding that source's entry from today's `ingestion/config/sources.yml` with the top-level source key stripped (the key becomes the directory name).
**Where**: `interface/sources/github/ingestion.yml`, `interface/sources/gitlab/ingestion.yml`
**Depends on**: None
**Reuses**: The existing `SourceYamlEntry` shape (`endpoints`, `headers`, `auth`, `id_field`, `type_field`) - content copied verbatim, not restructured
**Requirement**: ILO-01, ILO-02

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `interface/sources/github/ingestion.yml` validates against `SourceYamlEntry` with identical field values to today's `github` entry
- [x] `interface/sources/gitlab/ingestion.yml` validates against `SourceYamlEntry` with identical field values to today's `gitlab` entry
- [x] `ingestion/config/sources.yml` still exists at this point (removed in T2, once nothing reads it)

**Tests**: none (data files only, no code yet)
**Gate**: quick

---

### T2: Rework the ingestion loader for per-source files

**What**: Replace `_load_yaml_config()` (`ingestion/models.py:78-87`, `lru_cache(maxsize=1)` over one shared file) with a per-source load: given a source name, read `interface/sources/<source>/ingestion.yml`, validate against `SourceYamlEntry`, cache per source. Missing file → `NotImplementedError`, matching `get_source_config`'s existing contract. **Reuse the shape already proven in `flink/normalization/adapters/contract_repository.py:22-32`** (`YamlContractRepository._load`: build path, try/except `FileNotFoundError` → `NotImplementedError`).
**Where**: `ingestion/models.py`
**Depends on**: T1
**Reuses**: `flink/normalization/adapters/contract_repository.py`'s load pattern
**Requirement**: ILO-01, ILO-02, ILO-03

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `get_source_config("github", ...)` loads `interface/sources/github/ingestion.yml` and returns a `SourceConfig` with fields identical to before the move
- [x] `get_source_config("gitlab", ...)` loads `interface/sources/gitlab/ingestion.yml` with the same result
- [x] `get_source_config("nonexistent", ...)` raises `NotImplementedError` (unchanged contract)
- [x] `_resolve_url`'s endpoint/param resolution logic is untouched - only the loader changes
- [x] `tests/ingestion/test_models.py` required no changes - it already called `get_source_config` with no path assumptions; all 29 cases pass unmodified against the relocated files
- [x] Gate check passes: `pytest tests/ingestion/test_models.py` - 29 passed

**Tests**: unit
**Gate**: quick

---

### T3: Remove `ingestion/config/sources.yml`

**What**: Delete the now-unused shared file and its containing `ingestion/config/` directory (nothing reads it after T2).
**Where**: `ingestion/config/sources.yml` (removed)
**Depends on**: T2
**Reuses**: N/A
**Requirement**: ILO-04

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `ingestion/config/sources.yml` no longer exists (`ingestion/config/` removed entirely)
- [x] `grep -r "config/sources.yml" ingestion/ tests/` returns no matches
- [x] `make test` shows no new failures beyond the 6-failure baseline: 189 passed, 11 subtests passed, 6 failed (same pre-existing `KAFKA_BOOTSTRAP_SERVERS` failures)

**Tests**: none (deletion; covered by T2's tests still passing)
**Gate**: quick

---

### T4: Move the normalization contract to the source directory

**What**: Move `flink/normalization/sources/github.yml` to `interface/sources/github/normalization.yml`, content unchanged (byte-identical).
**Where**: `interface/sources/github/normalization.yml`
**Depends on**: T1 (the `interface/sources/github/` directory must exist)
**Reuses**: N/A - file move only
**Requirement**: ILO-05

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `interface/sources/github/normalization.yml` exists, byte-identical to today's `flink/normalization/sources/github.yml` (confirmed via `diff`)
- [x] `flink/normalization/sources/` no longer exists

**Tests**: none (file move only)
**Gate**: quick

---

### T5: Repoint the normalization contract repository

**What**: Update `YamlContractRepository._load` (`flink/normalization/adapters/contract_repository.py:23`) from `contracts_dir / f"{source}.yml"` to `contracts_dir / source / "normalization.yml"`. Update `flink/normalization/app.py:45`'s `contracts_dir` construction from `Path(__file__).parent / "sources"` to point at `interface/sources/` (three levels up from `flink/normalization/app.py`, matching the container's `WORKDIR /opt/flink/usrlib` layout once `interface/` is copied in - see T9).
**Where**: `flink/normalization/adapters/contract_repository.py`, `flink/normalization/app.py`
**Depends on**: T4
**Reuses**: N/A
**Requirement**: ILO-05, ILO-06, ILO-07

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `YamlContractRepository.get("github")` loads `interface/sources/github/normalization.yml` and returns a `NormalizationContract` identical to before the move
- [x] `YamlContractRepository.get("gitlab")` (no contract present) raises `NotImplementedError`, unchanged (verified via the existing `some_source_without_a_contract_file` case, same code path)
- [x] `tests/flink/normalization/adapters/test_contract_repository.py`'s `CONTRACTS_DIR` updated to `interface/sources`; all 4 existing cases still pass
- [x] `tests/flink/normalization/test_contracts_github.py`'s `CONTRACTS_DIR` updated to `interface/sources`; the full fixture-based `NormalizedEvent` assertions still pass byte-for-byte
- [x] **Correction (2026-08-26, found during T11's `make test` re-run):** `tests/flink/normalization/test_app.py`'s `CONTRACTS_DIR` (asserting the exact `contracts_dir` passed to `YamlContractRepository` in `app.py`) was missed in the original T5 pass - it kept asserting the old `flink/normalization/sources` path and failed once `app.py`'s `contracts_dir` was repointed. Fixed the same way as the other two files; full suite back to the 189-passed/11-subtests baseline
- [x] Gate check passes: `pytest tests/flink/normalization/adapters/test_contract_repository.py tests/flink/normalization/test_contracts_github.py tests/flink/normalization/test_app.py` - all passing, no regression

**Tests**: unit
**Gate**: quick

---

### T6: Rename `flink/aggregation/` to `flink/analytics/` and relocate queries

**What**: `git mv flink/aggregation flink/analytics`; `git mv flink/analytics/queries/repo_counts_5m.sql interface/analytics/repo_counts_5m.sql`; remove the now-empty `flink/analytics/queries/` directory.
**Where**: `flink/analytics/` (moved from `flink/aggregation/`), `interface/analytics/repo_counts_5m.sql`
**Depends on**: None (independent of Phases 1-2, ordered last only because it is the largest single change)
**Reuses**: N/A - directory/file move only
**Requirement**: ILO-08

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `flink/aggregation/` no longer exists
- [x] `flink/analytics/app.py` exists (content not yet updated - that is T7)
- [x] `interface/analytics/repo_counts_5m.sql` exists, byte-identical to today's `flink/aggregation/queries/repo_counts_5m.sql` (git mv - content untouched)

**Tests**: none (move only)
**Gate**: quick

---

### T7: Update `flink/analytics/app.py`'s query path and env var

**What**: Rename `os.environ["AGGREGATION_QUERY_FILE"]` (`app.py:21`) to `os.environ["ANALYTICS_QUERY_FILE"]`. Update the query path resolution (`app.py:23`, today `Path(__file__).parent / "queries" / query_file`) to `Path(__file__).parent.parent.parent / "interface" / "analytics" / query_file` (three levels up from `flink/analytics/app.py` reaches the container's `/opt/flink/usrlib` root, matching T9's `COPY interface/`). No other change to `app.py`'s logic (statement-split, checkpointing, `execute_sql` loop are F1-out-of-scope per `spec.md`'s Assumptions).
**Where**: `flink/analytics/app.py`
**Depends on**: T6
**Reuses**: N/A
**Requirement**: ILO-09

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `ANALYTICS_QUERY_FILE` is the only env var read for the query file name; `AGGREGATION_QUERY_FILE` no longer appears anywhere in `flink/analytics/`
- [x] Query path resolves to `interface/analytics/<file>` relative to the app's runtime root
- [x] `split_statements` and the checkpointing/execution logic are otherwise byte-identical to `flink/aggregation/app.py`'s prior content
- [x] No test file existed for `flink/aggregation/app.py` (`tests/flink/aggregation/` was never created) - nothing to rename; matrix's assumed precedent did not apply

**Tests**: unit, matching `flink-aggregation`'s own precedent (tested only if the split/path logic is non-trivial - it already has minimal coverage from `flink-aggregation`; carry it forward under the new name)
**Gate**: quick

---

### T8: Update `flink/analytics/Dockerfile`

**What**: Update `COPY flink/aggregation/ ./flink/aggregation/` → `COPY flink/analytics/ ./flink/analytics/`; update `CMD` from `standalone-job -py /opt/flink/usrlib/flink/aggregation/app.py` to `.../flink/analytics/app.py`; add `COPY interface/ ./interface/` (needed by T7's path resolution).
**Where**: `flink/analytics/Dockerfile`
**Depends on**: T6, T7
**Reuses**: Same base image / JAR-download structure as before (untouched)
**Requirement**: ILO-10

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `COPY` references `flink/analytics/` and `interface/`, not `flink/aggregation/`
- [x] `CMD` runs `flink/analytics/app.py`
- [x] No other line changed (base image, JAR download step, `requirements/flink.txt` install untouched)

**Tests**: none (build gate only)
**Gate**: build

---

### T9: Add `COPY interface/` to the other two Dockerfiles

**What**: `ingestion/Dockerfile` and `flink/normalization/Dockerfile` each gain `COPY interface/ ./interface/` (needed by T2's and T5's path resolution respectively).
**Where**: `ingestion/Dockerfile`, `flink/normalization/Dockerfile`
**Depends on**: T2, T5
**Reuses**: N/A
**Requirement**: ILO-10

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Both Dockerfiles include `COPY interface/ ./interface/` alongside their existing `COPY` lines
- [x] No other line changed

**Tests**: none (build gate only)
**Gate**: build

---

### T10: Wire the rename into `docker-compose.yml`

**What**: Rename the `aggregation`/`taskmanager-aggregation` services to `analytics`/`taskmanager-analytics`; update `dockerfile: flink/analytics/Dockerfile`; rename the env var `AGGREGATION_QUERY_FILE` → `ANALYTICS_QUERY_FILE`; retag the taskmanager image `apache/flink:2.3-aggregation` → `apache/flink:2.3-analytics`; update `FLINK_PROPERTIES`' `jobmanager.rpc.address` from `aggregation` to `analytics` in both the jobmanager and taskmanager blocks; keep the `8082:8081` port mapping unchanged.
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T8, T9
**Reuses**: The service pair's existing shape - only names change
**Requirement**: ILO-11

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `analytics`/`taskmanager-analytics` service keys exist; `aggregation`/`taskmanager-aggregation` do not (`container_name`, taskmanager image tag, and `depends_on` also renamed for consistency, beyond the task's literal wording but the same rename)
- [x] `dockerfile: flink/analytics/Dockerfile`; env `ANALYTICS_QUERY_FILE: repo_counts_5m.sql`; image `apache/flink:2.3-analytics`; both `jobmanager.rpc.address` occurrences say `analytics`
- [x] `docker` was available in this session (unlike `flink-aggregation`'s T4/T5) - `docker compose -f infra/docker/docker-compose.yml config --quiet` parses with no error

**Tests**: none (config gate only)
**Gate**: quick (offline validation) / build (if `docker` available)

---

### T11: Live verification

**What**: `docker compose -f infra/docker/docker-compose.yml up`, re-run `flink-aggregation` T6's exact procedure (`.specs/features/flink-aggregation/tasks.md`) against the renamed stage: publish the same hand-crafted `events-normalized` messages, inspect `events-aggregated` in Kafka UI, confirm the result is identical - correct per-window counts, late event dropped, malformed message skipped without killing the job, mid-window restart resumes state, post-window restart produces no duplicate, records keyed by `repo_name`.
**Where**: n/a (verification task, no new file - status notes recorded in this file, matching `flink-aggregation` T6's and `flink-normalization` T19's precedent)
**Depends on**: T3, T5, T10
**Reuses**: `flink-aggregation` T6's exact procedure and expected results
**Requirement**: ILO-12 (and re-confirms ILO-01..11 end to end)

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Ingestion polls GitHub normally under the relocated `ingestion.yml` - agent-observed: real polling against `https://api.github.com/events`, auth resolved, 30 events/poll published to `events-raw` on schedule
- [x] Normalization produces identical `NormalizedEvent` output under the relocated `normalization.yml` - agent-observed: real `events-normalized` messages inspected via `kafka-console-consumer`, correct envelope+common shape (`partition_key`, `entity_id`, `entity_name`, `repo_id`, `repo_name`, `org_id`, `org_login`, `public`); the pre-existing "unmapped event type falls back to envelope+common" behavior (e.g. `PushEvent`, not declared in `github.yml`'s `event_types`) reproduced unchanged
- [x] The renamed `analytics` service produces output in `events-aggregated` matching `flink-aggregation`'s T6 result **for 4 of 5 checks** - agent-observed: correct per-window counts keyed by `repo_name` (physical Kafka key, confirmed via `--property print.key=true`), a hand-crafted malformed message did not kill the job, a hand-crafted late event (`event_time` far in the past) did not appear in any window's count
- [x] **Restart/no-duplicate check does NOT match** - a real restart of the `analytics` container caused a full reprocess of `events-normalized` from `earliest-offset` (536 → 2569 aggregated rows after one restart), because no durable checkpoint storage is configured for `env.enable_checkpointing(60000)`. **Not a regression from this feature** - the checkpointing config is byte-identical to what `flink-aggregation` shipped; this is a pre-existing gap that `flink-aggregation`'s own T6 never independently observed (its AC7/AC8 were user-attested only, per `flink-aggregation/validation.md`). Recorded there as a dated addendum (2026-08-26) rather than silently passed here. Decided with the user: register as a known gap, do not fix under this feature's scope, and proceed

**Tests**: none - this task *is* the test (Test Coverage Matrix)
**Gate**: full

---

## Phase Execution Map

Full dependency graph, every edge below matches a task's `Depends on` field exactly (see the
Diagram-Definition Cross-Check table):

```
T1 → T2
T2 → T3
T1 → T4
T4 → T5
T6 → T7
T6 → T8
T7 → T8
T2 → T9
T5 → T9
T8 → T10
T9 → T10
T3 → T11
T5 → T11
T10 → T11
```

Phase 2 does not depend on Phase 1 completing (T4 only needs T1's directory), but phases execute in the order presented for clarity; Phase 3 is independent of Phases 1-2 until T9 and T11, which need earlier phases' changes in place.

Execution is strictly sequential - a single worker executes one task at a time, in the order
T1 → T2 → T3 → T4 → T5 → T6 → T7 → T8 → T9 → T10 → T11.

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: Split ingestion config | 2 files, mechanical split of one source file - cohesive | ✅ Granular |
| T2: Rework ingestion loader | 1 file (`ingestion/models.py`) + its test file | ✅ Granular |
| T3: Remove old ingestion config | 1 file (deletion) | ✅ Granular |
| T4: Move normalization contract | 1 file move | ✅ Granular |
| T5: Repoint contract repository | 2 files (repository + app.py's `contracts_dir`) - one cohesive path change | ✅ Granular |
| T6: Rename aggregation → analytics | 1 directory move + 1 file move - one cohesive rename | ✅ Granular |
| T7: Update app.py's env var/path | 1 file | ✅ Granular |
| T8: Update analytics Dockerfile | 1 file | ✅ Granular |
| T9: Update the other two Dockerfiles | 2 files, identical one-line change - cohesive | ✅ Granular |
| T10: Wire compose | 1 file, one service pair (cohesive - a jobmanager/taskmanager pair is one deployable unit, same precedent as `flink-aggregation` T5) | ✅ Granular |
| T11: Live verification | 1 procedure, no file | ✅ Granular (verification task, not a code task) |

## Diagram-Definition Cross-Check

| Task | Depends On (task body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | No incoming arrow | ✅ Match |
| T2 | T1 | T1 → T2 | ✅ Match |
| T3 | T2 | T2 → T3 | ✅ Match |
| T4 | T1 | T1 → T4 (cross-phase; Phase 2 depends on Phase 1's T1 only) | ✅ Match |
| T5 | T4 | T4 → T5 | ✅ Match |
| T6 | None | No incoming arrow | ✅ Match |
| T7 | T6 | T6 → T7 | ✅ Match |
| T8 | T6, T7 | T6 → T8, T7 → T8 | ✅ Match |
| T9 | T2, T5 | T2 → T9, T5 → T9 (cross-phase) | ✅ Match |
| T10 | T8, T9 | T8 → T10, T9 → T10 | ✅ Match |
| T11 | T3, T5, T10 | T3 → T11, T5 → T11, T10 → T11 (cross-phase) | ✅ Match |

All 14 edges above appear exactly once each in the "Phase Execution Map" diagram, with no extra
edges - the diagram is the single authoritative graph; the per-phase orderings above it are prose,
not additional diagrams.

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1 | data files only | none | none | ✅ OK |
| T2 | `ingestion/models.py` (loader) | unit | unit | ✅ OK |
| T3 | deletion | none | none | ✅ OK |
| T4 | file move only | none | none | ✅ OK |
| T5 | `contract_repository.py`, `app.py` (path) | unit | unit | ✅ OK |
| T6 | directory/file move only | none | none | ✅ OK |
| T7 | `flink/analytics/app.py` | unit (conditional, per matrix) | unit | ✅ OK |
| T8 | Dockerfile (build gate) | none | none | ✅ OK |
| T9 | 2 Dockerfiles (build gate) | none | none | ✅ OK |
| T10 | compose config (config gate) | none | none | ✅ OK |
| T11 | verification (is the test) | none (this task IS the live test) | none | ✅ OK |
