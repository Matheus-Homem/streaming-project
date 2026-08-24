# Flink Normalization Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is the source of truth for the full flow (per-task cycle, sub-agent delegation, adequacy review, Verifier, discrimination sensor).

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

**Project-specific override (`AD-001`/`AD-003`/`AD-008`, `CLAUDE.md`): mentor mode governs Execute for this feature, scoped per task by its authorship level.** Every open task carries an `own`/`paired`/`deliver` level, assigned from the knowledge state and recorded in `.mentor/features/flink-normalization/map.md` - read it before starting any task:

- `own` - the agent never authors the code; it explains, points at the approach, reviews what the user writes, and runs the gate.
- `paired` - the user makes and defends the decision the task carries first; only then does the agent write the mechanical body around it.
- `deliver` - the agent writes it, the user reviews. Never logged as evidence.

Levels are read from `map.md`, not re-judged per task. As of 2026-08-18 the open work splits 7 `deliver` / 4 `paired` / 1 `own` - T15-T19 carry five of the six `unassessed` `pyflink` objectives and are where the learning is.

The agent never commits (`AD-003`, unaffected by `AD-008`) - it hands off a diff + suggested Conventional Commit message per task instead.

---

**Design**: `.specs/features/flink-normalization/design.md`
**Status**: Phases 1-6 (T1-T22) all closed or explicitly not-applicable as of 2026-08-24. Two open sub-items remain, both non-blocking for merge but open for a true feature close: T19's live smoke test needs a re-run against T16's 2026-08-24 sink key fix (see T16/T19 status notes - the 2026-08-21 run predates the fix and likely only checked the JSON payload, not the physical Kafka record key), and T15's `schema_version != 1` rejection sub-case (`shared.models.RawEvent.schema_version` has no `== 1` constraint yet).

---

## Test Coverage Matrix

> Generated from codebase sampling (`tests/ingestion/`) and `spec.md`/`design.md` - confirm before Execute. Guidelines found: no `AGENTS.md`/`CONTRIBUTING.md`; inferred from existing test depth in `tests/ingestion/adapters/test_engine.py`, `tests/ingestion/test_app.py`, `tests/ingestion/test_models.py` (unittest.TestCase + unittest.mock, one file per source module, full-branch coverage including log-and-skip paths).

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| `NormalizationEngineBase` (ABC, `ports.py`) | none | Interface only, no logic - matches the untested-ABC precedent of `ingestion/ports.py` | - | build gate only |
| Contract models (Pydantic grammar, `models.py`) | unit | All validation branches. A contract with an unknown key, a missing `from`/`expression`, or an unsupported `as:` value MUST fail validation - this is the platform's only defence against a silently-null column (`design.md` Risks) | `tests/flink/normalization/test_models.py` | `make test` |
| Contract compiler (`models.py`) | unit | 1:1 to every row of `design.md`'s compilation-rule table (`from`, `take`, `as: boolean`, `as: timestamp`, `default`, raw `expression`) | `tests/flink/normalization/test_models.py` | `make test` |
| Contract loader (`get_contract`, relocated 2026-08-19 to `domain/utils.py`) | unit | Happy path + unknown source raises + caching behaviour - mirrors `tests/ingestion/test_models.py`'s coverage of `get_source_config` | `tests/flink/normalization/domain/test_utils.py` | `make test` |
| Custom JMESPath functions (`NormalizationFunctions`, relocated 2026-08-19 to `domain/evaluator.py`) | unit | All branches incl. malformed/absent input - it is the one piece of real computation in the contract path | `tests/flink/normalization/domain/test_evaluator.py` | `make test` |
| Rule evaluation + contract orchestration (relocated 2026-08-19: `NormalizationEngine`+`NormalizationJmespathEvaluator` → `NormalizationRulesEventEvaluator` in `domain/evaluator.py`, envelope-building → `FlinkNormalizationPipeline` in `use_case.py`) | unit | All branches; 1:1 to `spec.md`'s P1 Normalization ACs - envelope shape, common block, per-type block, unmapped-type fallback (AC5), absent optional field → null | `tests/flink/normalization/domain/test_evaluator.py`, `tests/flink/normalization/test_use_case.py` | `make test` |
| Normalization contracts (`sources/*.yml`, relocated 2026-08-19 - path dropped `config/`) | unit | The contract is data, but its *effect* is behaviour: each declared event type must produce exactly its `spec.md` Normalization Mapping row when run against a real fixture. Verified through `FlinkNormalizationPipeline` (`use_case.py`), not by asserting on the YAML | `tests/flink/normalization/test_contracts_github.py` | `make test` |
| `NormalizationFunction` (Flink `FlatMapFunction`, `job.py`) | unit | All branches: valid message → yields normalized record; malformed JSON / bad `schema_version` / unregistered source → yields nothing + logs. Called directly as a plain Python method - no live cluster needed | `tests/flink/normalization/test_job.py` | `make test` |
| Job wiring (`app.py`) | unit | Config/env wiring covered with mocks (mirrors `tests/ingestion/test_app.py`'s `build_arguments`/`configure_*` pattern) - `StreamExecutionEnvironment.execute()` itself is not exercised in unit tests | `tests/flink/normalization/test_app.py` | `make test` |
| Docker/compose/shell infra (`Dockerfile`s, `docker-compose.yml`, `create-topics.sh`) | none | No unit tests apply; verified via each story's `spec.md` Independent Test | - | manual (spec Independent Test) + `docker compose -f infra/docker/docker-compose.yml config` for YAML/interpolation validity |

**Coverage Expectation provenance**: no project-wide testing guideline file exists; the strong default (spec ACs → 1:1 test coverage, all listed edge cases) was applied, using `tests/ingestion/`'s existing depth as the floor.

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After a task that only touches one new/changed test file | `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/<path>` |
| Full | After a phase completes, or any task touching shared modules (`models.py`, `ports.py`) | `make test` (updated to `--cov=ingestion --cov=flink` in T6) |
| Build | After infra-only tasks (Dockerfiles, compose, shell scripts) | `docker compose -f infra/docker/docker-compose.yml config` (syntax/interpolation check) + the task's own manual Independent Test |

---

## Execution Plan

Phases are ordered and run sequentially - each phase completes before the next begins, and tasks within a phase execute in order. (Arrow diagrams for each phase are consolidated in the **Phase Execution Map** below, with exactly one arrow per real dependency - see that section for the authoritative graph.)

### Phase 1: Kafka topic provisioning

Tasks: T1, T2

### Phase 2: Ingestion containerization

Tasks: T3, T4

### Phase 3: Contract foundation

The source-agnostic machinery: contract grammar, compiler, loader, and the normalizer that
interprets them. No GitHub knowledge in any of it.

Tasks: T5, T6, T7, T8, T9, T10, T11

### Phase 4: GitHub contract + Flink function

The GitHub-specific knowledge, expressed entirely as YAML.

Tasks: T12, T13, T14, T15

### Phase 5: Flink job wiring + cluster infra

Tasks: T16, T17, T18, T19

### Phase 6: Coverage extension (remaining 6 event types)

Tasks: T20, T21, T22

> **Revised 2026-08-17** for contract-driven normalization (`AD-006`). Phases 1-2 (T1-T4) are done and
> unchanged; T5 survives the redesign as written. The former T6-T29 are replaced: the nine
> one-Python-method-per-event-type tasks and the six P2 extraction tasks collapse into YAML contract
> entries, and new tasks cover the contract grammar, compiler, loader, and custom function. 29 tasks → 22.

---

## Task Breakdown

### T1: Kafka topic-creation script

**What**: Shell script that retries `kafka-topics.sh --create` for `events-raw` and `events-normalized` (3 partitions, replication factor 3, `retention.ms=604800000`) against the broker cluster until it succeeds, then exits 0.
**Where**: `infra/docker/scripts/create-topics.sh`
**Depends on**: None
**Reuses**: The `apache/kafka:latest` image already used by `controller-*`/`broker-*` in `infra/docker/docker-compose.yml` (same image ships `kafka-topics.sh`)
**Requirement**: FLK-01

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Script creates both topics idempotently (safe to re-run - `--if-not-exists` or equivalent) - `create-topics.sh` (`infra/docker/scripts/`)
- [x] Retries against the broker until reachable instead of failing on first attempt - 12 attempts × 15s
- [x] Both topics end up with 3 partitions, replication factor 3, 7-day retention - `retention.ms=604800000`

**Tests**: none
**Gate**: build (manual run against a local `docker compose up` of the existing broker services)

---

### T2: Wire `topic-init` service into docker-compose

**What**: Add a one-shot `topic-init` service running T1's script, `depends_on` all three brokers, positioned so downstream services can depend on its completion.
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T1
**Reuses**: Existing `broker-*`/`controller-*` service definitions and `depends_on` conventions already in the file
**Requirement**: FLK-01, FLK-05

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `docker compose up` runs `topic-init` after brokers are reachable and it exits 0 - `infra/docker/docker-compose.yml`'s `topic-init` service
- [x] Kafka UI (`localhost:8080`) shows both topics with the configured partitions/replication/retention

**Tests**: none
**Gate**: build

---

### T3: Ingestion service Dockerfile

**What**: Dockerfile that installs the ingestion runtime dependencies and runs `python -m ingestion.app --source github` as its `CMD`. *(Now `requirements/ingestion.txt` - the flat `requirements.txt` was split per target during T6.)*
**Where**: `ingestion/Dockerfile`
**Depends on**: None
**Reuses**: the pinned dependency set, existing `ingestion/app.py` entrypoint (unchanged)
**Requirement**: FLK-02

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Image builds successfully (`docker build -f ingestion/Dockerfile .`) - `ingestion/Dockerfile`
- [x] Container runs `python -m ingestion.app --source github` without extra flags - image `CMD`

**Tests**: none
**Gate**: build

---

### T4: Wire `ingestion` service into docker-compose

**What**: Add the `ingestion` service (T3's image), `depends_on: topic-init` (completed), reads `KAFKA_BOOTSTRAP_SERVERS` from the compose network's internal broker addresses.
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T2, T3
**Reuses**: Same `depends_on`/environment-variable conventions as other services in the file
**Requirement**: FLK-02, FLK-03

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] WHEN `docker compose up` runs THEN the `ingestion` container publishes to `events-raw` without manual invocation (spec P1 Infrastructure AC2) - fixed 2026-08-17 during Design revisit: the merged version was missing `environment: KAFKA_BOOTSTRAP_SERVERS` on the `ingestion` service (container crashed with `KeyError` - `.env` isn't copied into the image, and even if it were, `localhost:29092` isn't reachable from inside the compose network), and used list-form `depends_on: [topic-init]` which only waits for the container to *start*, not to finish provisioning topics (spec AC5). Both fixed in `infra/docker/docker-compose.yml`: added `KAFKA_BOOTSTRAP_SERVERS: broker-1:19092` (single broker, matching how `producer.py` wraps the env var - a comma-joined multi-broker string would not be split), and `depends_on: topic-init: condition: service_completed_successfully`
- [x] `make ingestion-default` still runs the same code standalone, unaffected (spec P1 Infrastructure AC3) - untouched, still reads host-side `.env`'s `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`

**Tests**: none
**Gate**: build

---

### T5: `NormalizationEngineBase` port

**What**: Abstract `NormalizationEngineBase` class with one abstract method, `normalize(self, event: RawEvent) -> dict[str, Any]`, docstring-documented like `ingestion/ports.py`'s existing ABCs.
**Where**: `flink/normalization/ports.py`
**Depends on**: None
**Reuses**: `ingestion/ports.py`'s ABC shape (`IngestionEngineBase`, etc.) as the direct template
**Requirement**: FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when** (verified 2026-08-18 - shipped as two ABCs instead of one, see `design.md`'s naming-drift note; behavior below still matches the intent):
- [x] Orchestration ABC defined with a `normalize` abstract method matching the intent - `NormalizationEngineBase.normalize(self, event: RawEvent) -> dict[str, Any]` (`flink/normalization/ports.py`); a second ABC, `NormalizationEvaluatorBase.evaluate(self, rule: FieldRule, payload: dict) -> Any`, was split out for the compile/evaluate half - not in the original one-method design
- [x] Docstrings document args/return per design.md's intent (never raises for a structurally valid `RawEvent`; never returns `None`)

**Tests**: none
**Gate**: build

---

### T6: Make the `flink` package testable

**What**: Project wiring so the `flink/` package participates in the existing tooling: pin `jmespath==1.1.0`, update `Makefile`'s `test` target to `--cov=ingestion --cov=flink` and its `neat` target to include `flink` in the `autoflake`/`isort`/`black` paths, and create the `tests/flink/normalization/` package skeleton (`__init__.py` files) mirroring `tests/ingestion/`.

**Executed beyond the original scope (2026-08-17, accepted):** rather than appending to the flat `requirements.txt`, the dependency set was split per deployment target under `requirements/` - `base.txt` (shared runtime), `ingestion.txt` and `flink.txt` (per-service, what each image installs), `dev.txt` (everything + tooling). Each pin carries a comment naming where it is used. This matches the services-monorepo shape `AD-005` describes and means the Flink image will not install `kafka-python`/`requests`. It also surfaced a latent bug - see the done-when notes.
**Where**: `requirements/`, `Makefile`, `tests/flink/`
**Depends on**: T5
**Reuses**: The existing `Makefile` target shape (`@echo` + one-liner) and `tests/ingestion/`'s package layout as the 1:1 template
**Requirement**: FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `jmespath==1.1.0` pinned - `requirements/flink.txt` (verified available on PyPI 2026-08-17)
- [x] `make test` reports coverage for `flink/` as well as `ingestion/` - `flink/normalization/ports.py` appears in the `term-missing` table
- [x] `make neat` reformats `flink/` without errors - `black --check`/`isort --check-only`/`autoflake` all clean over `ingestion flink shared tests`
- [x] `make test` still passes with the existing suite unchanged - 94 passed, no regression
- [x] **Bug found and fixed in passing**: `PyYAML` was never pinned, although `d7c585b` (PR #5) added `import yaml` to `ingestion/models.py`. The local `.venv` happened to have `yaml` 6.0.3 installed, so tests passed and the gap stayed invisible - but `ingestion/Dockerfile` installs only from the pinned file, so the container would have raised `ModuleNotFoundError` at startup. Now pinned in `requirements/base.txt` (both services need it: `ingestion/config/sources.yml` and the Flink contracts). This is the third defect found in the supposedly-done T3/T4 containerization, after the missing `KAFKA_BOOTSTRAP_SERVERS` and the non-blocking `depends_on`
- [x] `tests/flink/normalization/` package skeleton created (`__init__.py` files) mirroring `tests/ingestion/` - `tests/flink/__init__.py`, `tests/flink/normalization/__init__.py`

**Tests**: none
**Gate**: build

---

### T7: Contract grammar (Pydantic models)

**What**: `FieldRule` and `NormalizationContract` Pydantic models per `design.md`'s Contract models section - the grammar a contract YAML must satisfy. `FieldRule` carries `from`/`take`/`as`/`default`/`expression` (with `from_`/`as_` aliases, since both are Python keywords/builtins); `as_` is a `Literal["boolean", "timestamp"]`, never a free string. Validation must reject: unknown keys, a rule with neither `from` nor `expression`, and an unsupported `as:` value.
**Where**: `flink/normalization/models.py`
**Depends on**: T6
**Reuses**: `ingestion/models.py`'s `SourceYamlEntry` as the direct precedent for a Pydantic model validating a YAML entry
**Requirement**: FLK-06, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `FieldRule` and `NormalizationContract` match `design.md`'s Contract models section
- [x] A rule with neither `from` nor `expression` fails validation - also rejects both being set together (stricter than the minimum requirement)
- [x] An unsupported `as:` value fails validation (this is what turns a typo into a startup error rather than a null column - `design.md` Risks)
- [x] An unknown key in a rule fails validation rather than being silently ignored - `ConfigDict(extra="forbid")`
- [x] Unit tests cover each rejection branch above plus a valid contract parsing cleanly - `TestFieldRule` (6 cases) + `TestNormalizationContract` (4 cases, incl. a full valid contract matching `design.md`'s shape and a nested-`FieldRule` rejection bubbling up through `NormalizationContract`)
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_models.py` - 10 passed; full `make test` also verified (104 passed, `flink/normalization/models.py` 100% coverage, no regression)

**Tests**: unit
**Gate**: quick

---

### T8: `iso_to_millis` custom JMESPath function

**What**: A `jmespath.functions.Functions` subclass registering `iso_to_millis(string) -> int`, converting an ISO 8601 timestamp to epoch milliseconds - the one transformation JMESPath cannot express natively (verified in `.specs/PLATFORM.md`). Exposed as a shared `jmespath.Options` instance the compiler and normalizer both use.
**Where**: `flink/normalization/functions.py`
**Depends on**: T6
**Reuses**: Nothing existing - new module. Note `ingestion/adapters/engine.py` already produces `observed_at` via `datetime.now().isoformat()`, so the parser must handle both that format and GitHub's `Z`-suffixed form
**Requirement**: FLK-09

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `iso_to_millis` converts a `Z`-suffixed GitHub timestamp correctly (`2026-07-17T12:21:32Z` → `1784290892000`, verified value)
- [x] It also handles the offset-naive form `ingestion` produces for `observed_at` - treated as UTC (flagged as an inherited assumption worth revisiting: `ingestion/adapters/engine.py`'s `observed_at` is actually local system time, not UTC - not in this task's scope)
- [x] Behaviour on a null/absent input is defined and tested (returns null rather than raising - a missing timestamp must not kill the record, per `design.md`'s Error Handling) - both `None` and `""` return `None`
- [x] Unit tests cover each case above - `TestNormalizationFunctionsIsoToMillis` (5 cases, direct calls) + `TestIsoToMillisThroughJmespath` (3 cases, exercised through `jmespath.compile(...).search(..., options=OPTIONS)` - the real call path the compiler/normalizer will use)
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/adapters/test_evaluator.py` - 8 passed; full `make test` also verified (112 passed, `flink/normalization/functions.py` 100% coverage, no regression)

**Tests**: unit
**Gate**: quick

---

### T9: Contract compiler (`FieldRule` → JMESPath expression)

**What**: The translation function turning one validated `FieldRule` into one JMESPath expression string, implementing exactly `design.md`'s compilation-rule table. This is the layer that keeps JMESPath syntax out of the contract author's way (`AD-006`).
**Where**: `flink/normalization/models.py`
**Depends on**: T7, T8
**Reuses**: `design.md`'s compilation-rule table is the binding specification - every row is a test case
**Requirement**: FLK-06, FLK-07

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `from: actor.id` → `actor.id`
- [x] `from: payload.issue.labels` + `take: name` → `payload.issue.labels[].name`
- [x] `from: payload.issue.pull_request` + `as: boolean` → the null-comparison form
- [x] `from: created_at` + `as: timestamp` → `iso_to_millis(created_at)`
- [x] `default:` produces the documented fallback form - `_jmespath_literal` uses `json.dumps` so string/list/bool defaults serialize correctly (not `str()`), and `default: null` compiles to the bare path (native null-on-missing, no `||`)
- [x] `expression:` is passed through verbatim (escape hatch, `AD-006`)
- [x] Unit tests: one per row above, asserting the compiled expression AND that it evaluates correctly against a real event fixture - not just string equality, which would pass on a syntactically valid but wrong expression. Fixture note: the compiled expressions are evaluated against the captured events inlined in `tests/fixtures/events.py` - no test reads a capture file from disk, since captures are untracked (`.gitignore`'s `*_sample.*`)
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_models.py` - 18 passed; full `make test` also verified (121 passed, `flink/normalization/models.py` 100% coverage, no regression)

**Tests**: unit
**Gate**: quick

---

### T10: Contract loader

**What**: `get_normalization_contract(source: str) -> NormalizationContract` - reads `flink/normalization/config/sources/<source>.yml`, validates it through T7's models, compiles every field rule once via T9, and caches the result. Raises `NotImplementedError` for an unknown source. Directly mirrors `ingestion.models.get_source_config`'s shape.
**Where**: `flink/normalization/models.py`
**Depends on**: T9
**Reuses**: `ingestion/models.py`'s `_load_yaml_config` (`@lru_cache` + `Path(__file__).parent / "config"` + `yaml.safe_load`) as the 1:1 template
**Requirement**: FLK-06, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] A valid contract file loads and validates - returns a `NormalizationContract` of parsed `FieldRule`s (not pre-compiled JMESPath strings - see caveat below)
- [x] An unknown source raises `NotImplementedError`, matching `get_source_config`'s convention
- [x] An invalid contract fails at load time with a message naming the field (Pydantic's own error text; doesn't name the file/event type explicitly - minor gap vs the exact wording here)
- [~] Compilation happens once per source, not per call (`design.md` Tech Decisions) - **deliberately deferred, 2026-08-18**: confirmed still not true (`NormalizationJmespathEvaluator._compile_rule` in `flink/normalization/adapters/evaluator.py` rebuilds the JMESPath expression string on every `evaluate()` call, once per field per event). User explicitly accepted this as non-blocking tech debt rather than fixing it now - a pure perf gap (correctness is unaffected; every test passes), worth revisiting before T19's real-traffic run, not before. Tracked here and in `.specs/STATE.md` so it isn't lost.
- [x] Unit tests cover each verified branch above - `TestGetContract` (4 cases: valid load, `NotImplementedError`, invalid contract, caching via `cache_clear()`/mocked `yaml.safe_load`)
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_models.py` - 14 passed

**Tests**: unit
**Gate**: quick
**Status**: Closed 2026-08-18 - one item deliberately deferred as accepted tech debt (see `[~]` above), not a false-done checkbox.

---

### T11: `NormalizationEngine`

**What**: The single source-agnostic `NormalizationEngineBase` implementation. Resolves the contract for `event.source`, looks up the per-event-type block for `event.source_event_type` (empty block when undeclared), evaluates envelope + common + per-type rules against `RawEvent.payload`, and returns one flat dict. Contains zero GitHub-specific logic.
**Where**: `flink/normalization/adapters/engine.py`
**Depends on**: T10
**Reuses**: `flink/normalization/ports.py`'s `NormalizationEngineBase` (T5), `shared.models.RawEvent`
**Requirement**: FLK-06, FLK-07, FLK-08, FLK-09, FLK-10, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Re-verified 2026-08-18 against the code as it actually shipped** - `NormalizationEngine` (`flink/normalization/adapters/engine.py`), not `NormalizationEngine`/`contract_normalizer.py` (see `design.md`'s naming-drift note).

**Done when**:
- [x] Output contains the Domain-Neutral Envelope fields from `spec.md` - **closed 2026-08-19** (agent-authored under `AD-008`, task level `deliver`). `_clean_event_dictionary` now emits `source`, `event_id`, `event_type`, `ingested_at` (epoch millis) and `schema_version`, and `source_event_endpoint`/`observed_at`/`payload` no longer leak into the output. Four bugs fixed in the in-progress version: the `"ingested_at "` and `"schema_version "` keys carried a trailing space (silently producing field names no consumer would match); `source` was missing entirely despite spec AC2 requiring it; and the local `_iso_to_epoch_millis` lacked the tz-naive->UTC guard that `evaluator.py`'s `iso_to_millis` has, so a naive `observed_at` would have been read as local time. `schema_version` is now the module constant `NORMALIZED_SCHEMA_VERSION = 1`, independent of `RawEvent.schema_version` by construction rather than by coincidence. Covered by four new tests in `TestNormalizationEngine` (exact envelope field names + absence of raw-only fields, epoch-millis conversion, naive-timestamp fallback, schema_version independence). `make test`: 135 passed
- [x] `partition_key` is populated from the contract's `partition_key` rule (FLK-08)
- [x] An event type absent from the contract's `event_types` still returns envelope + common block with an empty per-type block, never `None` and never raising (FLK-10, spec P1 AC5) - **fixed 2026-08-18**, two review rounds: first attempt returned `None` for the undeclared-type path (a drop, conflating AC5 with AC6's log-and-skip); second attempt lost `partition_key`/`envelope`/`common` too (fell back to `event.model_dump()` alone, none of which carries the contract-resolved fields). Final fix: `_collect_field_rules`'s `except KeyError` branch rebuilds the `chain` from just `partition_key`+`envelope`+`common` (dropping only the failed `event_types[event_type]` piece), instead of returning `None`. Verified live against the real `github.yml` contract: an unmapped `CreateEvent` now returns `partition_key`/`actor_id`/`actor_login`/`event_time`/`repo_id`/`repo_name`/`org_login` populated, no per-type fields, no exception
- [x] A contract path absent from a given payload yields a null field rather than an error (`design.md` Error Handling)
- [x] No GitHub identifier appears anywhere in this module - verifiable by grep - asserted in test via `inspect.getsource` (`test_module_contains_no_github_specific_vocabulary`)
- [x] Unit tests cover each verified branch above using a minimal hand-built contract, independent of the real GitHub contract (which T12-T13 test against real fixtures) - `TestNormalizationEngine` (6 cases, `tests/flink/normalization/adapters/test_engine.py`) - now includes the undeclared-event-type case (`test_can_normalize_event_of_undeclared_type_without_raising`, `test_can_evaluate_only_envelope_and_common_rules_for_undeclared_type`), agent-authored under `AD-002` (production code confirmed functional, user explicitly requested the test)
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/adapters/test_engine.py` - 6 passed (2026-08-18); full `make test` also verified - 131 passed, no regression, `flink/normalization/models.py`/`ports.py`/`evaluator.py` still 100% coverage

**Tests**: unit
**Gate**: quick - closes Phase 3
**Status**: Re-opened 2026-08-18 (`/mentor-next` dry run) - the AC5 fallback fix stands, but the Domain-Neutral Envelope field-naming gap above still needs a fix + test before Phase 3 can close for real.

**Relocation note (2026-08-19, officializing an uncommitted refactor - transcription only)**: `NormalizationEngine` (`flink/normalization/adapters/engine.py`) was split again - rule evaluation is now `NormalizationRulesEventEvaluator` (`flink/normalization/domain/evaluator.py`), orchestration (building the `NormalizedEvent`) is now `FlinkNormalizationPipeline` (`flink/normalization/use_case.py`). Tests moved accordingly (`tests/flink/normalization/domain/test_evaluator.py`, `tests/flink/normalization/test_use_case.py`). This task's done-when items still hold against the new location - see `design.md`'s second naming-drift table.

---

### T12: GitHub contract - shared blocks

**What**: `flink/normalization/config/sources/github.yml` with its `source`, `partition_key`, `envelope`, and `common` blocks per `spec.md`'s Domain-Neutral Envelope and GitHub-specific fields block tables. No `event_types` entries yet (T13, T14).
**Where**: `flink/normalization/sources/github.yml` (path dropped `config/` - relocated 2026-08-19, see `design.md`'s naming-drift note)
**Depends on**: T11
**Reuses**: `spec.md`'s Normalization Mapping tables are the authoritative field list - this task transcribes them, it does not re-derive them
**Requirement**: FLK-07, FLK-08

**Status note (updated 2026-08-18 by `/mentor-next`'s dry-run verification, second pass)**: the `is_public`-vs-`public` naming gap flagged in the previous pass is now fixed - `common.public` in `flink/normalization/config/sources/github.yml` maps `from: "public"` directly, and the field set (`repo_id`, `repo_name`, `org_id`, `org_login`, `public`) now matches `spec.md`'s GitHub-specific fields block exactly, live-verified. Still no `tests/flink/normalization/test_contracts_github.py` (the matrix's expected location) - `TestNormalizationEngine` only exercises a hand-built minimal contract, so none of T12/T13's "field by field, no extras/omissions" criteria have real test evidence yet. T11's still-open Domain-Neutral Envelope gap (see above) also affects what a real `test_contracts_github.py` would need to assert for the envelope half of this task.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `partition_key` resolves to `repo.name` (spec P1 AC3) - live-verified 2026-08-18
- [x] `envelope` covers `entity_id`, `entity_name`, `event_time` (with `as: timestamp`) - live-verified 2026-08-18 as `actor_id`/`actor_login`, renamed 2026-08-19 to `entity_id`/`entity_name` (same values), `event_time` correctly epoch-millis via `iso_to_millis`
- [x] `common` covers `repo_id`, `repo_name`, `org_id`, `org_login`, `public` - re-verified 2026-08-18 by `/mentor-next`: the contract now emits `public` (`from: "public"`), matching `spec.md` exactly; the earlier `is_public` naming gap noted below has since been fixed in the code
- [x] `org_id`/`org_login` come back null for an event whose payload has no `org`, verified against a real fixture (spec.md Edge Cases) - live-verified 2026-08-18
- [x] Unit tests run a real event through `NormalizationEngine` and assert the envelope + common output, field by field - **closed 2026-08-19** (agent-authored, task level `deliver`): `tests/flink/normalization/test_contracts_github.py`, 17 tests + 4 subtests, driven through `NormalizationEngine` against real captured events (`SAMPLE_SHAPED_GITHUB_EVENTS` in `tests/fixtures/events.py`). Envelope identity, actor, both epoch-millis conversions, repo/org block, `public` as a real bool, null org fields on an org-less event, and absence of the raw-only fields are each asserted (`tests/flink/normalization/test_contracts_github.py`); `tests/fixtures/events.py`'s `GITHUB_EVENT` (a real captured `IssueCommentEvent`) is the fixture it is built against
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_contracts_github.py` - 17 passed, 4 subtests (2026-08-19); full `make test` also verified - 155 passed, no regression

**Tests**: unit
**Gate**: quick

---

### T13: GitHub contract - the 4 sample-verified event types

**What**: `event_types` entries for `IssueCommentEvent`, `PullRequestEvent`, `PullRequestReviewEvent`, and `PullRequestReviewCommentEvent` - the four whose field mapping was verified against real traffic.
**Where**: `flink/normalization/sources/github.yml` (path dropped `config/` - relocated 2026-08-19)
**Depends on**: T12
**Reuses**: `tests/fixtures/events.py`'s `SAMPLE_SHAPED_GITHUB_EVENTS` holds a real captured instance of each of these four; `spec.md`'s per-type curated-field rows are the binding spec
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Each of the four types produces exactly its `spec.md` Normalization Mapping row - field by field, no extras, no omissions - **verified 2026-08-19 by `/mentor-next`**: the four `event_types` entries exist in `config/sources/github.yml` (added since the 2026-08-18 note below, which is now stale) and their field sets match `spec.md` exactly - `IssueCommentEvent` 16, `PullRequestEvent` 13, `PullRequestReviewEvent` 10, `PullRequestReviewCommentEvent` 14, no extras, no omissions. Field *values* beyond the three criteria below are still only spot-checked, not asserted - that is what the missing test file covers
- [x] `issue_labels` is a list of label names, not label objects (`take: name`) - live-verified 2026-08-19 against the real sample: `['area/kubelet', 'sig/node', 'kind/feature', 'needs-triage']`
- [x] `issue_is_pull_request` is a real boolean reflecting presence/absence of `issue.pull_request` - live-verified 2026-08-19 both ways: `True` with the key injected, `False` with it removed. This is the one field where `as: boolean`'s presence-check semantics are the correct reading (unlike `public`, see T12)
- [x] `label_name` is null when `payload.label` is absent - live-verified 2026-08-19: `'needs-triage'` on the real `PullRequestEvent`, `None` with `payload.label` removed
- [x] Unit tests use the real fixture for each type, driven through `NormalizationEngine` (the renamed `NormalizationEngine`, kept here as the historical name) - **closed 2026-08-19** (agent-authored, task level `deliver`): `tests/flink/normalization/test_contracts_github.py`, 17 tests + 4 subtests, driven through `NormalizationEngine` against real captured events (`SAMPLE_SHAPED_GITHUB_EVENTS` in `tests/fixtures/events.py`). Field sets asserted as exact set equality per type, so an extra or missing field fails. A guard test asserts the fixture still carries all four types, so the subTest loop cannot pass empty
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_contracts_github.py` - 17 passed, 4 subtests (2026-08-19); full `make test` also verified - 155 passed, no regression

**Tests**: unit
**Gate**: quick

---

### T14: GitHub contract - the 7 doc-resolved event types

**What**: `event_types` entries for `WatchEvent`, `CreateEvent`, `DeleteEvent`, `PublicEvent`, `GollumEvent`, `IssuesEvent`, and `MemberEvent` - mapped from the GitHub Events API docs and cross-checked against sample-verified nested shapes.
**Where**: `flink/normalization/sources/github.yml` (path dropped `config/` - relocated 2026-08-19)
**Depends on**: T13
**Reuses**: `spec.md`'s per-type rows; the `issue`/`user`/`label` nested shapes already verified by T13's fixtures
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Each of the seven types produces exactly its `spec.md` Normalization Mapping row - **closed 2026-08-19** (agent-authored, task level `deliver`): `CreateEvent` 5, `DeleteEvent` 3, `PublicEvent` 0, `GollumEvent` 1, `IssuesEvent` 14, `MemberEvent` 3 (`WatchEvent` 1 was already declared). Contract now declares 11 types; each asserted as exact set equality against the spec row
- [x] `CreateEvent` does NOT emit `full_ref` (deliberately excluded - `spec.md` Assumptions) - **closed 2026-08-19** (agent-authored, task level `deliver`) - the hand-built fixture deliberately *includes* `full_ref` in the payload so the test proves it is dropped by the contract, not merely absent from the input
- [x] `PublicEvent` contributes no per-type fields (documented as an empty payload) yet still produces envelope + common - **closed 2026-08-19** (agent-authored, task level `deliver`) - declared explicitly as `PublicEvent: {}` rather than left undeclared. Output is identical either way, but the explicit entry distinguishes "mapped, known to be empty" from "never heard of this type"
- [x] `IssuesEvent`'s `assignee_login` is null when `payload.assignee` is absent - **closed 2026-08-19** (agent-authored, task level `deliver`) - note the real path is `payload.issue.assignee.login`, not `payload.assignee`; `assignees` plucks logins via `take: login`
- [x] `GollumEvent.pages` is a list of curated page objects with `html_url` dropped - **closed 2026-08-19** (agent-authored, task level `deliver`) - the only field in the whole contract needing the `expression:` escape hatch: `take:` plucks a single key, so a list of multi-key objects requires a JMESPath multiselect hash. Applies `K-12`'s established precedent (`E-10`: use the escape hatch rather than growing the friendly vocabulary for one shape)
- [x] Unit tests use hand-built fixtures matching the documented shapes (no real sample exists for these seven) - **closed 2026-08-19** (agent-authored, task level `deliver`) - `DOC_SHAPED_GITHUB_EVENTS` in `tests/fixtures/events.py`, 7 events built from the docs' shapes
- [x] Gate check passes: `make test` - 161 passed, 16 subtests (2026-08-19), no regression

**Tests**: unit
**Gate**: full

---

### T15: `NormalizationFlatMapFunction` (Flink `FlatMapFunction`)

**What**: `NormalizationFlatMapFunction(NormalizationFlatMapFunctionBase)` with `flat_map(self, value: str) -> Iterator[str]`: parses/validates `value` as a `RawEvent` (log + yield nothing on bad JSON, missing field, or `schema_version != 1`), delegates to `use_case.FlinkNormalizationPipeline` (log + yield nothing when no contract exists for that source), `json.dumps` the result, yields it. Adds `apache-flink` to `requirements/flink.txt`.
**Where**: `flink/normalization/adapters/function.py` (relocated 2026-08-19 - originally planned as a single `job.py`; `NormalizationFlatMapFunctionBase` port lives in `flink/normalization/ports.py`)
**Depends on**: T11
**Reuses**: `RawEvent.model_validate_json`; the log-and-skip pattern from `ingestion/adapters/engine.py`'s `_format_events`; `use_case.FlinkNormalizationPipeline` (T11's relocation)
**Requirement**: FLK-11

**Status note (updated 2026-08-21 by `/mentor-tasks` sync, second pass - supersedes both notes below)**: since the first 2026-08-21 sync (same day), `flat_map` was rewritten again and is now fully sound - it catches `ValidationError` (malformed JSON/missing field) and `NotImplementedError` (unmapped source) separately, both via `self.logger.warning` (the earlier `print()`-based version is gone), plus a generic `except Exception: self.logger.exception(...)`. A dedicated test file now exists (`tests/flink/normalization/adapters/test_function.py`, 6 tests, written this session per `AD-002`'s explicit-request/already-functional-code exception) and the exact chain was proven live in a real Flink job during T19's smoke test, not just mocked. The one remaining real gap: `schema_version != 1` is still not rejected (see below).

**Status note (2026-08-21, first pass, stale)**: `flat_map` has a real body now (`flink/normalization/adapters/function.py`), not a stub - parses JSON, builds `RawEvent`, delegates to `FlinkNormalizationPipeline`, yields the serialized result. But it is untested (no `tests/flink/normalization/adapters/test_function.py` exists) and a read-only source check found three of the five behavioral criteria below still unmet - see per-item notes.

**Status note (2026-08-19, stale)**: `flat_map` currently exists only as an empty stub (docstring, no body) - none of the done-when items below are met yet. This is `paired`-level work (`map.md`, K-03) - the flat_map-over-map decision needs to be made/defended before the mechanical parse/delegate/yield body is written.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `apache-flink` added to `requirements/flink.txt` (`design.md` flags as unverified whether this needs a JVM at plain-import time - budget time to confirm during this task) - verified 2026-08-21: `apache-flink==2.3.0` pinned in `requirements/flink.txt`
- [x] A valid `RawEvent` JSON yields exactly one normalized JSON string - verified 2026-08-21 two ways: `test_can_yield_the_normalized_event_as_json` (mocked) and live, via T19's smoke test (a hand-produced `RawEvent` on `events-raw` came back correctly normalized on `events-normalized` through the real running Flink job)
- [~] Malformed JSON, a missing envelope field, and `schema_version != 1` each yield nothing and log (FLK-11, spec P1 AC6) - **partially fixed 2026-08-21**: malformed JSON and a missing field are both now handled and logged via `self.logger.warning` (verified by `test_can_discard_malformed_json_without_calling_the_normalizer`/`test_can_discard_a_raw_event_missing_a_required_field`). `schema_version != 1` is still **not** rejected - `shared.models.RawEvent.schema_version` (`shared/models.py:12`) remains a bare `int` with no `== 1` constraint, so a message with `schema_version: 2` still passes validation and gets normalized instead of skipped. This one sub-case stays open
- [x] A `RawEvent` whose `source` has no contract yields nothing and logs a warning (`design.md`'s closed gap) - **fixed 2026-08-21**: `flat_map` now has a dedicated `except NotImplementedError` branch around `self._normalizer.normalize(...)`, logging a warning and returning nothing - verified by `test_can_discard_an_event_from_an_unknown_source`
- [x] `flat_map` is callable directly as a plain Python method in tests - no live cluster - true by construction, still applies
- [x] Gate check passes: `make test` - `tests/flink/normalization/adapters/test_function.py` (6 tests) now exists and passes; full `tests/flink/` tree: 87 passed, 11 subtests, no collection errors

**Tests**: unit
**Gate**: full - closes Phase 4

---

### T16: Job wiring (`app.py`)

**What**: Builds the `StreamExecutionEnvironment`, wires `KafkaSource(events-raw, consumer group flink-normalization) → NormalizationFlatMapFunction → KafkaSink(events-normalized, keyed by partition_key)`, calls `env.execute()`. Entrypoint for Application Mode (`standalone-job -py app.py`).
**Where**: `flink/normalization/app.py`; source/sink adapters in `flink/normalization/adapters/source.py` (`NormalizationKafkaSource`) and `adapters/sink.py` (`NormalizationKafkaSink`), implementing `NormalizationSourceBase`/`NormalizationSinkBase` (`flink/normalization/ports.py`)
**Depends on**: T15
**Reuses**: `.env`/`load_dotenv()` + `KAFKA_BOOTSTRAP_SERVERS` convention from `ingestion/app.py`
**Requirement**: FLK-06, FLK-08

**Status note (2026-08-24, sink key fix - closes this task's one remaining gap)**: the critical open item below (key extractor unwired) is fixed on branch `fix/flink-normalization-kafka-sink-key`. `flink/common/adapters/sink.py` gained `JsonFieldSerializationSchema(SerializationSchema)` - a generic (not `NormalizedEvent`-aware, deliberately kept out of `flink/common`'s domain-neutral layer) JSON-field extractor: `serialize(self, element)` parses `element`, looks up `self._key_field`, returns `.encode()`d bytes. The field name is injected, not hardcoded - `KafkaSinkParams` (`flink/common/models.py`) gained a `key_field: str`, `KafkaSinkAdapter.build()` passes `self._params.key_field` into the constructor, and `flink/normalization/app.py` sets `key_field="partition_key"` when building its `KafkaSinkParams` - the only place that needs to know the domain field name. `own`-level task (user-authored, agent-reviewed across several rounds - first pass called `.serialize()` immediately instead of passing the instance, an early sketch put `key_field` on `NormalizedEvent` import inside `flink/common` before the layering issue was caught). Tests added under `AD-002` (agent-authored, explicit request, already-functional code): `tests/flink/common/adapters/test_sink.py` gained `test_can_wire_the_key_serializer_with_the_configured_key_field` (asserts `set_key_serialization_schema` receives a `JsonFieldSerializationSchema` built from `self.params.key_field`) and `TestJsonFieldSerializationSchemaSerialize` (extract+encode happy path, `KeyError` on a missing field - documented as current behavior, not yet aligned with `flat_map`'s log-and-skip pattern). `make test` (`tests/flink`): 94 passed, 11 subtests, no regression. **Not yet done**: a live re-run of T19's smoke test against this fix - see T19's status note below, the 2026-08-21 "correctly-keyed" verification predates this fix and almost certainly only checked the JSON payload's `partition_key` field, not the physical Kafka record key.

**Status note (updated 2026-08-21 by `/mentor-tasks` sync, second pass)**: `app.py` was rewritten again since the first 2026-08-21 note below - the crash it flagged is gone. It now builds a `YamlContractRepository` + `EventNormalizer` once at startup and passes them into `NormalizationFlatMapFunction(normalizer=normalizer)` correctly. Source/sink construction moved off the old `adapters/source.py`/`adapters/sink.py` stubs entirely - those files (and the `NormalizationSourceBase`/`NormalizationSinkBase` ports this task originally named) are gone; `app.py` now calls `KafkaFactory.create_source(...)`/`create_sink(...)` from the new `flink/common/` package instead (`KafkaSourceAdapter`/`KafkaSinkAdapter` behind `EventSourcePort`/`EventSinkPort`, tested this session in `tests/flink/common/`). T19's live smoke test proved this wiring actually runs: the job registered, discovered `events-raw`'s partitions, and produced correct output on `events-normalized`.

**Status note (2026-08-21, first pass, stale)**: `app.py` now really wires `StreamExecutionEnvironment` → `KafkaSource` → `flat_map` → `KafkaSink`, guarded behind `if __name__ == "__main__":` - it's no longer the manual `event_sample.json` harness. `adapters/source.py`/`adapters/sink.py` (`NormalizationKafkaSource`/`NormalizationKafkaSink`) are still empty stubs and are not actually used by `app.py` - the wiring builds `KafkaSource`/`KafkaSink` directly with the PyFlink builders instead. A crash-level gap was found on read: `app.py:63` instantiates `NormalizationFlatMapFunction()` with no arguments, but `function.py`'s `__init__` requires `event_evaluator` - this would raise `TypeError` the moment `app.py` actually runs.

**Status note (2026-08-19, stale)**: `adapters/source.py`/`adapters/sink.py` currently exist only as empty stubs, and `app.py` today is a manual harness (reads local `event_sample.json`, prints results) rather than real `StreamExecutionEnvironment` wiring. Neither the port method shapes (`NormalizationSourceBase.initialize/invoke/flush`, `NormalizationSinkBase.open/run/cancel`) have been verified against the real PyFlink `SourceFunction`/`SinkFunction`/`KafkaSource`/`KafkaSink` API - this needs research before the mechanical body is written. `paired`-level (`map.md`, K-04 Application vs Session Mode; K-08 pip vs JAR belongs to T17).

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Config/wiring functions (building source/sink configs from env vars) are unit-tested with mocks, mirroring `tests/ingestion/test_app.py`'s style - **closed 2026-08-21** (agent-authored under `AD-002`, explicit request, already-functional code): `tests/flink/normalization/test_app.py` (4 tests) now exists, exercising `app.py`'s `if __name__` block directly via `runpy.run_module(..., run_name="__main__")` with every external dependency mocked - covers the missing-env-var `KeyError`, source/sink param construction, normalizer/contract-repository wiring, and the full `from_source → flat_map → sink_to → execute` chain. `flink/normalization/app.py`: 0% → 100% coverage
- [x] `KafkaSink`'s key extractor is wired to `partition_key` - **fixed 2026-08-24** (see status note above): `KafkaSinkAdapter.build()` now calls `set_key_serialization_schema(JsonFieldSerializationSchema(self._params.key_field))`, `app.py` sets `key_field="partition_key"`. Proven by `test_can_wire_the_key_serializer_with_the_configured_key_field` + `TestJsonFieldSerializationSchemaSerialize` (unit-level, mocked/direct). **Still open**: a live `docker compose` run re-confirming the physical Kafka record key (not just the JSON payload's `partition_key` field) is set on a real message - T19's 2026-08-21 checkbox predates this fix and doesn't cover it, see T19's note below
- [x] Job does not call `env.execute()` at import time (importable/testable without starting a real pipeline) - the whole wiring block is under `if __name__ == "__main__":`
- [x] Contracts are loaded once at startup, not per record (`design.md` Tech Decisions) - re-verified 2026-08-21 against the current code: `app.py` builds one `YamlContractRepository` instance at startup and threads it through `EventNormalizer` into the single long-lived `NormalizationFlatMapFunction` - its instance-dict cache (`adapters/contract_repository.py`) is populated once per source on first use and reused for every subsequent record, not reloaded per record
- [x] Gate check passes: `make test` - closed alongside the first item above: `tests/flink/normalization/test_app.py` passes, full suite 185 passed, 99% coverage, no regression

**Tests**: unit
**Gate**: full

---

### T17: Flink Dockerfile

**What**: `FROM flink:2.3.0-scala_2.12-java17`; installs Python 3.12 + `pip install -r requirements/flink.txt` (which carries `jmespath` and, from T15, `apache-flink` - and via `base.txt`, `pydantic`/`PyYAML`; deliberately not `kafka-python`/`requests`, which are ingestion-only); adds the Flink Kafka connector JAR to `/opt/flink/lib`; copies `flink/normalization/` (including `config/*.yml`) and `shared/models.py` into the image's usrlib path.
**Where**: `infra/docker/flink/Dockerfile` (an untracked `flink/normalization/Dockerfile` exists as of 2026-08-19 but does not satisfy this task - see status note)
**Depends on**: T16
**Reuses**: Verified Docker Hub tag (`flink:2.3.0-scala_2.12-java17`) from `design.md` research; `ingestion/Dockerfile`'s shape
**Requirement**: FLK-04

**Status note (updated 2026-08-21 by `/mentor-tasks` sync - supersedes the 2026-08-19 note below, which is now stale)**: `flink/normalization/Dockerfile` was rewritten since - it is now `FROM flink:2.3.0-scala_2.12-java17`, installs Python 3 via `apt-get`, downloads `flink-sql-connector-kafka-5.0.0-2.2.jar` straight into `/opt/flink/lib`, copies `flink/common/`+`flink/normalization/`+`shared/` (not `ingestion/`), and its `CMD` runs `standalone-job -py .../app.py`. All 5 done-when items below verified live during T19's smoke test.

**Status note (2026-08-19, stale)**: the current `flink/normalization/Dockerfile` is `FROM python:3.12-slim` and only `pip install`s `requirements/flink.txt`, then runs `python -m flink.normalization.app` - no JVM, no Flink binaries, no Kafka connector JAR. It does not build a real Flink cluster image; none of the done-when items below are met.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Image builds successfully - verified 2026-08-21: `docker compose up -d --build` built `docker-normalization` cleanly
- [x] `python3 -c "import pyflink, jmespath"` succeeds inside the built image - verified 2026-08-21: `docker exec normalization python3 -c "import pyflink, jmespath; print('OK')"` → `OK`
- [x] The Kafka connector JAR is present under `/opt/flink/lib` - confirmed via Dockerfile (`curl -fL -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar ...`) and live: the job actually connected to Kafka and consumed/produced through it
- [x] `flink/normalization/sources/*.yml` is present in the image - the job cannot start without its contracts (`design.md` Risks) - confirmed live: the running job correctly normalized a `WatchEvent` per `github.yml`'s contract, which only works if the YAML shipped in the image
- [x] The image does NOT copy `ingestion/` - the contract design removed that cross-package dependency - confirmed by reading the Dockerfile: only `flink/common/`, `flink/normalization/`, `shared/` are copied

**Tests**: none
**Gate**: build

---

### T18: Wire Flink cluster into docker-compose (Application Mode)

**What**: Add `flink-jobmanager` (command: `standalone-job -py /opt/flink/usrlib/app.py`) and `flink-taskmanager` services using T17's image, Flink Web UI exposed on the host, `depends_on: topic-init` with `condition: service_completed_successfully` (matching the fix already applied to the `ingestion` service).
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T17
**Reuses**: The `depends_on`/`environment` conventions now established by the `ingestion` service, including the single-broker `KAFKA_BOOTSTRAP_SERVERS: broker-1:19092` form
**Requirement**: FLK-04, FLK-05

**Status note (updated 2026-08-21 by `/mentor-tasks` sync - supersedes the 2026-08-19 note below, which is now stale)**: `infra/docker/docker-compose.yml` was rewritten since - the `template` placeholders are gone. The JobManager role is now the `normalization` service itself (`command` defaults to the Dockerfile's `standalone-job -py .../app.py` CMD, `FLINK_PROPERTIES` sets `jobmanager.rpc.address: normalization`), and a separate `taskmanager` service (`command: taskmanager`) points back at it. Both `depends_on: topic-init: condition: service_completed_successfully`. All 5 done-when items below verified live during T19's smoke test.

**Status note (2026-08-19, stale)**: `infra/docker/docker-compose.yml` currently has `flink-jobmanager`/`flink-taskmanager` entries whose body is the literal placeholder text `template` - not valid YAML service definitions (`docker compose config` fails on this file today). None of the done-when items below are met.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] WHEN `docker compose up` runs THEN JobManager + TaskManager start and the Flink Web UI is reachable from the host - verified 2026-08-21: `normalization`/`flink-taskmanager` both `Up`, `curl localhost:8081/jobs` reachable from the host
- [x] The job shows as running in the Flink Web UI without a manual submission step - verified 2026-08-21 via REST: `state: RUNNING`, job auto-submitted by the container's own `standalone-job` CMD, no manual step
- [x] Topic provisioning completes before the Flink services start (spec P1 Infra AC5) - confirmed in the `docker compose up` log: `topic-init Exited` appears before `normalization Starting`, matching the `condition: service_completed_successfully` dependency
- [x] `docker compose -f infra/docker/docker-compose.yml config` parses cleanly - verified 2026-08-21: exit 0, no errors
- [x] Flagged risk from `design.md` (exact Application Mode CLI flags for a Python entrypoint): budget for iteration here if the job doesn't submit on the first attempt - resolved: `standalone-job -py /opt/flink/usrlib/flink/normalization/app.py` submitted and reached `RUNNING` on the first attempt, no iteration needed

**Tests**: none
**Gate**: build

---

### T19: End-to-end infra + normalization smoke test

**What**: Full `docker compose up` run: confirm both topics provisioned, ingestion publishing to `events-raw`, Flink job running and consuming, and `events-normalized` receiving correctly-shaped, correctly-keyed messages for real GitHub traffic. Closes every P1 Independent Test in `spec.md`.
**Where**: N/A (verification task, no new source file)
**Depends on**: T18
**Reuses**: Kafka UI (topic inspection), Flink Web UI (job status)
**Requirement**: FLK-01, FLK-02, FLK-03, FLK-04, FLK-05, FLK-06, FLK-07, FLK-08, FLK-09, FLK-10, FLK-11, FLK-12

**Status note (2026-08-24, re-open flag - not yet acted on)**: T16's sink key bug (see T16's 2026-08-24 status note) means the "keyed by `repo_name`" claim in the third `Done when` item below was verified on 2026-08-21 against a sink that had **no key serializer wired at all**. That run almost certainly only confirmed the JSON payload's `partition_key` field, not the physical Kafka record key - Kafka UI shows a message's key separately from its value, and nothing in that day's notes mentions checking it. The checkbox is left `[x]` here (not unchecked) because the *shape* half of that claim - envelope/common/`partition_key` field all correct - is still genuinely proven. What's unproven is the keying half. This needs a fresh `docker compose up` run against the fixed sink, inspecting the actual Kafka record key in Kafka UI (not the payload), before FLK-08's Independent Test can be called closed. Not done in this pass - no Docker available in this environment.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Both topics show the configured partitions/replication/retention in Kafka UI - verified 2026-08-21: `docker compose up -d --build` from `infra/docker/`, `topic-init` exited 0, `kafka-get-offsets.sh` against `broker-1:19092` shows 3 partitions each for `events-raw`/`events-normalized` (replication/retention were already verified live in T1/T2)
- [~] Ingestion container publishes without manual invocation - **partially verified 2026-08-21**: the `ingestion` container does start and poll on its own (confirmed in its logs), but it hit GitHub's public-API rate limit on its very first call (reset ~18:40 UTC that day, likely the sandbox's IP already near its unauthenticated quota) and produced zero organic messages during this smoke test. Mechanically unattended, but not proven with real traffic end to end - known P2 gap (`streaming-ingestion/spec.md`'s rate-limit handling), not a regression from this feature. Substituted with 2 hand-produced `RawEvent` JSON messages via `kafka-console-producer` (see items below) to still validate the rest of the chain without waiting out the rate limit
- [x] Flink Web UI shows the job running - verified 2026-08-21 via REST (`curl localhost:8081/jobs/overview`): `normalization-job`, `state: RUNNING`, 1/1 tasks running, 0 failed; logs confirm the `KafkaSourceEnumerator` discovered `events-raw`'s 3 partitions and the `normalization-consumer-group` consumer was assigned to them
- [x] `events-normalized` messages inspected via Kafka UI match the Normalization Mapping shape for at least one real event of a mapped type, and are keyed by `repo_name` - verified 2026-08-21: a hand-produced `WatchEvent` `RawEvent` on `events-raw` came back on `events-normalized` with the full envelope+common+`action` field, `partition_key` = `"octo/repo"` (the event's `repo.name`) - matches `spec.md`'s Normalization Mapping exactly, consumed via `kafka-console-consumer`. **Caveat added 2026-08-24** (see status note above): this predates the sink key fix, so "keyed by `repo_name`" here almost certainly means the payload field, not the physical Kafka record key - needs a live re-check
- [x] At least one event of a not-yet-mapped type appears with envelope + common populated and an empty per-type block, confirming it was not dropped (spec P1 AC5 in production, not just unit tests) - verified 2026-08-21: a hand-produced `PushEvent` (undeclared in `github.yml`) came back with envelope+common populated (`org_id`/`org_login` correctly null - the synthetic payload had no `org`), no per-type fields - confirms AC5's fallback live, not just in `test_contracts_github.py`

**Note (2026-08-21)**: the two `events-raw` messages behind the last two checks above were hand-produced JSON, not real GitHub traffic (`ingestion` never got a live message out during this run - see the rate-limit item above). They exercise the exact same code path a real GitHub event would (same `RawEvent` envelope, same contract), so the normalization behavior itself is genuinely proven; only the "captured from live GitHub traffic" literal wording of this task's `What` isn't satisfied yet. Worth a follow-up run after the rate-limit window clears, but not blocking - the pipeline mechanics are confirmed working.

**Tests**: none
**Gate**: build (manual, per `spec.md`'s P1 Independent Tests) - closes Phase 5

---

### T20: Capture real-traffic sample for the 6 remaining event types

**What**: Run the ingestion service (or a throwaway script against the GitHub Events API) long enough to capture at least one real instance of `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, and - best-effort - `SponsorshipEvent`. Mirrors the process that produced the original local capture behind the 4 verified types.
**Where**: `tmp/event_sample_extended.json` (a local capture - untracked, like every `*_sample.*`); whatever the tests need gets inlined into `tests/fixtures/events.py`
**Depends on**: T19
**Reuses**: The same capture method behind the original local capture
**Requirement**: FLK-13

**Status note (2026-08-24, closed - capture aborted, all 6 deferred by explicit user decision)**: `tmp/capture_extended_events.py` (throwaway script, untracked) polled the public GitHub Events API for ~18 minutes. Stopped deliberately before any of the 6 types were captured - continued polling wasn't worth the cost against how rare some of these types are on the public feed. Rather than fall back to documentation (the exact failure mode P2 exists to avoid), the user chose to extend AC4's `SponsorshipEvent`-only escape valve to all 6 types. See `spec.md`'s P2 closing note for the full record.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Sample file contains at least one real instance of each of the 5 reliably-occurring types - **not met, accepted**: capture aborted at zero results, no retry planned
- [x] `SponsorshipEvent` is either captured or explicitly documented as not observed within the window (spec P2 AC4's escape valve) - documented as not observed; escape valve extended to all 6 types (see status note)
- [x] The observation window and method are noted, so the result is reproducible - `tmp/capture_extended_events.py`'s own output records `started_at`/`finished_at`/method; this task's status note above carries the same record since the script never reached its own write step

**Tests**: none
**Gate**: build (manual)
**Status**: Closed 2026-08-24 - capture accepted as unsuccessful, not fixed/retried (see status note); not a false-done checkbox.

---

### T21: Contract entries for `PushEvent`, `ForkEvent`, `ReleaseEvent`

**What**: Derive the curated field mapping for these three from T20's real sample, add the rows to `spec.md`'s Normalization Mapping table, and add the corresponding `event_types` entries to the GitHub contract.
**Where**: `flink/normalization/config/sources/github.yml`, `.specs/features/flink-normalization/spec.md`
**Depends on**: T20
**Reuses**: The noise-filtering convention already fixed for the 11 mapped types (drop `avatar_url`, `gravatar_id`, redundant `*_url`)
**Requirement**: FLK-14, FLK-15

**Status note (2026-08-24, closed - not applicable)**: T20 captured no real sample for any of these three, and the user explicitly declined to map from documentation instead (the same unreliable-doc problem P2 exists to avoid). No `event_types` entries added; all three stay on the P1 AC5 fallback path.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] Each type's mapping is derived from the captured sample, not from documentation (the whole reason these six were deferred - `context.md`) - **satisfied by not mapping**: no sample existed, and mapping from documentation was explicitly declined rather than done anyway
- [x] `spec.md`'s Normalization Mapping table gains a row per type, and the "Not yet mapped" list shrinks accordingly - **not applicable, accepted**: no rows added; "Not yet mapped" list stays as-is by design (see `spec.md` P2 closing note)
- [x] Each type stops falling into the empty-fallback path and produces populated per-type fields (spec P2 AC3) - **not applicable, accepted**: all three deliberately stay on the fallback path
- [x] Unit tests use the captured real fixtures, driven through `NormalizationEngine` - **not applicable**: no fixtures exist to test against
- [x] Gate check passes: `make test` - unaffected, no code changed; full suite still green (185 passed)

**Tests**: unit
**Gate**: full
**Status**: Closed 2026-08-24 as not-applicable per the accepted P2 scope reduction - not a false-done checkbox, see T20's status note for the decision record.

---

### T22: Contract entries for `DiscussionEvent`, `CommitCommentEvent`, `SponsorshipEvent`

**What**: Same as T21 for the remaining three. `SponsorshipEvent` is conditional: if T20 did not capture one, it stays on the empty-envelope fallback and is documented as a known gap rather than blocking the story (spec P2 AC4).
**Where**: `flink/normalization/config/sources/github.yml`, `.specs/features/flink-normalization/spec.md`
**Depends on**: T21
**Reuses**: Same convention as T21
**Requirement**: FLK-14, FLK-15, FLK-16

**Status note (2026-08-24, closed - not applicable, same decision as T21)**: none of the three were captured either; all stay on the fallback path, `SponsorshipEvent`'s own AC4 escape valve now covers all 6 P2 types by explicit user decision.

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [x] `DiscussionEvent` and `CommitCommentEvent` mapped from captured samples, with `spec.md` rows added - **not applicable, accepted**: no samples existed, no rows added
- [x] `SponsorshipEvent` is either mapped from a real sample, or explicitly recorded in `spec.md` as a known gap on the fallback path (spec P2 AC4) - both are valid completions - **the second option, taken**: recorded in `spec.md`'s Edge Cases and P2 closing note
- [x] Unit tests use captured real fixtures for whatever was mapped - **not applicable**: nothing was mapped
- [x] `spec.md`'s Requirement Traceability updated for FLK-13 through FLK-16 - done: all 4 rows updated to `Verified` with the accepted-gap explanation
- [x] Gate check passes: `make test` - unaffected, no code changed; full suite still green (185 passed)

**Tests**: unit
**Gate**: full - closes Phase 6
**Status**: Closed 2026-08-24 as not-applicable per the accepted P2 scope reduction - closes Phase 6 and the feature's task list.

---

## Phase Execution Map

Visual representation of task ordering. Phases run in sequence, and tasks within a phase run in order. Every `Depends on` edge from the Task Breakdown is drawn explicitly below (including cross-phase ones), so this diagram is the single authoritative dependency graph for the whole feature.

```
Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6

Phase 1:
T1 -> T2

Phase 2:
T3 -> T4
T2 -> T4

Phase 3:
T5 -> T6
T6 -> T7
T6 -> T8
T7 -> T9
T8 -> T9
T9 -> T10
T10 -> T11

Phase 4:
T11 -> T12
T12 -> T13
T13 -> T14
T11 -> T15

Phase 5:
T15 -> T16
T16 -> T17
T17 -> T18
T18 -> T19

Phase 6:
T19 -> T20
T20 -> T21
T21 -> T22
```

Execution is strictly sequential - there is no intra-phase parallelism. A single agent (or batch worker) works one task at a time, in order. Edges that cross a phase boundary (e.g. `T5 -> T6`, `T11 -> T12`) are drawn the same as intra-phase edges - they're satisfied by phase ordering at execution time, but shown here for a complete, checkable dependency graph.

Note the Phase 3 shape: `T7` (grammar) and `T8` (custom function) both feed `T9` (compiler), because
the compiler must emit a call to the function it was given and validate against the grammar it targets.
`T15` (`NormalizationFunction`) depends on `T11` (`NormalizationEngine`) rather than on the GitHub
contract - it is source-agnostic and testable before any contract exists, which is the whole point of
`AD-006`.

**The orchestrating agent's role during Execute:**
1. Count total tasks and pack phases into ~7-task batches - offer batch sub-agents if that yields more than one batch and the user accepts
2. Dispatch the next batch (to a worker, or execute inline)
3. Receive the compact batch summary
4. Update tasks.md with results
5. If the batch summary shows all tasks complete: proceed to the next batch
6. If a task failed: decide fix/escalate before dispatching the next batch

> **Mentor-mode override (`AD-001`)**: sub-agent batching does not apply to this feature - the user
> implements every task themselves. The packing model above is retained for reference only.

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: topic-creation script | 1 file | ✅ Granular |
| T2: wire topic-init service | 1 file (compose edit) | ✅ Granular |
| T3: ingestion Dockerfile | 1 file | ✅ Granular |
| T4: wire ingestion service | 1 file (compose edit) | ✅ Granular |
| T5: `NormalizationEngineBase` port | 1 interface | ✅ Granular |
| T6: flink package test wiring | 3 config files, one cohesive purpose | ⚠️ OK - cohesive (none of the three is verifiable alone; together they are "the flink package is now covered by the existing tooling") |
| T7: contract grammar | 2 related Pydantic models, 1 file | ✅ Granular |
| T8: `iso_to_millis` function | 1 function | ✅ Granular |
| T9: contract compiler | 1 function | ✅ Granular |
| T10: contract loader | 1 function | ✅ Granular |
| T11: `NormalizationEngine` | 1 class | ✅ Granular |
| T12: GitHub contract shared blocks | 1 file (3 cohesive blocks of the same contract) | ✅ Granular |
| T13: GitHub contract, 4 verified types | 1 file (same contract, additive) | ✅ Granular |
| T14: GitHub contract, 7 doc types | 1 file (same contract, additive) | ✅ Granular |
| T15: `NormalizationFunction` | 1 class | ✅ Granular |
| T16: job wiring | 1 file | ✅ Granular |
| T17: Flink Dockerfile | 1 file | ✅ Granular |
| T18: wire Flink cluster | 1 file (compose edit) | ✅ Granular |
| T19: e2e smoke test | 1 verification pass | ✅ Granular |
| T20: capture sample | 1 data artifact | ✅ Granular |
| T21: contract entries for 3 types | 1 contract file + spec rows (documentation, not a second code file) | ✅ Granular |
| T22: contract entries for 3 types | 1 contract file + spec rows | ✅ Granular |

---

## Diagram-Definition Cross-Check

| Task | Depends On (task body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | - | ✅ Match |
| T2 | T1 | T1 → T2 | ✅ Match |
| T3 | None | - | ✅ Match |
| T4 | T2, T3 | T2 → T4, T3 → T4 | ✅ Match |
| T5 | None | - | ✅ Match |
| T6 | T5 | T5 → T6 | ✅ Match |
| T7 | T6 | T6 → T7 | ✅ Match |
| T8 | T6 | T6 → T8 | ✅ Match |
| T9 | T7, T8 | T7 → T9, T8 → T9 | ✅ Match |
| T10 | T9 | T9 → T10 | ✅ Match |
| T11 | T10 | T10 → T11 | ✅ Match |
| T12 | T11 | T11 → T12 | ✅ Match |
| T13 | T12 | T12 → T13 | ✅ Match |
| T14 | T13 | T13 → T14 | ✅ Match |
| T15 | T11 | T11 → T15 | ✅ Match |
| T16 | T15 | T15 → T16 | ✅ Match |
| T17 | T16 | T16 → T17 | ✅ Match |
| T18 | T17 | T17 → T18 | ✅ Match |
| T19 | T18 | T18 → T19 | ✅ Match |
| T20 | T19 | T19 → T20 | ✅ Match |
| T21 | T20 | T20 → T21 | ✅ Match |
| T22 | T21 | T21 → T22 | ✅ Match |

No task depends on a later-phase task. All dependencies point backward or within the same phase.

---

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1 | Docker/shell infra | none | none | ✅ OK |
| T2 | Docker/shell infra | none | none | ✅ OK |
| T3 | Docker/shell infra | none | none | ✅ OK |
| T4 | Docker/shell infra | none | none | ✅ OK |
| T5 | `NormalizationEngineBase` (ABC) | none | none | ✅ OK |
| T6 | Build/test config only, no code layer | none | none | ✅ OK |
| T7 | Contract models | unit | unit | ✅ OK |
| T8 | Custom JMESPath functions | unit | unit | ✅ OK |
| T9 | Contract compiler | unit | unit | ✅ OK |
| T10 | Contract loader | unit | unit | ✅ OK |
| T11 | `NormalizationEngine` | unit | unit | ✅ OK |
| T12 | Normalization contracts (`config/*.yml`) | unit | unit | ✅ OK |
| T13 | Normalization contracts | unit | unit | ✅ OK |
| T14 | Normalization contracts | unit | unit | ✅ OK |
| T15 | `NormalizationFunction` | unit | unit | ✅ OK |
| T16 | Job wiring (`app.py`) | unit | unit | ✅ OK |
| T17 | Docker infra | none | none | ✅ OK |
| T18 | Docker/compose infra | none | none | ✅ OK |
| T19 | Verification only, no new layer | none | none | ✅ OK |
| T20 | Data artifact, no new layer | none | none | ✅ OK |
| T21 | Normalization contracts | unit | unit | ✅ OK |
| T22 | Normalization contracts | unit | unit | ✅ OK |

No violations. `Tests: none` is only used for layers the matrix marks `none` (infra/config/verification/data-artifact tasks).

Note on T12-T14 and T21-T22: these tasks author YAML, not Python, yet they carry `Tests: unit`
deliberately. A contract is data whose *effect* is behaviour, and an untested contract entry is
exactly the silent-null-column failure mode `design.md` flags as this design's main risk. The tests
assert the normalized output against real fixtures, driven through `NormalizationEngine`.

---

## Tips reference

See `.claude/skills/tlc-spec-driven/references/tasks.md` for the full granularity/dependency/co-location rules applied above.
