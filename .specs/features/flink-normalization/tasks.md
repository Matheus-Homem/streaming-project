# Flink Normalization Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is the source of truth for the full flow (per-task cycle, sub-agent delegation, adequacy review, Verifier, discrimination sensor).

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

**Project-specific override (`AD-001`/`AD-003`, `CLAUDE.md`): mentor mode governs Execute for this feature.** The agent never authors production code for these tasks - it explains, points at the approach, reviews what the user writes, and runs the gate. The agent never commits (`AD-003`) - it hands off a diff + suggested Conventional Commit message per task instead.

---

**Design**: `.specs/features/flink-normalization/design.md`
**Status**: Draft

---

## Test Coverage Matrix

> Generated from codebase sampling (`tests/ingestion/`) and `spec.md`/`design.md` - confirm before Execute. Guidelines found: no `AGENTS.md`/`CONTRIBUTING.md`; inferred from existing test depth in `tests/ingestion/adapters/test_engine.py`, `tests/ingestion/test_app.py`, `tests/ingestion/test_models.py` (unittest.TestCase + unittest.mock, one file per source module, full-branch coverage including log-and-skip paths).

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| `NormalizerBase` (ABC, `ports.py`) | none | Interface only, no logic - matches the untested-ABC precedent of `ingestion/ports.py` | - | build gate only |
| Contract models (Pydantic grammar, `models.py`) | unit | All validation branches. A contract with an unknown key, a missing `from`/`expression`, or an unsupported `as:` value MUST fail validation - this is the platform's only defence against a silently-null column (`design.md` Risks) | `tests/flink/normalization/test_models.py` | `make test` |
| Contract compiler (`models.py`) | unit | 1:1 to every row of `design.md`'s compilation-rule table (`from`, `take`, `as: boolean`, `as: timestamp`, `default`, raw `expression`) | `tests/flink/normalization/test_models.py` | `make test` |
| Contract loader (`models.py`) | unit | Happy path + unknown source raises + caching behaviour - mirrors `tests/ingestion/test_models.py`'s coverage of `get_source_config` | `tests/flink/normalization/test_models.py` | `make test` |
| Custom JMESPath functions (`functions.py`) | unit | All branches incl. malformed/absent input - it is the one piece of real computation in the contract path | `tests/flink/normalization/test_functions.py` | `make test` |
| `ContractNormalizer` (domain logic, `adapters/contract_normalizer.py`) | unit | All branches; 1:1 to `spec.md`'s P1 Normalization ACs - envelope shape, common block, per-type block, unmapped-type fallback (AC5), absent optional field → null | `tests/flink/normalization/adapters/test_contract_normalizer.py` | `make test` |
| Normalization contracts (`config/*.yml`) | unit | The contract is data, but its *effect* is behaviour: each declared event type must produce exactly its `spec.md` Normalization Mapping row when run against a real fixture. Verified through `ContractNormalizer`, not by asserting on the YAML | `tests/flink/normalization/test_contracts_github.py` | `make test` |
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

### T5: `NormalizerBase` port

**What**: Abstract `NormalizerBase` class with one abstract method, `normalize(self, event: RawEvent) -> dict[str, Any]`, docstring-documented like `ingestion/ports.py`'s existing ABCs.
**Where**: `flink/normalization/ports.py`
**Depends on**: None
**Reuses**: `ingestion/ports.py`'s ABC shape (`IngestionEngineBase`, etc.) as the direct template
**Requirement**: FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] `NormalizerBase` defined with the `normalize` abstract method matching design.md's signature
- [ ] Docstring documents args/return per design.md (never raises for a structurally valid `RawEvent`; never returns `None`)

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
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_functions.py` - 8 passed; full `make test` also verified (112 passed, `flink/normalization/functions.py` 100% coverage, no regression)

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
- [x] Unit tests: one per row above, asserting the compiled expression AND that it evaluates correctly against a real event fixture - not just string equality, which would pass on a syntactically valid but wrong expression. Fixture note: `tmp/event_sample.json` has moved to `flink/normalization/event_sample.json` (test resolves it via `Path(__file__).parents[3]`)
- [x] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_models.py` - 18 passed; full `make test` also verified (121 passed, `flink/normalization/models.py` 100% coverage, no regression)

**Tests**: unit
**Gate**: quick

---

### T10: Contract loader

**What**: `get_normalization_contract(source: str) -> NormalizationContract` - reads `flink/normalization/config/<source>.yml`, validates it through T7's models, compiles every field rule once via T9, and caches the result. Raises `NotImplementedError` for an unknown source. Directly mirrors `ingestion.models.get_source_config`'s shape.
**Where**: `flink/normalization/models.py`
**Depends on**: T9
**Reuses**: `ingestion/models.py`'s `_load_yaml_config` (`@lru_cache` + `Path(__file__).parent / "config"` + `yaml.safe_load`) as the 1:1 template
**Requirement**: FLK-06, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] A valid contract file loads, validates, and returns compiled rules
- [ ] An unknown source raises `NotImplementedError`, matching `get_source_config`'s convention
- [ ] An invalid contract fails at load time with a message naming the file, event type, and field (`design.md` Error Handling: fail-fast at startup, never silently at runtime)
- [ ] Compilation happens once per source, not per call (`design.md` Tech Decisions) - asserted via caching behaviour
- [ ] Unit tests cover each branch above
- [ ] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_models.py`

**Tests**: unit
**Gate**: quick

---

### T11: `ContractNormalizer`

**What**: The single source-agnostic `NormalizerBase` implementation. Resolves the contract for `event.source`, looks up the per-event-type block for `event.source_event_type` (empty block when undeclared), evaluates envelope + common + per-type rules against `RawEvent.payload`, and returns one flat dict. Contains zero GitHub-specific logic.
**Where**: `flink/normalization/adapters/contract_normalizer.py`
**Depends on**: T10
**Reuses**: `flink/normalization/ports.py`'s `NormalizerBase` (T5), `shared.models.RawEvent`
**Requirement**: FLK-06, FLK-07, FLK-08, FLK-09, FLK-10, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output contains the Domain-Neutral Envelope fields from `spec.md`, with `source`/`event_id`/`event_type`/`ingested_at` taken from the `RawEvent` envelope itself (not the contract) and `schema_version` set to the normalized schema's own version
- [ ] `partition_key` is populated from the contract's `partition_key` rule (FLK-08)
- [ ] An event type absent from the contract's `event_types` still returns envelope + common block with an empty per-type block, never `None` and never raising (FLK-10, spec P1 AC5)
- [ ] A contract path absent from a given payload yields a null field rather than an error (`design.md` Error Handling)
- [ ] No GitHub identifier appears anywhere in this module - verifiable by grep
- [ ] Unit tests cover each branch above using a minimal hand-built contract, independent of the real GitHub contract (which T12-T14 test separately)
- [ ] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/adapters/test_contract_normalizer.py`

**Tests**: unit
**Gate**: quick - closes Phase 3

---

### T12: GitHub contract - shared blocks

**What**: `flink/normalization/config/github.yml` with its `source`, `partition_key`, `envelope`, and `common` blocks per `spec.md`'s Domain-Neutral Envelope and GitHub-specific fields block tables. No `event_types` entries yet (T13, T14).
**Where**: `flink/normalization/config/github.yml`
**Depends on**: T11
**Reuses**: `spec.md`'s Normalization Mapping tables are the authoritative field list - this task transcribes them, it does not re-derive them
**Requirement**: FLK-07, FLK-08

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] `partition_key` resolves to `repo.name` (spec P1 AC3)
- [ ] `envelope` covers `actor_id`, `actor_login`, `event_time` (with `as: timestamp`)
- [ ] `common` covers `repo_id`, `repo_name`, `org_id`, `org_login`, `public`
- [ ] `org_id`/`org_login` come back null for an event whose payload has no `org`, verified against a real fixture (spec.md Edge Cases)
- [ ] Unit tests run a real `tmp/event_sample.json` event through `ContractNormalizer` and assert the envelope + common output, field by field
- [ ] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_contracts_github.py`

**Tests**: unit
**Gate**: quick

---

### T13: GitHub contract - the 4 sample-verified event types

**What**: `event_types` entries for `IssueCommentEvent`, `PullRequestEvent`, `PullRequestReviewEvent`, and `PullRequestReviewCommentEvent` - the four whose field mapping was verified against real traffic.
**Where**: `flink/normalization/config/github.yml`
**Depends on**: T12
**Reuses**: `tmp/event_sample.json` holds a real instance of each of these four; `spec.md`'s per-type curated-field rows are the binding spec
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Each of the four types produces exactly its `spec.md` Normalization Mapping row - field by field, no extras, no omissions
- [ ] `issue_labels` is a list of label names, not label objects (`take: name`)
- [ ] `issue_is_pull_request` is a real boolean reflecting presence/absence of `issue.pull_request`
- [ ] `label_name` is null when `payload.label` is absent
- [ ] Unit tests use the real fixture for each type, driven through `ContractNormalizer`
- [ ] Gate check passes: `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/test_contracts_github.py`

**Tests**: unit
**Gate**: quick

---

### T14: GitHub contract - the 7 doc-resolved event types

**What**: `event_types` entries for `WatchEvent`, `CreateEvent`, `DeleteEvent`, `PublicEvent`, `GollumEvent`, `IssuesEvent`, and `MemberEvent` - mapped from the GitHub Events API docs and cross-checked against sample-verified nested shapes.
**Where**: `flink/normalization/config/github.yml`
**Depends on**: T13
**Reuses**: `spec.md`'s per-type rows; the `issue`/`user`/`label` nested shapes already verified by T13's fixtures
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Each of the seven types produces exactly its `spec.md` Normalization Mapping row
- [ ] `CreateEvent` does NOT emit `full_ref` (deliberately excluded - `spec.md` Assumptions)
- [ ] `PublicEvent` contributes no per-type fields (documented as an empty payload) yet still produces envelope + common
- [ ] `IssuesEvent`'s `assignee_login` is null when `payload.assignee` is absent
- [ ] `GollumEvent.pages` is a list of curated page objects with `html_url` dropped
- [ ] Unit tests use hand-built fixtures matching the documented shapes (no real sample exists for these seven)
- [ ] Gate check passes: `make test`

**Tests**: unit
**Gate**: full

---

### T15: `NormalizationFunction` (Flink `FlatMapFunction`)

**What**: `NormalizationFunction(FlatMapFunction)` with `flat_map(self, value: str) -> Iterator[str]`: parses/validates `value` as a `RawEvent` (log + yield nothing on bad JSON, missing field, or `schema_version != 1`), delegates to `ContractNormalizer` (log + yield nothing when no contract exists for that source), `json.dumps` the result, yields it. Adds `apache-flink` to `requirements/flink.txt`.
**Where**: `flink/normalization/job.py`
**Depends on**: T11
**Reuses**: `RawEvent.model_validate_json`; the log-and-skip pattern from `ingestion/adapters/engine.py`'s `_format_events`
**Requirement**: FLK-11

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] `apache-flink` added to `requirements/flink.txt` (`design.md` flags as unverified whether this needs a JVM at plain-import time - budget time to confirm during this task)
- [ ] A valid `RawEvent` JSON yields exactly one normalized JSON string
- [ ] Malformed JSON, a missing envelope field, and `schema_version != 1` each yield nothing and log (FLK-11, spec P1 AC6)
- [ ] A `RawEvent` whose `source` has no contract yields nothing and logs a warning (`design.md`'s closed gap)
- [ ] `flat_map` is callable directly as a plain Python method in tests - no live cluster
- [ ] Gate check passes: `make test`

**Tests**: unit
**Gate**: full - closes Phase 4

---

### T16: Job wiring (`app.py`)

**What**: Builds the `StreamExecutionEnvironment`, wires `KafkaSource(events-raw, consumer group flink-normalization) → NormalizationFunction → KafkaSink(events-normalized, keyed by partition_key)`, calls `env.execute()`. Entrypoint for Application Mode (`standalone-job -py app.py`).
**Where**: `flink/normalization/app.py`
**Depends on**: T15
**Reuses**: `.env`/`load_dotenv()` + `KAFKA_BOOTSTRAP_SERVERS` convention from `ingestion/app.py`
**Requirement**: FLK-06, FLK-08

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Config/wiring functions (building source/sink configs from env vars) are unit-tested with mocks, mirroring `tests/ingestion/test_app.py`'s style
- [ ] `KafkaSink`'s key extractor is wired to `partition_key`
- [ ] Job does not call `env.execute()` at import time (importable/testable without starting a real pipeline)
- [ ] Contracts are loaded once at startup, not per record (`design.md` Tech Decisions)
- [ ] Gate check passes: `make test`

**Tests**: unit
**Gate**: full

---

### T17: Flink Dockerfile

**What**: `FROM flink:2.3.0-scala_2.12-java17`; installs Python 3.12 + `pip install -r requirements/flink.txt` (which carries `jmespath` and, from T15, `apache-flink` - and via `base.txt`, `pydantic`/`PyYAML`; deliberately not `kafka-python`/`requests`, which are ingestion-only); adds the Flink Kafka connector JAR to `/opt/flink/lib`; copies `flink/normalization/` (including `config/*.yml`) and `shared/models.py` into the image's usrlib path.
**Where**: `infra/docker/flink/Dockerfile`
**Depends on**: T16
**Reuses**: Verified Docker Hub tag (`flink:2.3.0-scala_2.12-java17`) from `design.md` research; `ingestion/Dockerfile`'s shape
**Requirement**: FLK-04

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Image builds successfully
- [ ] `python3 -c "import pyflink, jmespath"` succeeds inside the built image
- [ ] The Kafka connector JAR is present under `/opt/flink/lib`
- [ ] `flink/normalization/config/*.yml` is present in the image - the job cannot start without its contracts (`design.md` Risks)
- [ ] The image does NOT copy `ingestion/` - the contract design removed that cross-package dependency

**Tests**: none
**Gate**: build

---

### T18: Wire Flink cluster into docker-compose (Application Mode)

**What**: Add `flink-jobmanager` (command: `standalone-job -py /opt/flink/usrlib/app.py`) and `flink-taskmanager` services using T17's image, Flink Web UI exposed on the host, `depends_on: topic-init` with `condition: service_completed_successfully` (matching the fix already applied to the `ingestion` service).
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T17
**Reuses**: The `depends_on`/`environment` conventions now established by the `ingestion` service, including the single-broker `KAFKA_BOOTSTRAP_SERVERS: broker-1:19092` form
**Requirement**: FLK-04, FLK-05

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] WHEN `docker compose up` runs THEN JobManager + TaskManager start and the Flink Web UI is reachable from the host
- [ ] The job shows as running in the Flink Web UI without a manual submission step
- [ ] Topic provisioning completes before the Flink services start (spec P1 Infra AC5)
- [ ] `docker compose -f infra/docker/docker-compose.yml config` parses cleanly
- [ ] Flagged risk from `design.md` (exact Application Mode CLI flags for a Python entrypoint): budget for iteration here if the job doesn't submit on the first attempt

**Tests**: none
**Gate**: build

---

### T19: End-to-end infra + normalization smoke test

**What**: Full `docker compose up` run: confirm both topics provisioned, ingestion publishing to `events-raw`, Flink job running and consuming, and `events-normalized` receiving correctly-shaped, correctly-keyed messages for real GitHub traffic. Closes every P1 Independent Test in `spec.md`.
**Where**: N/A (verification task, no new source file)
**Depends on**: T18
**Reuses**: Kafka UI (topic inspection), Flink Web UI (job status)
**Requirement**: FLK-01, FLK-02, FLK-03, FLK-04, FLK-05, FLK-06, FLK-07, FLK-08, FLK-09, FLK-10, FLK-11, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Both topics show the configured partitions/replication/retention in Kafka UI
- [ ] Ingestion container publishes without manual invocation
- [ ] Flink Web UI shows the job running
- [ ] `events-normalized` messages inspected via Kafka UI match the Normalization Mapping shape for at least one real event of a mapped type, and are keyed by `repo_name`
- [ ] At least one event of a not-yet-mapped type appears with envelope + common populated and an empty per-type block, confirming it was not dropped (spec P1 AC5 in production, not just unit tests)

**Tests**: none
**Gate**: build (manual, per `spec.md`'s P1 Independent Tests) - closes Phase 5

---

### T20: Capture real-traffic sample for the 6 remaining event types

**What**: Run the ingestion service (or a throwaway script against the GitHub Events API) long enough to capture at least one real instance of `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, and - best-effort - `SponsorshipEvent`. Mirrors the process that produced `tmp/event_sample.json`.
**Where**: `tmp/event_sample_extended.json`
**Depends on**: T19
**Reuses**: The same capture method behind `tmp/event_sample.json`
**Requirement**: FLK-13

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Sample file contains at least one real instance of each of the 5 reliably-occurring types
- [ ] `SponsorshipEvent` is either captured or explicitly documented as not observed within the window (spec P2 AC4's escape valve)
- [ ] The observation window and method are noted, so the result is reproducible

**Tests**: none
**Gate**: build (manual)

---

### T21: Contract entries for `PushEvent`, `ForkEvent`, `ReleaseEvent`

**What**: Derive the curated field mapping for these three from T20's real sample, add the rows to `spec.md`'s Normalization Mapping table, and add the corresponding `event_types` entries to the GitHub contract.
**Where**: `flink/normalization/config/github.yml`, `.specs/features/flink-normalization/spec.md`
**Depends on**: T20
**Reuses**: The noise-filtering convention already fixed for the 11 mapped types (drop `avatar_url`, `gravatar_id`, redundant `*_url`)
**Requirement**: FLK-14, FLK-15

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Each type's mapping is derived from the captured sample, not from documentation (the whole reason these six were deferred - `context.md`)
- [ ] `spec.md`'s Normalization Mapping table gains a row per type, and the "Not yet mapped" list shrinks accordingly
- [ ] Each type stops falling into the empty-fallback path and produces populated per-type fields (spec P2 AC3)
- [ ] Unit tests use the captured real fixtures, driven through `ContractNormalizer`
- [ ] Gate check passes: `make test`

**Tests**: unit
**Gate**: full

---

### T22: Contract entries for `DiscussionEvent`, `CommitCommentEvent`, `SponsorshipEvent`

**What**: Same as T21 for the remaining three. `SponsorshipEvent` is conditional: if T20 did not capture one, it stays on the empty-envelope fallback and is documented as a known gap rather than blocking the story (spec P2 AC4).
**Where**: `flink/normalization/config/github.yml`, `.specs/features/flink-normalization/spec.md`
**Depends on**: T21
**Reuses**: Same convention as T21
**Requirement**: FLK-14, FLK-15, FLK-16

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] `DiscussionEvent` and `CommitCommentEvent` mapped from captured samples, with `spec.md` rows added
- [ ] `SponsorshipEvent` is either mapped from a real sample, or explicitly recorded in `spec.md` as a known gap on the fallback path (spec P2 AC4) - both are valid completions
- [ ] Unit tests use captured real fixtures for whatever was mapped
- [ ] `spec.md`'s Requirement Traceability updated for FLK-13 through FLK-16
- [ ] Gate check passes: `make test`

**Tests**: unit
**Gate**: full - closes Phase 6

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
`T15` (`NormalizationFunction`) depends on `T11` (`ContractNormalizer`) rather than on the GitHub
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
| T5: `NormalizerBase` port | 1 interface | ✅ Granular |
| T6: flink package test wiring | 3 config files, one cohesive purpose | ⚠️ OK - cohesive (none of the three is verifiable alone; together they are "the flink package is now covered by the existing tooling") |
| T7: contract grammar | 2 related Pydantic models, 1 file | ✅ Granular |
| T8: `iso_to_millis` function | 1 function | ✅ Granular |
| T9: contract compiler | 1 function | ✅ Granular |
| T10: contract loader | 1 function | ✅ Granular |
| T11: `ContractNormalizer` | 1 class | ✅ Granular |
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
| T5 | `NormalizerBase` (ABC) | none | none | ✅ OK |
| T6 | Build/test config only, no code layer | none | none | ✅ OK |
| T7 | Contract models | unit | unit | ✅ OK |
| T8 | Custom JMESPath functions | unit | unit | ✅ OK |
| T9 | Contract compiler | unit | unit | ✅ OK |
| T10 | Contract loader | unit | unit | ✅ OK |
| T11 | `ContractNormalizer` | unit | unit | ✅ OK |
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
assert the normalized output against real fixtures, driven through `ContractNormalizer`.

---

## Tips reference

See `.claude/skills/tlc-spec-driven/references/tasks.md` for the full granularity/dependency/co-location rules applied above.
