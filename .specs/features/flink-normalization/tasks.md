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
| `NORMALIZER_REGISTRY` (`models.py`) | unit | Registry resolves the correct concrete `Normalizer` per `SourceType` - mirrors `tests/ingestion/test_models.py`'s coverage of `SOURCE_REGISTRY`/`get_source_config` | `tests/flink/normalization/test_models.py` | `make test` |
| `GitHubNormalizer` (domain logic, `adapters/github_normalizer.py`) | unit | All branches; 1:1 to every row of `spec.md`'s Normalization Mapping table (11 mapped types + shared GitHub-common block + envelope conversion) + the unmapped-type fallback (P1 AC5) | `tests/flink/normalization/adapters/test_github_normalizer.py` | `make test` |
| `NormalizationFunction` (Flink `FlatMapFunction`, `job.py`) | unit | All branches: valid message → yields normalized record; malformed JSON / bad `schema_version` / unregistered source → yields nothing + logs. Called directly as a plain Python method - no live cluster needed | `tests/flink/normalization/test_job.py` | `make test` |
| Job wiring (`app.py`) | unit | Config/env wiring covered with mocks (mirrors `tests/ingestion/test_app.py`'s `build_arguments`/`configure_*` pattern) - `StreamExecutionEnvironment.execute()` itself is not exercised in unit tests | `tests/flink/normalization/test_app.py` | `make test` |
| Docker/compose/shell infra (`Dockerfile`s, `docker-compose.yml`, `create-topics.sh`) | none | No unit tests apply; verified via each story's `spec.md` Independent Test | - | manual (spec Independent Test) + `docker compose -f infra/docker/docker-compose.yml config` for YAML/interpolation validity |

**Coverage Expectation provenance**: no project-wide testing guideline file exists; the strong default (spec ACs → 1:1 test coverage, all listed edge cases) was applied, using `tests/ingestion/`'s existing depth as the floor.

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After a task that only touches one new/changed test file | `python -B -m pytest -s -vv --log-cli-level=INFO tests/flink/normalization/<path>` |
| Full | After a phase completes, or any task touching shared modules (`models.py`, `ports.py`) | `make test` (once updated to `--cov=ingestion --cov=flink`, done in T17) |
| Build | After infra-only tasks (Dockerfiles, compose, shell scripts) | `docker compose -f infra/docker/docker-compose.yml config` (syntax/interpolation check) + the task's own manual Independent Test |

---

## Execution Plan

Phases are ordered and run sequentially - each phase completes before the next begins, and tasks within a phase execute in order. (Arrow diagrams for each phase are consolidated in the **Phase Execution Map** below, with exactly one arrow per real dependency - see that section for the authoritative graph.)

### Phase 1: Kafka topic provisioning

Tasks: T1, T2

### Phase 2: Ingestion containerization

Tasks: T3, T4

### Phase 3: Normalizer foundation + verified event types

Tasks: T5, T6, T7, T8, T9, T10

### Phase 4: Doc-resolved event types + dispatch + Flink function

Tasks: T11, T12, T13, T14, T15, T16, T17, T18

### Phase 5: Flink job wiring + cluster infra

Tasks: T19, T20, T21, T22

### Phase 6: Coverage extension (remaining 6 event types)

Tasks: T23, T24, T25, T26, T27, T28, T29

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
- [ ] Script creates both topics idempotently (safe to re-run - `--if-not-exists` or equivalent)
- [ ] Retries against the broker until reachable instead of failing on first attempt
- [ ] Both topics end up with 3 partitions, replication factor 3, 7-day retention

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
- [ ] `docker compose up` runs `topic-init` after brokers are reachable and it exits 0
- [ ] Kafka UI (`localhost:8080`) shows both topics with the configured partitions/replication/retention

**Tests**: none
**Gate**: build

---

### T3: Ingestion service Dockerfile

**What**: Dockerfile that installs `requirements.txt` and runs `python -m ingestion.app --source github` as its `CMD`.
**Where**: `ingestion/Dockerfile`
**Depends on**: None
**Reuses**: `requirements.txt`, existing `ingestion/app.py` entrypoint (unchanged)
**Requirement**: FLK-02

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Image builds successfully (`docker build -f ingestion/Dockerfile .`)
- [ ] Container runs `python -m ingestion.app --source github` without extra flags

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
- [ ] WHEN `docker compose up` runs THEN the `ingestion` container publishes to `events-raw` without manual invocation (spec P1 Infrastructure AC2)
- [ ] `make ingestion-default` still runs the same code standalone, unaffected (spec P1 Infrastructure AC3)

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

### T6: `GitHubNormalizer` skeleton + shared envelope/common-block extraction

**What**: `GitHubNormalizer(NormalizerBase)` class with the envelope-building helper (`source`, `event_id`, `event_type`, `actor_id`, `actor_login`, `event_time`/`ingested_at` as epoch millis, `schema_version`, `partition_key`=`repo_name`) and the GitHub-common-block helper (`repo_id`, `repo_name`, `org_id`, `org_login`, `public`) - both shared across every event type per `spec.md`'s Normalization Mapping. No per-type dispatch yet (that's T16).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T5
**Reuses**: `ingestion.models.RawEvent`/`GitHubEventType` (imported, not redefined, per design.md's Code Reuse Analysis)
**Requirement**: FLK-06, FLK-07, FLK-08, FLK-09

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Envelope fields match spec.md's Domain-Neutral Envelope table exactly (field names and types)
- [ ] `event_time`/`ingested_at` correctly convert ISO 8601 → epoch milliseconds
- [ ] `partition_key` equals `repo_name`
- [ ] GitHub-common block correctly reads `org_id`/`org_login` as `None` when `GitHubEvent.org` is absent
- [ ] Unit tests cover: envelope conversion, common-block extraction, null-org edge case

**Tests**: unit
**Gate**: quick

---

### T7: `IssueCommentEvent` extraction

**What**: `_extract_issue_comment` method producing the curated fields row for `IssueCommentEvent` from spec.md's Normalization Mapping table (`action`, `issue_*` fields including `issue_is_pull_request`/`issue_labels`, `comment_*` fields).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's envelope/common-block helpers
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `IssueCommentEvent` row exactly, field by field
- [ ] Unit test uses a real `IssueCommentEvent` fixture from `tmp/event_sample.json`
- [ ] `issue_is_pull_request` correctly reflects presence/absence of the `issue.pull_request` key
- [ ] `issue_labels` is a list of label names, not full label objects

**Tests**: unit
**Gate**: quick

---

### T8: `PullRequestEvent` extraction

**What**: `_extract_pull_request` method producing the curated fields row for `PullRequestEvent` (`action`, `pr_number`, `pr_id`, `pr_base_*`, `pr_head_*`, `label_name`, `labels`).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's helpers; `pr_base_*`/`pr_head_*` extraction shape reused again in T9/T10
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `PullRequestEvent` row exactly
- [ ] Unit test uses a real `PullRequestEvent` fixture from `tmp/event_sample.json`
- [ ] `label_name` is correctly `None` when `payload.label` is absent

**Tests**: unit
**Gate**: quick

---

### T9: `PullRequestReviewEvent` extraction

**What**: `_extract_pull_request_review` method (`action`, `pr_number`, `pr_id`, `pr_base_ref`, `pr_head_ref`, `review_*` fields).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's helpers, T8's `pr_base_ref`/`pr_head_ref` extraction shape
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `PullRequestReviewEvent` row exactly
- [ ] Unit test uses a real fixture from `tmp/event_sample.json`

**Tests**: unit
**Gate**: quick

---

### T10: `PullRequestReviewCommentEvent` extraction

**What**: `_extract_pull_request_review_comment` method (`action`, `pr_number`, `pr_id`, `pr_base_ref`, `pr_head_ref`, `comment_*` fields including `comment_path`/`comment_position`/`comment_diff_hunk`/`comment_commit_id`).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's helpers, T8's `pr_base_ref`/`pr_head_ref` shape
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `PullRequestReviewCommentEvent` row exactly
- [ ] Unit test uses a real fixture from `tmp/event_sample.json`
- [ ] Phase 3 fully green: `pytest tests/flink/normalization/adapters/test_github_normalizer.py` passes with all 4 verified-type tests

**Tests**: unit
**Gate**: full (`make test`, end of Phase 3)

---

### T11: `WatchEvent` + `PublicEvent` extraction

**What**: `_extract_watch` (`action`, always `"started"` per docs) and `_extract_public` (no fields - payload is documented as empty).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's helpers
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Both methods match their spec.md rows exactly
- [ ] Unit tests use hand-built fixtures (no real sample exists for these types) matching the documented shape

**Tests**: unit
**Gate**: quick

---

### T12: `CreateEvent` + `DeleteEvent` extraction

**What**: `_extract_create` (`ref`, `ref_type`, `master_branch`, `description`, `pusher_type` - **not** `full_ref`, excluded per spec.md's Assumptions) and `_extract_delete` (`ref`, `ref_type`, `pusher_type`).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's helpers
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Both methods match their spec.md rows exactly, `full_ref` is NOT present in `CreateEvent`'s output
- [ ] Unit tests use hand-built fixtures matching the documented shape

**Tests**: unit
**Gate**: quick

---

### T13: `GollumEvent` extraction

**What**: `_extract_gollum` method producing `pages` as a list of `{page_name, title, summary, action, sha}`.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: T6's helpers
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `GollumEvent` row exactly (list of curated page objects, `html_url` dropped)
- [ ] Unit test uses a hand-built fixture matching the documented shape

**Tests**: unit
**Gate**: quick

---

### T14: `IssuesEvent` extraction

**What**: `_extract_issues` method (`action`, `issue_*` fields, `assignee_login`, `assignees`, `label_name`, `labels`).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6, T7
**Reuses**: T7's `issue_*` field extraction, verified `user`/`label` shapes already used there
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `IssuesEvent` row exactly
- [ ] `assignee_login` is correctly `None` when `payload.assignee` is absent
- [ ] Unit test uses a hand-built fixture combining documented top-level keys with the verified `issue`/`user`/`label` shapes from T7's fixture

**Tests**: unit
**Gate**: quick

---

### T15: `MemberEvent` extraction

**What**: `_extract_member` method (`action`, `member_id`, `member_login`).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T6
**Reuses**: The verified `user` shape (same as `actor`/`comment.user` elsewhere)
**Requirement**: FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Output matches spec.md's `MemberEvent` row exactly
- [ ] Unit test uses a hand-built fixture matching the documented shape

**Tests**: unit
**Gate**: quick

---

### T16: `normalize()` dispatch + unmapped-type fallback

**What**: The public `normalize(self, event: RawEvent) -> dict[str, Any]` method: builds the envelope + common block (T6), dispatches to the right `_extract_*` method by `event.source_event_type` for the 11 mapped types, and returns envelope + common block with an **empty** source-specific fields block for the 6 not-yet-mapped types (spec.md P1 Normalization AC5) - never raises, never returns `None`.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T7, T8, T9, T10, T11, T12, T13, T14, T15
**Reuses**: All `_extract_*` methods built in T7-T15
**Requirement**: FLK-06, FLK-10

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] All 11 mapped types dispatch to their correct extraction method (parametrized or explicit test per type)
- [ ] Each of the 6 unmapped types (`PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, `SponsorshipEvent`) returns envelope + common block populated, source-specific fields empty/null - not dropped
- [ ] `GitHubNormalizer` fully satisfies `NormalizerBase`

**Tests**: unit
**Gate**: quick

---

### T17: `NORMALIZER_REGISTRY` + Makefile coverage update

**What**: `NORMALIZER_REGISTRY: dict[SourceType, NormalizerBase] = {SourceType.GITHUB: GitHubNormalizer()}` in `flink/normalization/models.py`. Also updates `Makefile`'s `test` target to `--cov=ingestion --cov=flink` and `neat` target to include `flink` in its `autoflake`/`isort`/`black` paths.
**Where**: `flink/normalization/models.py`
**Depends on**: T16
**Reuses**: `ingestion/models.py`'s `SOURCE_REGISTRY`/`get_source_config` pattern (registry dict, not `if`/`elif`)
**Requirement**: FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] `NORMALIZER_REGISTRY[SourceType.GITHUB]` resolves to a `GitHubNormalizer` instance
- [ ] `Makefile`'s `test` target updated to `--cov=ingestion --cov=flink`, `neat` target updated to include `flink`
- [ ] `make test` runs and covers `flink/` (verify via `--cov-report=term-missing` output)
- [ ] `make neat` reformats `flink/` without errors

**Tests**: unit
**Gate**: full (`make test`)

---

### T18: `NormalizationFunction` (Flink `FlatMapFunction`)

**What**: `NormalizationFunction(FlatMapFunction)` with `flat_map(self, value: str) -> Iterator[str]`: parses/validates `value` as `RawEvent` (log + yield nothing on bad JSON, missing field, or `schema_version != 1`), looks up `NORMALIZER_REGISTRY.get(event.source)` (log + yield nothing if unregistered), calls `normalize()`, `json.dumps`s the result, yields it. Adds `apache-flink` to `requirements.txt` (flagged in design.md as reasoned-not-hands-on-verified whether this needs a JVM at plain-Python-import time - budget time to confirm during this task).
**Where**: `flink/normalization/job.py`
**Depends on**: T17
**Reuses**: `RawEvent.model_validate_json`, log-and-skip pattern from `ingestion/adapters/engine.py`'s `_format_events`
**Requirement**: FLK-11, FLK-06

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] `apache-flink` added to `requirements.txt`
- [ ] Valid `RawEvent` JSON yields exactly one normalized JSON string
- [ ] Malformed JSON, missing envelope field, and `schema_version != 1` each yield nothing and log
- [ ] A `RawEvent` with an unregistered `source` yields nothing and logs a warning
- [ ] Phase 4 fully green: `pytest tests/flink/normalization` passes

**Tests**: unit
**Gate**: full (`make test`, end of Phase 4)

---

### T19: Job wiring (`app.py`)

**What**: Builds the `StreamExecutionEnvironment`, wires `KafkaSource(events-raw, consumer group flink-normalization) → NormalizationFunction → KafkaSink(events-normalized, keyed by partition_key)`, calls `env.execute()`. Entrypoint for Application Mode (`standalone-job -py app.py`).
**Where**: `flink/normalization/app.py`
**Depends on**: T18
**Reuses**: `.env`/`load_dotenv()` + `KAFKA_BOOTSTRAP_SERVERS` convention from `ingestion/app.py`
**Requirement**: FLK-06, FLK-08

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Config/wiring functions (e.g. building source/sink configs from env vars) are unit-tested with mocks, mirroring `tests/ingestion/test_app.py`'s style
- [ ] `KafkaSink`'s key extractor is wired to `partition_key`
- [ ] Job does not call `env.execute()` at import time (importable/testable without starting a real pipeline)

**Tests**: unit
**Gate**: quick

---

### T20: Flink Dockerfile

**What**: `FROM flink:2.3.0-scala_2.12-java17`; installs Python 3.12 + `pip install apache-flink==2.3.0`; adds the Flink Kafka connector JAR to `/opt/flink/lib`; copies `flink/normalization/` and `ingestion/models.py` into the image's usrlib path.
**Where**: `infra/docker/flink/Dockerfile`
**Depends on**: T19
**Reuses**: Verified Docker Hub tag (`flink:2.3.0-scala_2.12-java17`) from design.md research
**Requirement**: FLK-04

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Image builds successfully
- [ ] `python3 -c "import pyflink"` succeeds inside the built image
- [ ] The Kafka connector JAR is present under `/opt/flink/lib`

**Tests**: none
**Gate**: build

---

### T21: Wire Flink cluster into docker-compose (Application Mode)

**What**: Add `flink-jobmanager` (command: `standalone-job -py /opt/flink/usrlib/app.py`, per the confirmed Application Mode decision) and `flink-taskmanager` services using T20's image, Flink Web UI exposed on the host, `depends_on: topic-init` (completed).
**Where**: `infra/docker/docker-compose.yml`
**Depends on**: T20
**Reuses**: Same `depends_on`/environment-variable conventions as other services
**Requirement**: FLK-04, FLK-05

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] WHEN `docker compose up` runs THEN JobManager + TaskManager start and the Flink Web UI is reachable from the host
- [ ] The job (`flink.normalization`) shows as running in the Flink Web UI without a manual submission step
- [ ] Flagged risk from design.md (exact Application Mode CLI flags for a Python entrypoint): budget for iteration here if the job doesn't submit on the first attempt

**Tests**: none
**Gate**: build

---

### T22: End-to-end infra + normalization smoke test

**What**: Full `docker compose up` run: confirm both topics provisioned correctly, ingestion container publishing to `events-raw`, Flink job running and consuming, and `events-normalized` receiving correctly-shaped, correctly-keyed messages for real GitHub traffic. Closes out every P1 Infrastructure and P1 Normalization Independent Test from spec.md.
**Where**: N/A (verification task, no new source file)
**Depends on**: T2, T4, T21
**Reuses**: Kafka UI (topic inspection), Flink Web UI (job status)
**Requirement**: FLK-01, FLK-02, FLK-03, FLK-04, FLK-05, FLK-06, FLK-07, FLK-08, FLK-09, FLK-10, FLK-11, FLK-12

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Both topics show the configured partitions/replication/retention in Kafka UI
- [ ] Ingestion container is publishing without manual invocation
- [ ] Flink Web UI shows the job running
- [ ] `events-normalized` messages inspected via Kafka UI match the Normalization Mapping table shape for at least one real event of a mapped type, and are keyed by `repo_name`

**Tests**: none
**Gate**: build (manual, per spec.md's P1 Independent Tests) - closes Phase 5

---

### T23: Capture real-traffic sample for the 6 remaining event types

**What**: Run the ingestion service (or a throwaway script hitting the GitHub Events API directly) long enough to capture at least one real instance of `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, and (best-effort) `SponsorshipEvent`, saved as a new sample file. Mirrors the process that produced `tmp/event_sample.json`.
**Where**: `tmp/event_sample_extended.json`
**Depends on**: T22
**Reuses**: `tmp/event_sample.json`'s capture method
**Requirement**: FLK-13

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Sample file contains ≥1 real instance of each of `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`
- [ ] `SponsorshipEvent` is included if observed; if not observed within a reasonable window, documented as absent per spec.md P2 AC4 (not a blocker)

**Tests**: none
**Gate**: build (data artifact, not code)

---

### T24: `PushEvent` extraction

**What**: `_extract_push` method, field mapping derived from T23's captured sample (not the webhook docs already proven unreliable for this type). Extends spec.md's Normalization Mapping table with the verified row before/alongside implementation.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T23
**Reuses**: T16's dispatch (adds one more branch), T6's helpers; also updates `.specs/features/flink-normalization/spec.md`'s Normalization Mapping table
**Requirement**: FLK-14, FLK-15

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] spec.md's Normalization Mapping table gains a real, sample-verified `PushEvent` row
- [ ] `_extract_push` matches that row exactly
- [ ] Unit test uses the real fixture from T23
- [ ] `PushEvent` no longer falls into the T16 empty-fallback path

**Tests**: unit
**Gate**: quick

---

### T25: `ForkEvent` extraction

**What**: `_extract_fork` method, field mapping (curated subset of the `forkee` repository object) derived from T23's captured sample.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T23
**Reuses**: T16's dispatch, T6's helpers; also updates `.specs/features/flink-normalization/spec.md`'s Normalization Mapping table
**Requirement**: FLK-14, FLK-15

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] spec.md gains a real, sample-verified `ForkEvent` row
- [ ] `_extract_fork` matches that row exactly
- [ ] Unit test uses the real fixture from T23

**Tests**: unit
**Gate**: quick

---

### T26: `ReleaseEvent` extraction

**What**: `_extract_release` method, field mapping (curated subset of the `release` resource) derived from T23's captured sample.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T23
**Reuses**: T16's dispatch, T6's helpers; also updates `.specs/features/flink-normalization/spec.md`'s Normalization Mapping table
**Requirement**: FLK-14, FLK-15

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] spec.md gains a real, sample-verified `ReleaseEvent` row
- [ ] `_extract_release` matches that row exactly
- [ ] Unit test uses the real fixture from T23

**Tests**: unit
**Gate**: quick

---

### T27: `DiscussionEvent` extraction

**What**: `_extract_discussion` method, field mapping (curated subset of the `discussion` resource) derived from T23's captured sample.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T23
**Reuses**: T16's dispatch, T6's helpers; also updates `.specs/features/flink-normalization/spec.md`'s Normalization Mapping table
**Requirement**: FLK-14, FLK-15

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] spec.md gains a real, sample-verified `DiscussionEvent` row
- [ ] `_extract_discussion` matches that row exactly
- [ ] Unit test uses the real fixture from T23

**Tests**: unit
**Gate**: quick

---

### T28: `CommitCommentEvent` extraction

**What**: `_extract_commit_comment` method, field mapping derived from T23's captured sample (not assumed-similar to `PullRequestReviewCommentEvent`'s `comment` shape).
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T23
**Reuses**: T16's dispatch, T6's helpers; also updates `.specs/features/flink-normalization/spec.md`'s Normalization Mapping table
**Requirement**: FLK-14, FLK-15

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] spec.md gains a real, sample-verified `CommitCommentEvent` row
- [ ] `_extract_commit_comment` matches that row exactly
- [ ] Unit test uses the real fixture from T23

**Tests**: unit
**Gate**: quick

---

### T29: `SponsorshipEvent` extraction (conditional)

**What**: IF T23 captured a real `SponsorshipEvent` sample, add `_extract_sponsorship` + its spec.md row, same as T24-T28. IF NOT, this task instead updates spec.md to explicitly record `SponsorshipEvent` as remaining on the empty-envelope fallback, per spec.md P2 AC4 - a documented gap, not a failure.
**Where**: `flink/normalization/adapters/github_normalizer.py`
**Depends on**: T23
**Reuses**: T16's dispatch, T6's helpers (if mapped); updates `.specs/features/flink-normalization/spec.md` either way
**Requirement**: FLK-16

**Tools**: MCP: NONE / Skill: NONE

**Done when**:
- [ ] Either `_extract_sponsorship` exists with a sample-verified row and a passing unit test, OR spec.md explicitly documents `SponsorshipEvent` as an open gap per P2 AC4
- [ ] Requirement Traceability table in spec.md reflects the final state (`Verified` or explicitly noted as a documented gap, not left `Pending`)

**Tests**: unit (if mapped) / none (if documenting the gap)
**Gate**: quick (if mapped) / none

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
T6 -> T9
T6 -> T10

Phase 4:
T6 -> T11
T6 -> T12
T6 -> T13
T6 -> T14
T7 -> T14
T6 -> T15
T7 -> T16
T8 -> T16
T9 -> T16
T10 -> T16
T11 -> T16
T12 -> T16
T13 -> T16
T14 -> T16
T15 -> T16
T16 -> T17
T17 -> T18

Phase 5:
T18 -> T19
T19 -> T20
T20 -> T21
T2 -> T22
T4 -> T22
T21 -> T22

Phase 6:
T22 -> T23
T23 -> T24
T23 -> T25
T23 -> T26
T23 -> T27
T23 -> T28
T23 -> T29
```

Execution is strictly sequential - there is no intra-phase parallelism. A single agent (or batch worker) works one task at a time, in order. Edges that cross a phase boundary (e.g. `T2 -> T4`, `T18 -> T19`) are drawn the same as intra-phase edges - they're satisfied by phase ordering at execution time, but shown here for a complete, checkable dependency graph.

**The orchestrating agent's role during Execute:**
1. Count total tasks and pack phases into ~7-task batches - offer batch sub-agents if that yields more than one batch and the user accepts
2. Dispatch the next batch (to a worker, or execute inline)
3. Receive the compact batch summary
4. Update tasks.md with results
5. If the batch summary shows all tasks complete: proceed to the next batch
6. If a task failed: decide fix/escalate before dispatching the next batch

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: topic-creation script | 1 file | ✅ Granular |
| T2: wire topic-init service | 1 file (compose edit) | ✅ Granular |
| T3: ingestion Dockerfile | 1 file | ✅ Granular |
| T4: wire ingestion service | 1 file (compose edit) | ✅ Granular |
| T5: NormalizerBase port | 1 interface | ✅ Granular |
| T6: envelope/common-block helpers | 1 cohesive unit (2 related helpers, same class) | ✅ Granular |
| T7-T15: per-event-type extraction | 1-2 methods each | ✅ Granular |
| T16: dispatch + fallback | 1 method | ✅ Granular |
| T17: registry + Makefile update | 1 dict + 1 config file | ⚠️ OK - cohesive (registry can't be verified as wired without the coverage config that proves `flink/` is actually exercised) |
| T18: NormalizationFunction | 1 class | ✅ Granular |
| T19: job wiring | 1 file | ✅ Granular |
| T20: Flink Dockerfile | 1 file | ✅ Granular |
| T21: wire Flink cluster | 1 file (compose edit) | ✅ Granular |
| T22: e2e smoke test | 1 verification pass | ✅ Granular |
| T23: capture sample | 1 data artifact | ✅ Granular |
| T24-T29: per-event-type extraction (P2) | 1 method each (spec.md row update is documentation, not a second code file) | ✅ Granular |

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
| T9 | T6 | T6 → T9 | ✅ Match |
| T10 | T6 | T6 → T10 | ✅ Match |
| T11 | T6 | T6 → T11 | ✅ Match |
| T12 | T6 | T6 → T12 | ✅ Match |
| T13 | T6 | T6 → T13 | ✅ Match |
| T14 | T6, T7 | T6 → T14, T7 → T14 | ✅ Match |
| T15 | T6 | T6 → T15 | ✅ Match |
| T16 | T7, T8, T9, T10, T11, T12, T13, T14, T15 | T7 → T16, T8 → T16, T9 → T16, T10 → T16, T11 → T16, T12 → T16, T13 → T16, T14 → T16, T15 → T16 | ✅ Match |
| T17 | T16 | T16 → T17 | ✅ Match |
| T18 | T17 | T17 → T18 | ✅ Match |
| T19 | T18 | T18 → T19 | ✅ Match |
| T20 | T19 | T19 → T20 | ✅ Match |
| T21 | T20 | T20 → T21 | ✅ Match |
| T22 | T2, T4, T21 | T2 → T22, T4 → T22, T21 → T22 | ✅ Match |
| T23 | T22 | T22 → T23 | ✅ Match |
| T24 | T23 | T23 → T24 | ✅ Match |
| T25 | T23 | T23 → T25 | ✅ Match |
| T26 | T23 | T23 → T26 | ✅ Match |
| T27 | T23 | T23 → T27 | ✅ Match |
| T28 | T23 | T23 → T28 | ✅ Match |
| T29 | T23 | T23 → T29 | ✅ Match |

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
| T6 | `GitHubNormalizer` (domain logic) | unit | unit | ✅ OK |
| T7-T16 | `GitHubNormalizer` (domain logic) | unit | unit | ✅ OK |
| T17 | `NORMALIZER_REGISTRY` + Makefile | unit (registry) / none (Makefile) | unit | ✅ OK (highest of the two layers touched) |
| T18 | `NormalizationFunction` | unit | unit | ✅ OK |
| T19 | Job wiring (`app.py`) | unit | unit | ✅ OK |
| T20 | Docker/shell infra | none | none | ✅ OK |
| T21 | Docker/shell infra | none | none | ✅ OK |
| T22 | Verification only, no new layer | none | none | ✅ OK |
| T23 | Data artifact, no new layer | none | none | ✅ OK |
| T24-T28 | `GitHubNormalizer` (domain logic) | unit | unit | ✅ OK |
| T29 | `GitHubNormalizer` (domain logic) or spec doc only | unit / none | unit / none | ✅ OK (conditional, both branches valid) |

No violations. `Tests: none` is only used for layers the matrix marks `none` (infra/config/verification/data-artifact tasks).

---

## Tips reference

See `.claude/skills/tlc-spec-driven/references/tasks.md` for the full granularity/dependency/co-location rules applied above.
