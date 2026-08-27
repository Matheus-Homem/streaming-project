# GitHub Ingestion Specification

## Problem Statement

The streaming project needs a continuously running service that pulls public GitHub events and turns them into a stable, replayable Kafka stream, plus the local infrastructure to develop against. Everything downstream (Flink normalization/aggregation, OpenSearch, Grafana) depends on this raw stream existing and being trustworthy. This spec formalizes retroactively what is already built (P1) and defines what is explicitly still missing before the ingestion service can run unattended (P2).

## Goals

- [x] A developer can bring up a local 3-controller/3-broker Kafka cluster + Kafka UI with one `docker compose up`
- [x] Public GitHub events are fetched, validated, and published to Kafka as a standardized `RawEvent` envelope
- [x] The ingestion service survives transient GitHub/Kafka failures and avoids re-publishing duplicates within a run (P2 - code-level behavior verified by an independent Verifier, `validation.md` iteration 2, PASS; the "runs unattended for hours" success criterion below is a separate, still-open live-observation item)

## Out of Scope

| Feature | Reason |
| ------- | ------ |
| OpenSearch + Grafana (Epic 1, Story 3) | Separate feature; ingestion doesn't depend on it |
| Flink normalization/aggregation (Epic 3, 4) | Separate feature; consumes this topic, doesn't affect ingestion itself |
| GitLab ingestion | A `gitlab` entry exists under `interface/sources/gitlab/ingestion.yml` (endpoints/headers/`id_field`/`type_field`) but no client/engine run has been exercised against it; treated as scaffolding for a future source, not a working capability |
| Kubernetes / Drone / Terraform | RFC marks these "if there's an opportunity" - not tied to this feature |
| Kafka topic creation automation | Topics currently rely on broker auto-creation; explicit topic provisioning (partitions, retention, replication) is infra work, not ingestion-service work |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Raw topic name | Keep the code's current default `events-raw`, shared by every source (GitHub and GitLab both publish here) | User decision: one source-agnostic raw topic instead of one topic per source; the `source` field already on `RawEvent` is what tells events apart downstream, not the topic name | y |
| Schema versioning strategy for `RawEvent.schema_version` | Monotonic integer, bumped only on a breaking change to the envelope shape (not the inner payload) | Simplest scheme that still lets a future Flink job branch on version | n |
| Kafka message key | Out of scope for P1 baseline (documents current unkeyed behavior); a key strategy (e.g. `repo full_name`) becomes a P2 item | No consumer currently depends on partition ordering, so this is safe to defer, but it must be decided before Flink windowing (Epic 3/4) relies on ordering per key | n |

**Open questions:** none outside the table above - all resolved to a default or deferred explicitly.

---

## Implicit-Requirement Dimensions Sweep

| Dimension | Status |
| --- | --- |
| Input validation & bounds | Covered (P1) - the generic `EventModel` (pydantic, `extra="allow"`) rejects malformed payloads once `SourceConfig.get_event_id`/`get_event_type` resolve `id`/`type` from the YAML-declared dotted paths; invalid events are skipped and logged, not fatal |
| Failure / partial-failure states | Covered (P2) - `app.py`'s poll loop wraps `execute()` in try/except; an unhandled exception logs and backs off instead of crashing the process |
| Idempotency / retry / duplicate handling | Covered (P2) - `InMemoryDuplicateTracker` skips re-publishing a `source_event_id` already seen in the current process run |
| Auth boundaries & rate limits | Covered (P2) - a GitHub rate-limit response raises `RateLimitError`, and `RetryTimer.schedule_sleep` backs off until the reported reset time |
| Concurrency / ordering | Single-threaded sequential poll loop (no concurrency to reason about). Message key strategy undecided (see Assumptions). |
| Data lifecycle / expiry | N/A for this feature - Kafka topic retention is an infra/topic-provisioning concern, out of scope here |
| Observability | Partially covered - structured per-module logging exists (`shared/logger.py`); no metrics/tracing. Acceptable for P1/P2 scope |
| External-dependency failure | Covered (P2), same mechanism as failure states above |
| State-transition integrity | N/A - this is a stateless fetch → transform → publish pipeline; no persisted state machine |

---

## User Stories

### P1: Local Kafka infrastructure ⭐ MVP (already built)

**User Story**: As a developer, I want a local Kafka cluster and UI so that I can produce and inspect events while building the pipeline.

**Why P1**: Nothing downstream can be developed or demoed without a running broker to publish to.

**Acceptance Criteria**:

1. The system SHALL provide a 3-controller/3-broker Kafka cluster defined in `infra/docker/docker-compose.yml` (moved from `docker/docker-compose.yml` per `AD-005`), reachable on the host via `localhost:29092`, `localhost:39092`, `localhost:49092`.
2. WHEN the compose stack is started THEN Kafka UI SHALL be reachable at `localhost:8080` and list the cluster's brokers via `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS`.
3. The system SHALL start brokers only after their declared controller dependencies (`depends_on`) are up.

**Independent Test**: `docker compose -f infra/docker/docker-compose.yml up`, then open `localhost:8080` and confirm the cluster and its 3 brokers appear.

---

### P1: GitHub event fetch, normalization, and publish ⭐ MVP (already built)

**User Story**: As a developer, I want the ingestion service to fetch GitHub events, normalize them into a stable envelope, and publish them to Kafka, so downstream jobs have a trustworthy raw stream to consume.

**Why P1**: This is the actual data entry point of the whole project; every other epic depends on it existing.

**Acceptance Criteria**:

1. WHEN `main()` parses CLI args THEN `--source` (required) selects the YAML entry under `interface/sources/<source>/ingestion.yml`, `--endpoint` (optional, default `"default"`) selects the endpoint variant, and repeated `--param KEY=VALUE` supplies the template parameters (e.g. `owner`, `repo`, `org`) needed to fill that variant's URL template. *(`ingestion/app.py`)*
2. WHEN `YamlSourceConfigRepository.get(source, endpoint, endpoint_params)` is called THEN it SHALL look up the source's `ingestion.yml`, resolve the requested endpoint template (its `endpoints` map), and format it with the given params into a `SourceConfig.url`. *(`ingestion/domain/source_config_repository.py`)*
3. IF the requested source or endpoint variant does not exist, or a required template parameter is missing THEN `YamlSourceConfigRepository.get` SHALL raise (`NotImplementedError` for an unknown source, `ValueError` for an unknown endpoint or missing params).
4. WHEN `RequestsClientAdapter.get_events()` is called THEN it SHALL `GET` `self.source_config.url` (already fully resolved by `YamlSourceConfigRepository`) and return the parsed JSON list. *(`ingestion/adapters/client.py`)*
5. IF the GitHub response has a non-2xx status THEN the client SHALL raise `HTTPError` (via `response.raise_for_status()`) and return no events.
6. IF a connection-level error occurs THEN the client SHALL propagate it rather than returning a partial or empty result silently.
7. WHEN a fetched payload's `id`/`type` are resolvable via `SourceConfig.get_event_id`/`get_event_type` (the YAML-declared `id_field`/`type_field` dotted paths) THEN `ValidatingRawEventFormatter.process()` SHALL produce a `RawEvent` with `source=<source name>`, `source_event_id`, `source_event_endpoint=<endpoint variant>`, `source_event_type`, `observed_at` (ingestion-time timestamp, UTC), `schema_version=1`, and the original payload (validated as a generic `EventModel`) preserved under `payload`. *(`ingestion/domain/formatter.py`)*
8. IF a fetched payload fails `EventModel` validation, or its `id`/`type` path can't be resolved (`ValidationError`, `KeyError`, `ValueError`) THEN the formatter SHALL log the failure and exclude that event from the output, without stopping processing of the remaining events in the batch.
9. WHEN the input event list is empty THEN `process()` SHALL return an empty list.
10. WHEN `KafkaProducerAdapter.publish()` is called with one or more `RawEvent` THEN it SHALL JSON-serialize and send each one to the configured Kafka topic, then flush the producer exactly once after all sends succeed. *(`ingestion/adapters/producer.py`)*
11. IF a Kafka send fails THEN the producer SHALL NOT call flush and SHALL propagate the `KafkaError`.
12. WHERE no explicit `bootstrap_servers` is passed to `KafkaProducerAdapter` THEN it SHALL read `KAFKA_BOOTSTRAP_SERVERS` from the environment; IF that variable is also unset THEN it SHALL raise `KeyError`.
13. WHEN `IngestionPipeline.execute()` runs THEN it SHALL call, strictly in order, `client.get_events()` → `engine.process()` → (dedup via `tracker`) → `producer.publish()`. *(`ingestion/use_case.py`)*
14. IF any stage of `execute()` raises THEN the pipeline SHALL propagate the exception and SHALL NOT invoke the remaining stage(s).

**Independent Test**: Run `python -m ingestion.app --source github` against a local Kafka broker and confirm messages land on the raw topic via Kafka UI; `make test` passes standalone (no live network/Kafka needed - everything above is mocked).

---

### P2: Ingestion resilience and deduplication

**User Story**: As a developer, I want the ingestion service to survive transient GitHub/Kafka failures and avoid re-publishing duplicates within a run, so the raw stream stays usable when left running unattended.

**Why P2**: The P1 loop crashed on the first unhandled error and had no dedup/rate-limit awareness - acceptable for a manual demo, not for anything left running.

**Acceptance Criteria**:

1. IF `execute()` raises inside the poll loop THEN `app.py`'s `main()` SHALL log the failure and continue to the next poll iteration instead of terminating the process.
2. IF the GitHub API responds with a rate-limit status THEN the client SHALL back off instead of retrying immediately.
3. WHEN an event's `source_event_id` was already published in the current process run THEN the pipeline SHALL skip re-publishing it.
4. WHERE `--poll-interval` is not provided THEN the service SHALL default to the current 5-second interval; WHEN it is provided THEN the service SHALL use that value instead of the hardcoded `sleep(5)`.

**Independent Test**: Simulate a GitHub 5xx/network failure mid-run and confirm the process logs and keeps polling rather than exiting; feed the same event id twice in one run and confirm only one publish call happens.

---

## Edge Cases

- IF a source/endpoint variant is unknown, or a required template parameter (`owner`/`repo`/`org`, per the YAML endpoint template) is missing THEN `get_source_config` SHALL raise (`NotImplementedError`/`ValueError`) before any request is made (covered, P1 AC3)
- IF GitHub returns a non-2xx response THEN the client SHALL raise `HTTPError` (covered, P1 AC5)
- IF a GitHub payload is missing the field its `id_field`/`type_field` points to, or fails `EventModel` validation THEN the engine SHALL drop and log it, not raise (covered, P1 AC8)
- IF Kafka publish fails THEN no flush happens and the error propagates (covered, P1 AC11)
- IF `KAFKA_BOOTSTRAP_SERVERS` is unset and no explicit servers are passed THEN `IngestionProducer` SHALL raise `KeyError` at construction (covered, P1 AC12)
- IF an unhandled exception occurs inside the poll loop THEN it is logged and the loop continues, rather than terminating the process (covered, P2 AC1)

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| ING-01 | P1: Kafka infrastructure | - | Verified |
| ING-02 | P1: Kafka infrastructure | - | Verified |
| ING-03 | P1: Kafka infrastructure | - | Verified |
| ING-04 | P1: Fetch/normalize/publish (client URL routing) | - | Verified |
| ING-05 | P1: Fetch/normalize/publish (client error handling) | - | Verified |
| ING-06 | P1: Fetch/normalize/publish (engine normalization) | - | Verified |
| ING-07 | P1: Fetch/normalize/publish (publisher) | - | Verified |
| ING-08 | P1: Fetch/normalize/publish (pipeline orchestration) | - | Verified |
| ING-09 | P2: Resilience & dedup (crash recovery) | In Tasks (T1) | ✅ Verified |
| ING-10 | P2: Resilience & dedup (rate limiting) | In Tasks (T2) | ✅ Verified |
| ING-11 | P2: Resilience & dedup (dedup within run) | In Tasks (T3, T4) | ✅ Verified |
| ING-12 | P2: Resilience & dedup (configurable poll interval) | In Tasks (T5) | ✅ Verified |

**ID format:** `ING-NN`

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 12 total, 12 verified against existing code + passing test suite (`make test` - 66 passed), 0 with open fix tasks (`.specs/features/streaming-ingestion/validation.md` iteration 2 - ING-10's rate-limit-detection edge branch and ING-12's `--poll-interval` explicit-override are now covered), 0 unmapped

---

## Success Criteria

- [x] `docker compose up` brings up a usable local Kafka cluster + UI
- [x] `python -m ingestion.app --source github` fetches, normalizes, and publishes real GitHub events to Kafka, with 35 passing unit tests covering client/engine/publisher/pipeline
- [ ] The service can run unattended for hours without crashing on a transient GitHub/Kafka failure (P2, pending)
