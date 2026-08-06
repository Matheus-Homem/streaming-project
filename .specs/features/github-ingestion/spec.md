# GitHub Ingestion Specification

## Problem Statement

The streaming project needs a continuously running service that pulls public GitHub events and turns them into a stable, replayable Kafka stream, plus the local infrastructure to develop against. Everything downstream (Flink normalization/aggregation, OpenSearch, Grafana) depends on this raw stream existing and being trustworthy. This spec formalizes retroactively what is already built (P1) and defines what is explicitly still missing before the ingestion service can run unattended (P2).

## Goals

- [x] A developer can bring up a local 3-controller/3-broker Kafka cluster + Kafka UI with one `docker compose up`
- [x] Public GitHub events are fetched, validated, and published to Kafka as a standardized `RawEvent` envelope
- [ ] The ingestion service survives transient GitHub/Kafka failures and avoids re-publishing duplicates within a run (P2 - not yet built)

## Out of Scope

| Feature | Reason |
| ------- | ------ |
| OpenSearch + Grafana (Epic 1, Story 3) | Separate feature; ingestion doesn't depend on it |
| Flink normalization/aggregation (Epic 3, 4) | Separate feature; consumes this topic, doesn't affect ingestion itself |
| GitLab ingestion | `GitLabEvent`/`SourceType.GITLAB` exist as a stub (`id` field only) but no client/engine path is exercised for it; treated as scaffolding for a future source, not a working capability |
| Kubernetes / Drone / Terraform | RFC marks these "if there's an opportunity" - not tied to this feature |
| Kafka topic creation automation | Topics currently rely on broker auto-creation; explicit topic provisioning (partitions, retention, replication) is infra work, not ingestion-service work |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Raw topic name | Keep the code's current default `events-raw`, shared by every source (GitHub and GitLab both publish here) | User decision: one source-agnostic raw topic instead of one topic per source; the `source` field already on `RawEvent` is what tells events apart downstream, not the topic name. Supersedes the roadmap doc's `github-events-raw` naming, which is now stale | y |
| Schema versioning strategy for `RawEvent.schema_version` | Monotonic integer, bumped only on a breaking change to the envelope shape (not the inner payload) | Simplest scheme that still lets a future Flink job branch on version | n |
| Kafka message key | Out of scope for P1 baseline (documents current unkeyed behavior); a key strategy (e.g. `repo full_name`) becomes a P2 item | No consumer currently depends on partition ordering, so this is safe to defer, but it must be decided before Flink windowing (Epic 3/4) relies on ordering per key | n |

**Open questions:** none outside the table above - all resolved to a default or deferred explicitly.

---

## Implicit-Requirement Dimensions Sweep

| Dimension | Status |
| --- | --- |
| Input validation & bounds | Covered (P1) - `GitHubEvent` (pydantic) rejects malformed payloads; invalid events are skipped and logged, not fatal |
| Failure / partial-failure states | **Gap found**: `app.py`'s `while True` loop has no try/except around `execute()`. Any unhandled exception (GitHub down, Kafka down) currently crashes the whole process instead of retrying the next poll. → P2 |
| Idempotency / retry / duplicate handling | Not implemented. No tracking of previously-seen `source_event_id`; GitHub's public events feed can return overlapping events across polls. → P2 |
| Auth boundaries & rate limits | Not implemented. Anonymous GitHub API calls are capped at 60 req/hour; no rate-limit detection or backoff exists. → P2 |
| Concurrency / ordering | Single-threaded sequential poll loop (no concurrency to reason about). Message key strategy undecided (see Assumptions). |
| Data lifecycle / expiry | N/A for this feature - Kafka topic retention is an infra/topic-provisioning concern, out of scope here |
| Observability | Partially covered - structured per-module logging exists (`shared/logger.py`); no metrics/tracing. Acceptable for P1 scope |
| External-dependency failure | Same gap as failure states above - no circuit breaker/backoff for GitHub or Kafka. → P2 |
| State-transition integrity | N/A - this is a stateless fetch → transform → publish pipeline; no persisted state machine |

---

## User Stories

### P1: Local Kafka infrastructure ⭐ MVP (already built)

**User Story**: As a developer, I want a local Kafka cluster and UI so that I can produce and inspect events while building the pipeline.

**Why P1**: Nothing downstream can be developed or demoed without a running broker to publish to.

**Acceptance Criteria**:

1. The system SHALL provide a 3-controller/3-broker Kafka cluster defined in `docker/docker-compose.yml`, reachable on the host via `localhost:29092`, `localhost:39092`, `localhost:49092`.
2. WHEN the compose stack is started THEN Kafka UI SHALL be reachable at `localhost:8080` and list the cluster's brokers via `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS`.
3. The system SHALL start brokers only after their declared controller dependencies (`depends_on`) are up.

**Independent Test**: `docker compose -f docker/docker-compose.yml up`, then open `localhost:8080` and confirm the cluster and its 3 brokers appear.

---

### P1: GitHub event fetch, normalization, and publish ⭐ MVP (already built)

**User Story**: As a developer, I want the ingestion service to fetch GitHub events, normalize them into a stable envelope, and publish them to Kafka, so downstream jobs have a trustworthy raw stream to consume.

**Why P1**: This is the actual data entry point of the whole project; every other epic depends on it existing.

**Acceptance Criteria**:

1. WHEN `IngestionClient.get_events()` is called with no `owner`/`repo`/`org` THEN the client SHALL `GET https://api.github.com/events` and return the parsed JSON list. *(`ingestion/client.py:39-45`, `tests/ingestion/test_client.py`)*
2. WHEN `owner` and `repo` are both provided and `org` is not THEN the client SHALL request `https://api.github.com/networks/{owner}/{repo}/events`.
3. WHEN `org` is provided alone THEN the client SHALL request `https://api.github.com/orgs/{org}/events`.
4. IF the `owner`/`repo`/`org` combination is invalid (exactly one of `owner`/`repo` set, or `org` combined with either) THEN the client SHALL raise `ValueError`.
5. IF the GitHub response has a non-2xx status THEN the client SHALL raise `HTTPError` and return no events.
6. IF a connection-level error occurs THEN the client SHALL propagate it rather than returning a partial or empty result silently.
7. WHEN a fetched payload validates against `GitHubEvent` THEN `IngestionEngine.process()` SHALL produce a `RawEvent` with `source=GITHUB`, `source_event_id`, `source_event_type`, `observed_at` (ingestion-time timestamp), `schema_version=1`, and the original validated payload preserved under `payload`. *(`ingestion/engine.py:40-52`)*
8. IF a fetched payload fails `GitHubEvent` validation THEN the engine SHALL log the failure (including the event id when present) and exclude that event from the output, without stopping processing of the remaining events in the batch.
9. WHEN the input event list is empty THEN `process()` SHALL return an empty list.
10. WHEN `IngestionPublisher.publish()` is called with one or more `RawEvent` THEN it SHALL JSON-serialize and send each one to the configured Kafka topic, then flush the producer exactly once after all sends succeed. *(`ingestion/publisher.py:34-41`)*
11. IF a Kafka send fails THEN the publisher SHALL NOT call flush and SHALL propagate the `KafkaError`.
12. WHERE no explicit `bootstrap_servers` is passed to `IngestionPublisher` THEN it SHALL read `KAFKA_BOOTSTRAP_SERVERS` from the environment; IF that variable is also unset THEN it SHALL raise `KeyError`.
13. WHEN `IngestionPipeline.execute()` runs THEN it SHALL call, strictly in order, `client.get_events()` → `engine.process()` → `producer.publish()`. *(`ingestion/use_case.py:20-26`)*
14. IF any stage of `execute()` raises THEN the pipeline SHALL propagate the exception and SHALL NOT invoke the remaining stage(s).

**Independent Test**: Run `python -m ingestion.app --source github` against a local Kafka broker and confirm messages land on the raw topic via Kafka UI; `pytest tests/ingestion` passes standalone (35 tests, no live network/Kafka needed - everything above is mocked).

---

### P2: Ingestion resilience and deduplication (not yet built)

**User Story**: As a developer, I want the ingestion service to survive transient GitHub/Kafka failures and avoid re-publishing duplicates within a run, so the raw stream stays usable when left running unattended.

**Why P2**: The current loop crashes on the first unhandled error and has no dedup/rate-limit awareness - acceptable for a manual demo, not for anything left running.

**Acceptance Criteria**:

1. IF `execute()` raises inside the poll loop THEN `app.py`'s `main()` SHALL log the failure and continue to the next poll iteration instead of terminating the process.
2. IF the GitHub API responds with a rate-limit status THEN the client SHALL back off instead of retrying immediately.
3. WHEN an event's `source_event_id` was already published in the current process run THEN the pipeline SHALL skip re-publishing it.
4. WHERE `--poll-interval` is not provided THEN the service SHALL default to the current 5-second interval; WHEN it is provided THEN the service SHALL use that value instead of the hardcoded `sleep(5)`.

**Independent Test**: Simulate a GitHub 5xx/network failure mid-run and confirm the process logs and keeps polling rather than exiting; feed the same event id twice in one run and confirm only one publish call happens.

---

## Edge Cases

- IF `owner`/`repo`/`org` are combined invalidly THEN `IngestionClient` SHALL raise `ValueError` (covered, P1 AC4)
- IF GitHub returns a non-2xx response THEN the client SHALL raise `HTTPError` (covered, P1 AC5)
- IF a GitHub payload is missing required fields or has an unknown event `type` THEN the engine SHALL drop and log it, not raise (covered, P1 AC8)
- IF Kafka publish fails THEN no flush happens and the error propagates (covered, P1 AC11)
- IF `KAFKA_BOOTSTRAP_SERVERS` is unset and no explicit servers are passed THEN `IngestionPublisher` SHALL raise `KeyError` at construction (covered, P1 AC12)
- IF the unhandled-exception loop crash happens (P2 AC1) THEN today's behavior is process termination - explicitly the gap this P2 story closes

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
| ING-09 | P2: Resilience & dedup (crash recovery) | In Tasks (T1) | In Tasks |
| ING-10 | P2: Resilience & dedup (rate limiting) | In Tasks (T2) | In Tasks |
| ING-11 | P2: Resilience & dedup (dedup within run) | In Tasks (T3, T4) | In Tasks |
| ING-12 | P2: Resilience & dedup (configurable poll interval) | In Tasks (T5) | In Tasks |

**ID format:** `ING-NN`

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 12 total, 8 verified against existing code + passing test suite (`pytest tests` - 35 passed), 4 mapped to `.specs/features/github-ingestion/tasks.md` (T1-T5), 0 unmapped

---

## Success Criteria

- [x] `docker compose up` brings up a usable local Kafka cluster + UI
- [x] `python -m ingestion.app --source github` fetches, normalizes, and publishes real GitHub events to Kafka, with 35 passing unit tests covering client/engine/publisher/pipeline
- [ ] The service can run unattended for hours without crashing on a transient GitHub/Kafka failure (P2, pending)
