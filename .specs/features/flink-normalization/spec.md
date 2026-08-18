# Flink Normalization Specification

## Problem Statement

`events-raw` exists and is being filled by the ingestion service, but nothing downstream consumes it yet, and the local stack has never been formalized beyond "whatever got me far enough to test ingestion" - Kafka topics rely on broker auto-creation, and the ingestion service only runs as a manually-launched host process. This feature builds the first Flink consumer: a PyFlink job that flattens raw GitHub event payloads into a common, flat shape on a new `events-normalized` topic. Because that job needs a real running stack to mean anything, this feature also formalizes how the whole local pipeline comes up - explicit Kafka topic provisioning, the ingestion service as a docker-compose service, and a standalone Flink cluster - as the foundation the next feature (windowed aggregation) builds on.

## Goals

- [ ] The local stack (Kafka with explicitly provisioned topics, the ingestion service, a Flink cluster) comes up with one `docker compose up`
- [ ] A PyFlink job consumes `events-raw` and publishes flattened, common-shaped events to `events-normalized` for the 11 GitHub event types with a verified field mapping
- [ ] `events-normalized` is partitioned by `repo_name`, resolving the message-key decision the `streaming-ingestion` spec deferred
- [ ] The remaining 6 GitHub event types get real-traffic samples captured and their field mapping added, within this same feature

## Out of Scope

| Feature | Reason |
| ------- | ------ |
| Aggregation / windowing over `events-normalized` | Separate future feature; this feature only produces the normalized stream it will consume |
| OpenSearch + Grafana | Separate future feature; unaffected by this one |
| GitLab ingestion wiring | Still a stub (a `gitlab` entry in `ingestion/config/sources.yml`, no client/engine run exercised); unaffected here |
| Non-VCS API sources (weather, financial/monetary, etc.) | `AD-004`/`AD-006` (`.specs/STATE.md`) record the architectural intent to stay pluggable for this, but no additional source is declared in this feature - GitHub is the only normalization contract authored now. Under `AD-006` adding one is a YAML file, not a code change |
| Flink job resilience / checkpointing hardening (restart strategies, exactly-once tuning) | Mirrors how `streaming-ingestion`'s resilience work was its own P2 after the initial P1; deferred to a future feature once the job exists to harden |
| Kubernetes / Drone / Terraform | RFC marks these "if there's an opportunity" - not tied to this feature. `AD-005` (`.specs/STATE.md`) records the concrete trigger conditions identified for each (K8s: local stack outgrowing Compose orchestration - plausible once this feature and OpenSearch/Grafana land; Terraform: only if the project provisions real AWS free-tier resources) and reserves `infra/k8s/`/`infra/terraform/` as their landing paths, but neither is adopted by this feature |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Flink job language/API | PyFlink (Python) | Consistent with the rest of the repo's stack; keeps the learning curve on Flink concepts, not a new language | y |
| Flink deployment topology | Standalone cluster (JobManager + TaskManager) via docker-compose, alongside Kafka | Mirrors the existing Kafka pattern; gives a Flink Web UI to observe jobs, like Kafka UI does for the broker | y |
| Kafka topic provisioning | Explicit creation via docker-compose for both `events-raw` and `events-normalized`, replacing broker auto-creation | User wants the cluster formalized instead of "however auto-creation happened to configure it" | y |
| Topic config (partitions / replication / retention) | 3 partitions, replication factor 3, 7-day retention, for both topics | Matches the existing 3-broker cluster; a reasonable, easily-revised local-dev default | n |
| Ingestion service runtime | New docker-compose service (own Dockerfile); `make ingestion-default` also stays working outside compose | Flink needs continuous live data to consume; keeping the manual path avoids losing a simple debug workflow | y |
| Normalization depth | Full per-event-type flattening with a curated subset per nested object (not full recursive flatten, not envelope-only) | User's explicit choice - more thorough than MVP-minimal, but noise fields (`avatar_url`, `gravatar_id`, redundant `*_url`) are dropped | y |
| Timestamp representation | `event_time`/`ingested_at` as epoch milliseconds in the normalized schema | Flink event-time/watermarks work natively with epoch millis; resolves this before the next feature (windowed aggregation) needs it | y |
| Partition key | `partition_key` = `repo_name` | Resolves the message-key item `streaming-ingestion/spec.md` explicitly deferred "before Flink windowing... relies on ordering per key" | y |
| Normalizer architecture | **Revised 2026-08-17 (`AD-006`)**: contract-driven. A single source-agnostic `ContractNormalizer` interprets a YAML contract per source; the `Normalizer` port (ABC) is retained as the escape hatch for a source too irregular for a contract. Normalized schema still splits into a domain-neutral envelope + a source-specific fields block | Driven by the user's long-term API-agnostic-platform intent. `AD-004` set the pluggability principle; `AD-006` sets the authoring medium - users declare sources in data, never in Python. See `.specs/PLATFORM.md` | y |
| Field-naming convention for the GitHub-specific block | No per-field source prefix (e.g. `repo_id`, not `github_repo_id`); the block as a whole is understood as GitHub-specific because only one source exists today | Simplicity over speculative collision-avoidance; revisit if/when a second real source shares the topic | n |
| Fallback for the 6 not-yet-mapped event types | Envelope populated, GitHub-specific fields block empty/null; event still published, not dropped | Avoids silent data loss while mapping coverage is incomplete, matching the "don't lose data" bias already in `ingestion/adapters/engine.py` | n |
| Malformed message / unknown `schema_version` handling | Log and skip, job keeps running | Mirrors `ingestion/adapters/engine.py`'s existing pattern for payloads that fail validation | n |
| Processing semantics | At-least-once is sufficient | The normalizer is a stateless map; reprocessing the same message yields the same output. Exactly-once deferred to stateful aggregation | n |
| `CreateEvent`'s `full_ref` field | Excluded from the curated mapping | Appeared in the one doc source consulted but wasn't independently corroborated, and the same doc source already proved incomplete for `PushEvent`; excluding avoids propagating a possibly-fabricated field | n |

**Open questions:** none outside the table above - all resolved to a default or deferred explicitly.

---

## Implicit-Requirement Dimensions Sweep

| Dimension | Status |
| --- | --- |
| Input validation & bounds | Covered - malformed JSON, a missing envelope field, or an unexpected `schema_version` are logged and the message is skipped, not fatal (P1 Normalization AC6) |
| Failure / partial-failure states | Covered for single-message failures (same AC6). Job-level failure/restart hardening (checkpointing, restart strategies) is explicitly Out of Scope, deferred to a future feature |
| Idempotency / retry / duplicate handling | Covered - the normalizer is a stateless map; at-least-once reprocessing of the same message produces the same output, so no new dedup logic is needed beyond what ingestion already guarantees upstream |
| Auth boundaries & rate limits | N/A - internal pipeline stage, no external caller or auth boundary involved |
| Concurrency / ordering | Covered - `partition_key = repo_name` gives per-repo ordering on `events-normalized` (P1 Normalization AC3), resolving the ordering dependency `streaming-ingestion/spec.md` flagged for future windowing |
| Data lifecycle / expiry | Covered - explicit 7-day retention set for both `events-raw` and `events-normalized` (P1 Infrastructure AC1) |
| Observability | Covered - per-class logger convention carried over (`shared/logger.py`); Flink Web UI gives job-level visibility (P1 Infrastructure AC4). No new metrics/tracing beyond what Grafana/OpenSearch will add later |
| External-dependency failure | Partially covered - malformed-message handling is covered (AC6); Kafka/Flink connection-level resilience is explicitly Out of Scope, deferred |
| State-transition integrity | N/A - stateless transform, no persisted state machine |

---

## User Stories

### P1: Local stack infrastructure ⭐ MVP

**User Story**: As a developer, I want the whole local pipeline (Kafka with explicitly provisioned topics, the ingestion service, a Flink cluster) to come up with one command, so the normalization job always has real, continuous data to process without manual multi-step setup.

**Why P1**: The normalization job is meaningless without a running Flink cluster, provisioned topics, and live data - none of which exist as a formalized, repeatable setup today.

**Acceptance Criteria**:

1. The system SHALL explicitly provision `events-raw` and `events-normalized` via docker-compose (not broker auto-creation), each with 3 partitions, replication factor 3, and 7-day retention.
2. WHEN `docker compose up` runs THEN the ingestion service SHALL start as a container, publishing GitHub events to `events-raw` continuously without manual invocation.
3. The system SHALL continue to support `make ingestion-default` as a standalone host process outside docker-compose, unchanged from its current behavior.
4. WHEN `docker compose up` runs THEN a Flink JobManager and at least one TaskManager SHALL start as containers, with the Flink Web UI reachable from the host.
5. WHEN `docker compose up` runs THEN topic provisioning SHALL complete (via compose dependency ordering) before the ingestion and Flink containers start.

**Independent Test**: `docker compose -f infra/docker/docker-compose.yml up`, confirm both topics exist with the configured partition/replication/retention via Kafka UI, confirm the ingestion container is publishing to `events-raw`, and confirm the Flink Web UI is reachable and shows a running (or ready-to-submit) cluster.

---

### P1: GitHub event normalization ⭐ MVP

**User Story**: As a developer, I want a PyFlink job that reads `events-raw` and publishes flattened, common-shaped GitHub events to `events-normalized`, so downstream aggregation has a stable, predictable schema instead of GitHub's deeply nested raw payload.

**Why P1**: This is the actual capability the feature exists to deliver - everything else is the infrastructure it needs to run against.

**Acceptance Criteria**:

1. WHEN a message on `events-raw` has `source=github` and a `source_event_type` matching one of the 11 types in the Normalization Mapping table below THEN the job SHALL publish to `events-normalized` a normalized event containing the Domain-Neutral Envelope fields plus exactly the Curated Fields listed for that type.
2. The Domain-Neutral Envelope SHALL always include `source`, `event_id`, `event_type`, `actor_id`, `actor_login`, `event_time`, `ingested_at`, `schema_version`, and `partition_key`.
3. WHERE the source event is GitHub THEN `partition_key` SHALL be set to the event's `repo_name`.
4. `event_time` (from the source `created_at`) and `ingested_at` (from the source `observed_at`) SHALL be epoch-millisecond integers, converted from the source `RawEvent`'s ISO 8601 strings.
5. IF a message's `source_event_type` is one of the 6 types not yet in the Normalization Mapping table (`PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, `SponsorshipEvent`) THEN the job SHALL still publish a normalized event with the Domain-Neutral Envelope populated and the GitHub-specific fields block empty/null, rather than dropping the event.
6. IF a message fails to deserialize as valid JSON, is missing a required envelope field, or has a `schema_version` other than `1` THEN the job SHALL log the failure (including `event_id` when parseable) and skip that message without stopping the job.
7. The job SHALL reach all GitHub-specific field extraction through a `Normalizer` interface, with `GitHubNormalizer` as its sole concrete implementation - the job's core pipeline SHALL NOT contain GitHub-specific branching itself (`AD-004`).

**Independent Test**: Feed each of the 11 mapped event types (from `tmp/event_sample.json` where available, hand-built fixtures otherwise) through the job and confirm the published `events-normalized` message matches its table row exactly; feed one of the 6 unmapped types and confirm it publishes with an empty GitHub-specific block instead of being dropped; feed a malformed message and confirm it's logged and skipped without crashing the job.

---

### Normalization Mapping (referenced by P1: GitHub event normalization, AC1)

> **Path notation (revised 2026-08-17, `AD-006`).** Sources below are written as the paths a
> normalization contract declares, evaluated against `RawEvent.payload` - which holds the whole source
> event dict. So `actor`, `repo`, `org`, `created_at`, and `public` sit at the root, while GitHub's own
> nested payload object is reached as `payload.<...>`. The earlier `GitHubEvent.x` notation referred to
> a Pydantic class removed by `refactor/yaml-driven-source-config` (PR #5); the *shape* is unchanged,
> only the notation. Fields sourced from the pipeline envelope rather than the payload are marked
> `RawEvent.x` and are populated by the normalizer itself, not by a contract rule.

**Domain-Neutral Envelope** (every normalized event, regardless of source):

| Field | Type | Source |
| --- | --- | --- |
| `source` | string | `RawEvent.source` (envelope, not a contract rule) |
| `event_id` | string | `RawEvent.source_event_id` (envelope) |
| `event_type` | string | `RawEvent.source_event_type` (envelope) |
| `actor_id` | integer | `actor.id` |
| `actor_login` | string | `actor.login` |
| `event_time` | long (epoch millis) | `created_at`, converted (`as: timestamp`) |
| `ingested_at` | long (epoch millis) | `RawEvent.observed_at`, converted (envelope) |
| `schema_version` | integer | `1` (normalized schema's own version, independent of `RawEvent.schema_version`) |
| `partition_key` | string | Per-source contract rule (GitHub: `repo.name`) |

**GitHub-specific fields block** (common to all 17 GitHub event types - `actor`/`repo`/`org` are at the payload root, not inside the nested `payload` object):

| Field | Type | Source |
| --- | --- | --- |
| `repo_id` | integer | `repo.id` |
| `repo_name` | string | `repo.name` |
| `org_id` | integer, nullable | `org.id` |
| `org_login` | string, nullable | `org.login` |
| `public` | boolean | `public` |

**Per-type curated fields** (from the nested `payload` object, the only part of the shape that varies by type):

| `source_event_type` | Source of mapping | Curated fields (from `payload`) |
| --- | --- | --- |
| `IssueCommentEvent` | Verified (`tmp/event_sample.json`) | `action`; `issue_id`, `issue_number`, `issue_title`, `issue_state`, `issue_created_at`, `issue_updated_at`, `issue_comments_count` (from `issue.comments`), `issue_is_pull_request` (bool: `issue.pull_request` key present), `issue_user_login`, `issue_labels` (list of `issue.labels[].name`); `comment_id`, `comment_body`, `comment_user_login`, `comment_created_at`, `comment_updated_at` |
| `PullRequestEvent` | Verified | `action`, `pr_number`, `pr_id`; `pr_base_ref`, `pr_base_sha`, `pr_base_repo_id`, `pr_base_repo_name`; `pr_head_ref`, `pr_head_sha`, `pr_head_repo_id`, `pr_head_repo_name`; `label_name` (nullable, from `payload.label.name`), `labels` (list of `payload.labels[].name`) |
| `PullRequestReviewEvent` | Verified | `action`, `pr_number`, `pr_id`, `pr_base_ref`, `pr_head_ref`; `review_id`, `review_state`, `review_body`, `review_submitted_at`, `review_user_login` |
| `PullRequestReviewCommentEvent` | Verified | `action`, `pr_number`, `pr_id`, `pr_base_ref`, `pr_head_ref`; `comment_id`, `comment_body`, `comment_path`, `comment_position`, `comment_diff_hunk`, `comment_commit_id`, `comment_created_at`, `comment_updated_at`, `comment_user_login` |
| `WatchEvent` | GitHub Events API docs | `action` (docs: only value is `started`) |
| `CreateEvent` | GitHub Events API docs | `ref`, `ref_type`, `master_branch`, `description`, `pusher_type` (`full_ref` excluded - see Assumptions) |
| `DeleteEvent` | GitHub Events API docs | `ref`, `ref_type`, `pusher_type` |
| `PublicEvent` | GitHub Events API docs | none (payload is documented as empty) |
| `GollumEvent` | GitHub Events API docs | `pages` - list of `{page_name, title, summary, action, sha}` |
| `IssuesEvent` | Docs (top-level) + verified nested shapes (`issue`, `user`, `label`) | `action`; `issue_id`, `issue_number`, `issue_title`, `issue_state`, `issue_created_at`, `issue_updated_at`, `issue_comments_count`, `issue_user_login`, `issue_labels`; `assignee_login` (nullable), `assignees` (list of logins); `label_name` (nullable), `labels` (list of names) |
| `MemberEvent` | Docs (top-level) + verified `user` shape | `action`; `member_id`, `member_login` |

**Not yet mapped** (P2 story below): `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, `SponsorshipEvent`.

---

### P2: Coverage extension for the remaining 6 event types

**User Story**: As a developer, I want the normalization job's field mapping extended to `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, and `SponsorshipEvent` using real captured traffic, so normalization coverage is complete before aggregation work begins - matching the priority the user set for this feature.

**Why P2**: These 6 types couldn't be mapped trustworthily from documentation alone during Specify - the same doc source that covers them already proved incomplete for `PushEvent` (omitted fields expected from experience) and doesn't document `SponsorshipEvent` at all. Mapping them requires the same real-traffic-sample approach that grounded the other 11.

**Acceptance Criteria**:

1. The developer SHALL capture a real-traffic sample containing at least one instance of each of the 6 remaining event types, using the same method that produced `tmp/event_sample.json`.
2. WHEN the sample confirms a type's real payload shape THEN the Normalization Mapping table SHALL be extended with that type's curated fields, following the same noise-filtering convention as the 11 already-mapped types (drop `avatar_url`, `gravatar_id`, redundant `*_url` fields; keep identifying/content fields).
3. WHEN `GitHubNormalizer` is updated for a newly-mapped type THEN messages of that type SHALL stop falling into the P1 AC5 empty-fallback path and SHALL produce populated GitHub-specific fields.
4. IF real-traffic capture does not yield a `SponsorshipEvent` sample within a reasonable observation window (GitHub Sponsors events are rare on the public feed) THEN that type MAY remain on the empty-envelope fallback, documented as a known gap rather than blocking the rest of this story.

**Independent Test**: For each of the 6 types, feed a captured real sample through the updated job and confirm the published `events-normalized` message has populated (non-null) GitHub-specific fields instead of falling into the empty-fallback path.

---

## Edge Cases

- IF `owner`/`repo`/`org` combinations or other ingestion-side concerns arise THEN they're out of this feature's scope - covered by `streaming-ingestion/spec.md` (ING-04)
- IF a message's `schema_version` is not `1` THEN the job SHALL log and skip it (covered, P1 Normalization AC6)
- IF a message is not valid JSON THEN the job SHALL log and skip it (covered, same AC6)
- IF `org` is absent from the payload (private profile or org-less actor) THEN `org_id`/`org_login` SHALL be null in the normalized event, not omitted or fatal (covered, GitHub-specific fields block table)
- IF an event's `source_event_type` isn't yet in the Normalization Mapping table THEN the event SHALL still publish with an empty GitHub-specific block, not be dropped (covered, P1 Normalization AC5)
- IF `SponsorshipEvent` traffic never appears during the P2 sample-capture window THEN it MAY stay on the empty-fallback path indefinitely, explicitly documented as a known gap (covered, P2 AC4)

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| FLK-01 | P1: Local stack infrastructure (topic provisioning) | - | Pending |
| FLK-02 | P1: Local stack infrastructure (ingestion container) | - | Pending |
| FLK-03 | P1: Local stack infrastructure (manual ingestion path retained) | - | Pending |
| FLK-04 | P1: Local stack infrastructure (Flink cluster) | - | Pending |
| FLK-05 | P1: Local stack infrastructure (startup ordering) | - | Pending |
| FLK-06 | P1: GitHub event normalization (11-type mapping) | - | Pending |
| FLK-07 | P1: GitHub event normalization (envelope fields) | - | Pending |
| FLK-08 | P1: GitHub event normalization (partition key) | - | Pending |
| FLK-09 | P1: GitHub event normalization (timestamp conversion) | - | Pending |
| FLK-10 | P1: GitHub event normalization (unmapped-type fallback) | - | Pending |
| FLK-11 | P1: GitHub event normalization (malformed-message handling) | - | Pending |
| FLK-12 | P1: GitHub event normalization (pluggable Normalizer architecture) | - | Pending |
| FLK-13 | P2: Coverage extension (sample capture) | - | Pending |
| FLK-14 | P2: Coverage extension (mapping table update) | - | Pending |
| FLK-15 | P2: Coverage extension (fallback resolved per type) | - | Pending |
| FLK-16 | P2: Coverage extension (SponsorshipEvent escape valve) | - | Pending |

**ID format:** `FLK-NN`

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 16 total, 0 mapped to tasks yet, 0 unmapped (Design/Tasks phases still ahead)

---

## Success Criteria

- [ ] `docker compose up` brings up Kafka (with `events-raw`/`events-normalized` explicitly provisioned), the ingestion service, and a Flink cluster, all from one command
- [ ] The PyFlink job normalizes all 11 mapped GitHub event types into `events-normalized`, verified against real captured traffic for the 4 types that were already sampled and against docs-plus-verified-nested-shapes fixtures for the other 7
- [ ] `events-normalized` messages are keyed by `repo_name`
- [ ] The remaining 6 event types are mapped from real traffic (or `SponsorshipEvent` is explicitly documented as an open gap)
