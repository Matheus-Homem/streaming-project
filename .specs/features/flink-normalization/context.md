# Flink Normalization Context

**Gathered:** 2026-08-10
**Spec:** `.specs/features/flink-normalization/spec.md`
**Status:** Ready for design

---

## Feature Boundary

Build the first Flink consumer of `events-raw`: a PyFlink job that flattens raw GitHub event payloads into a common, flat shape on a new `events-normalized` topic. Because nothing downstream of ingestion exists yet, this feature also formalizes how the local stack comes up: explicit Kafka topic provisioning (replacing broker auto-creation), the ingestion service as a docker-compose service (replacing the manual-only `make ingestion-default` path), and a standalone Flink cluster in the same compose file. Aggregation/windowing, OpenSearch, and Grafana are explicitly the next feature, not this one.

---

## Implementation Decisions

### Flink job language and API

- PyFlink (Python), not Java/Scala. Matches the rest of the repo's stack (Pydantic, black/isort, unittest) and keeps the learning curve on Flink concepts rather than a new language at the same time.

### Flink deployment topology

- Standalone Flink cluster (JobManager + TaskManager) as containers in `infra/docker/docker-compose.yml`, alongside Kafka. Mirrors the pattern already established for Kafka; gives a Flink Web UI to observe running jobs, matching what Kafka UI already does for the broker.

### Kafka topic provisioning

- Both `events-raw` (today auto-created) and the new `events-normalized` get explicit provisioning via docker-compose (init step / one-shot service running `kafka-topics --create`), instead of relying on broker auto-creation.
- Config for both: 3 partitions, replication factor 3, 7-day retention. Local-dev defaults, chosen to match the 3-broker cluster; easy to revise later. Not explicitly re-confirmed with the user beyond this session - logged as an assumption in spec.md.

### Ingestion service runtime

- The Python ingestion app becomes a new docker-compose service (own Dockerfile), so the whole local pipeline (Kafka + ingestion + Flink) comes up with one command and Flink always has live data to consume.
- `make ingestion-default` (`python -m ingestion.app --source github`) stays working as a manual, outside-compose path for local debugging.

### Normalization depth and schema shape

- Full per-`GitHubEventType` flattening, not just the common envelope - the user chose depth over an MVP-minimal envelope-only approach.
- Nested objects (issue, pull_request, comment, review, release, forkee, member, discussion) are flattened to a **curated subset** per object (identifying/useful fields: id, number, title/body, state, `user.login`, timestamps) - not a full recursive flatten of every field (drops `avatar_url`, `gravatar_id`, redundant `*_url` fields as noise).
- Timestamps (`created_at`, `observed_at`) convert to epoch millis in the normalized output, since Flink event-time/watermark handling (needed by the next feature, windowed aggregation) works natively with epoch millis - resolving this now avoids a schema revisit later.

### Architecture: pluggable normalizer (AD-004)

- The Flink job calls the `NormalizationEngineBase` port (ABC), mirroring `ingestion/ports.py`'s `IngestionClientBase`/`IngestionEngineBase`/`IngestionProducerBase` pattern. Per `AD-006` the only concrete implementation is the source-agnostic `NormalizationEngine`; per-source knowledge lives in YAML contracts, not in a class per source.
- Triggered by the user's long-term intent to grow this project into an API-agnostic real-time ingestion/analytics platform (register arbitrary APIs - not just GitHub/GitLab - and stream/aggregate their events). Recorded as a formal project-wide decision in `.specs/STATE.md` (`AD-004`), not just a local choice for this feature.
- Consequence for the normalized schema: it splits into a **domain-neutral envelope** (`source`, `event_id`, `event_type`, `actor`, `observed_at`, `schema_version`, `partition_key`) that any future source would populate the same way, plus a **source-specific fields block** where GitHub's `repo_name`, `repo_id`, `org_login`, and all the per-event-type curated fields live. The envelope never encodes VCS-specific vocabulary at the top level.

### Partition key

- `events-normalized` is keyed by the generic `partition_key` field; the GitHub contract's `partition_key` rule populates it from `repo.name`. Resolves the "message key" item the `streaming-ingestion` spec explicitly deferred ("must be decided before Flink windowing... relies on ordering per key") - this is that moment, ahead of the next feature (windowed aggregation) needing it.

### Event type coverage sequencing

GitHub has 17 `GitHubEventType` values. Coverage splits by how the field mapping was sourced:

- **4 verified from real traffic** (a local capture, untracked by `.gitignore`'s `*_sample.*`; the four events themselves are inlined in `tests/fixtures/events.py` as `SAMPLE_SHAPED_GITHUB_EVENTS`): `PullRequestEvent`, `IssueCommentEvent`, `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`.
- **7 resolved from official GitHub Events API docs**, cross-checked against sample-verified nested shapes (`issue`, `user`, `label` objects already known from the 4 verified types): `WatchEvent`, `CreateEvent`, `DeleteEvent`, `PublicEvent`, `GollumEvent`, `IssuesEvent`, `MemberEvent`.
- **6 remain unresolved** and are explicitly sequenced as a follow-up story within this same feature, not deferred to a separate feature: `PushEvent`, `ForkEvent`, `ReleaseEvent`, `DiscussionEvent`, `CommitCommentEvent`, `SponsorshipEvent`. Official docs proved unreliable for these (the Events API docs for `PushEvent` omit fields the user has seen in real traffic before; `SponsorshipEvent` isn't documented at all on the Events API reference page checked). The story captures real traffic samples first (same method that produced the original local capture), then maps from verified data - avoiding a repeat of the doc-vs-reality mismatch already caught during this session (webhook docs were initially and mistakenly used for `PushEvent` instead of Events API docs).
- Until the 6 are mapped, events of those types still flow through: envelope populated, source-specific fields block empty/null (see Declined/Assumptions below) - not dropped.

### Agent's Discretion

- Exact module/directory layout under `flink/` (mirroring `ingestion/`'s `adapters/`/`ports.py`/`use_case.py` shape or an equivalent PyFlink-appropriate structure) - left to Design.
- Exact `kafka-topics --create` invocation mechanics in docker-compose (init container vs one-shot service vs entrypoint script) - left to Design.

### Declined / Undiscussed Gray Areas → Assumptions

- **Fallback shape for the 6 not-yet-mapped event types**: envelope fields populated, source-specific fields block empty/null, event still published (not dropped). Chosen by the agent, not explicitly asked - low-stakes and reversible, follows the "don't lose data" bias already established in ingestion (`GitHubEvent` validation failures are logged and skipped, but only for genuinely invalid payloads, not merely under-mapped ones).
- **Malformed message / unknown `schema_version` handling** in the Flink job: log and skip, without crashing the job. Mirrors `ingestion/adapters/engine.py`'s existing pattern for invalid `GitHubEvent` payloads.
- **Processing semantics**: at-least-once is sufficient for this feature (the normalizer is a stateless map; reprocessing the same message yields the same output). Exactly-once semantics are deferred to whenever stateful aggregation lands.
- **Flink job resilience/checkpointing hardening** (restart strategies, failure recovery tuning): out of scope for this feature, mirroring how `streaming-ingestion`'s resilience work (P2) followed its initial P1 rather than shipping together.

---

## Specific References

- `AD-004` in `.specs/STATE.md` is the authoritative record of the API-agnostic-platform intent driving the pluggable-normalizer decision - read it before designing any future source integration.
- The 4 verified event type mappings are backed by real captured traffic. The capture file itself is a local artifact and never enters the repo (`.gitignore`'s `*_sample.*`), so the events that tests need live inlined in `tests/fixtures/events.py` (`SAMPLE_SHAPED_GITHUB_EVENTS`); the 6-type follow-up story needs a fresh capture that actually contains those types, inlined the same way.

---

## Deferred Ideas

- Registering arbitrary non-VCS APIs (weather, monetary systems, etc.) as new sources - the platform-agnostic vision behind `AD-004`. Not built in this feature or the next; `AD-004` only sets the architectural principle so it's cheap to add later.
- Windowed aggregation over `events-normalized` - explicitly the next feature after this one.
- OpenSearch + Grafana - unaffected by this feature, still future work.
- Flink job resilience/checkpointing hardening - noted above, future feature mirroring `streaming-ingestion`'s P1→P2 split.
- **Session cluster + submitter service** instead of Application Mode: decided during Design (2026-08-10) to build Application Mode first (job embedded in the JobManager's `standalone-job` entrypoint - simplest path to "one command brings up everything"). The user explicitly wants this recorded as a known improvement opportunity: a session cluster (JobManager/TaskManager as a generic, reusable cluster) plus a one-shot `job-submitter` service (same wait-then-run-once pattern as `topic-init`) submitting via `flink run -py`/REST - closer to how a real Flink cluster is usually operated, and reusable if a second job ever needs the same cluster. See `flink-normalization/design.md`'s Tech Decisions table.
