# Flink Aggregation Specification

## Problem Statement

`events-normalized` carries one row per GitHub event, keyed by `repo_name`. Nothing yet turns that
stream into a metric someone could put on a dashboard. This feature adds the aggregation stage
between `events-normalized` and the future OpenSearch/Grafana layer: a Flink SQL job that windows
the normalized stream and publishes counted results to a new `events-aggregated` topic.

Per `AD-009`, this stage has no YAML contract - the Flink SQL query itself, as a `.sql` file, is the
declarative contract. `AD-006`'s Table API/SQL direction for aggregation is unchanged; what changed
is that nothing compiles *to* the SQL - it is authored directly.

## Goals

- [x] A running Flink SQL job counts `events-normalized` rows per `repo_name` per 5-minute tumbling
      window (event time) and publishes one row per closed window to `events-aggregated`.
- [x] The job survives a restart without losing an in-flight window's partial state (checkpointing).
- [x] Publishing to `events-aggregated` is exactly-once - no duplicate window result after a restart.

## Out of Scope

| Feature | Reason |
| --- | --- |
| Event counts broken down by `event_type` | Not selected as P1 metric; shape (composite key? separate query?) not decided - future increment |
| Distinct-actor count per repo/window | Different aggregation class (`COUNT(DISTINCT entity_id)`, larger state) - future increment |
| Global event count (unkeyed, no `repo_name`) | Not selected as P1 metric - future increment |
| OpenSearch / Grafana consumption of `events-aggregated` | Separate planned stage per `CLAUDE.md`'s Architecture section - own feature |
| DDL for `events-normalized` derived from `flink/normalization/sources/github.yml` | `PLATFORM.md`'s "Aggregation is SQL" section leaves this as a per-feature call; P1's query only touches envelope fields, so a hand-written `CREATE TABLE` is enough - revisit if a future query needs `event_types` columns |
| Sliding/hopping windows, side-output for late data | P1 uses tumbling windows with late data dropped (see Assumptions) |
| A generalized "add a new aggregation query" mechanism | Each aggregation query is its own `.sql` file / job per `AD-009`; no framework for registering new ones is built here |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Window size / watermark bound | 5-minute tumbling window, event time, 30s bounded out-of-orderness | User decision - granular enough to observe locally without producing mostly-empty windows at GitHub's event volume | y |
| Late-data policy | Late events (past the watermark) are dropped, not routed to a side output | User decision - keeps the query and the output shape simple; side-output routing is real complexity deferred until a concrete need for it shows up | y |
| Checkpointing | Enabled in P1 | User decision - the job must survive a restart without losing in-flight window state | y |
| Delivery guarantee | Exactly-once, via a transactional Kafka sink coordinated with the checkpoint | User decision | y |
| Malformed / schema-incompatible row from `events-normalized` | Skipped, logged, job keeps running (`json.ignore-parse-errors`-style behavior) | User decision - normalization already validates its output; a row that doesn't fit here most likely signals an upstream bug worth noticing in logs, not a reason to take the whole job down | y |
| Output row shape | JSON: `repo_name`, `window_start`, `window_end`, `event_count` | User decision - mirrors `events-normalized`'s plain-JSON convention; `repo_name` doubles as the Kafka record key via the same `key_field` pattern `KafkaSinkParams` already uses | y |
| Source table columns | `CREATE TABLE` for `events-normalized` declares only the envelope fields P1's query needs (`repo_name`, `event_time`) - not a union of every `event_types` field across all 11 GitHub event types | Agent default - mechanical/plumbing (a DDL is a config shape, not a trade-off); revisit if a future query needs per-type fields | n |
| Checkpoint interval | 60 seconds | Agent default - a starting point balancing recovery-point recency against checkpoint overhead for a low-volume local stream; not a product decision, tune during Design/Execute if it proves wrong | n |
| State backend | Flink's default (`HashMapStateBackend`, on-heap) | Agent default - P1's state (one count per open window per repo) is small; RocksDB only pays off at a state size this feature doesn't reach | n |
| Kafka transaction timeout vs. broker's `transaction.max.timeout.ms` | To be verified against `infra/docker/docker-compose.yml`'s actual broker config during Design/Execute | Exactly-once Kafka sinks are a known source of a specific runtime failure (Flink's transaction timeout exceeding the broker's allowed maximum) - flagged here so it isn't discovered live; not resolved yet because it requires reading the running broker's config, not a product decision | n |

**Open questions:** none - all resolved or logged above.

---

## Implicit-Requirement Dimensions Sweep

| Dimension | Coverage |
| --- | --- |
| Input validation & bounds | AC-covered - malformed/incompatible row from `events-normalized` is skipped and logged (P1 AC6) |
| Failure / partial-failure states | AC-covered - checkpointing (P1 AC7) and exactly-once sink recovery (P1 AC8) |
| Idempotency / retry / duplicate handling | AC-covered - exactly-once delivery guarantee (P1 AC8); the aggregation itself (`COUNT` per window) is naturally idempotent under reprocessing even before that guarantee |
| Auth boundaries & rate limits | N/A - internal pipeline stage, no external caller, no auth surface |
| Concurrency / ordering | AC-covered - `repo_name`-keyed grouping partitions naturally across `events-normalized`'s existing 3 Kafka partitions (P1 AC1); `COUNT(*)` has no ordering dependency within a window |
| Data lifecycle / expiry | `events-aggregated` gets the same `retention.ms=604800000` (7 days) as the other two topics per the existing `create-topics.sh` convention (P1 AC9) |
| Observability | Flink's own JobManager UI (already exposed on `:8081` for the normalization job) covers job/checkpoint metrics; a skipped-row log line covers malformed input (P1 AC6). No new observability tooling built here |
| External-dependency failure | N/A beyond what Kafka's own client defaults already handle (connection retry) - no new external dependency introduced |
| State-transition integrity | AC-covered - a window's lifecycle (open → watermark passes → emit once → closed) is the state machine; P1 AC3-AC6 pin its behavior |

---

## User Stories

### P1: Repo activity counts per window ⭐ MVP

**User Story**: As someone watching the pipeline's output, I want a count of GitHub events per
repository per 5-minute window so that I can see which repositories are active, without reading raw
`events-normalized` traffic by hand.

**Why P1**: This is the smallest end-to-end slice of the aggregation stage - one metric, one window
type, one sink - that proves out Flink SQL windowing, watermarks, and exactly-once Kafka publishing
in this codebase for the first time. Every later aggregation query (by type, distinct actors, etc.)
reuses the same job shape.

**Acceptance Criteria**:

1. WHEN the job starts THEN the system SHALL read `events-normalized` as a Flink SQL source table
   keyed by `repo_name`, with `event_time` as the event-time attribute.
2. The system SHALL apply a bounded-out-of-orderness watermark of 30 seconds on `event_time`.
3. WHEN a 5-minute tumbling window (event time) closes for a given `repo_name` THEN the system SHALL
   publish exactly one row to `events-aggregated` containing `repo_name`, `window_start`,
   `window_end`, and `event_count` (the number of `events-normalized` rows for that `repo_name` whose
   `event_time` fell inside the window).
4. WHILE no `events-normalized` row exists for a given `repo_name` in a window, the system SHALL NOT
   emit a row for that `repo_name` in that window (no zero-count rows).
5. IF an event's `event_time` falls behind the current watermark when it arrives THEN the system
   SHALL exclude it from any window's count and SHALL NOT fail the job.
6. IF a row from `events-normalized` cannot be parsed against the declared source table schema THEN
   the system SHALL skip that row, log it, and continue processing.
7. The system SHALL checkpoint its state at a regular interval so that a job restart resumes without
   losing an in-flight window's partial count.
8. The system SHALL publish to `events-aggregated` with an exactly-once delivery guarantee - a job
   restart SHALL NOT result in a duplicate row for a window already published before the restart.
9. The system SHALL publish `events-aggregated` records keyed by `repo_name` (the Kafka record key,
   not only a JSON field).

**Independent Test**: Publish a handful of hand-crafted `events-normalized` messages spanning two
5-minute windows and two repos (including one message with `event_time` late enough to miss its
window's watermark, and one malformed message), run the job, and inspect `events-aggregated` in
Kafka UI - correct counts per repo/window, the late event absent from any count, the malformed
message skipped without killing the job, and a duplicate check by restarting the job mid-stream and
confirming affected windows are not re-published.

---

## Edge Cases

- IF the `events-normalized` topic is empty when the job starts THEN the system SHALL start cleanly
  and emit nothing until data arrives (no error on an empty source).
- IF two events for the same `repo_name` share the exact same `event_time` THEN the system SHALL
  count both toward the same window (no dedup by timestamp).
- WHEN the job restarts from a checkpoint mid-window THEN the system SHALL resume that window's count
  from the checkpointed state, not from zero.
- IF the Kafka transaction timeout configured for the sink exceeds the broker's
  `transaction.max.timeout.ms` THEN the job SHALL fail fast at startup with a clear error, not at an
  arbitrary point later in the stream (verify the actual broker config during Design/Execute per the
  open assumption above).

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| FLA-01 | P1: Repo activity counts per window (topic provisioning) | T1 | Verified |
| FLA-02 | P1: Repo activity counts per window (source table DDL) | T2, T6 | Verified |
| FLA-03 | P1: Repo activity counts per window (watermark strategy) | T2, T6 | Verified |
| FLA-04 | P1: Repo activity counts per window (windowed aggregation query) | T2, T6 | Verified |
| FLA-05 | P1: Repo activity counts per window (late-data drop) | T2, T6 | Verified |
| FLA-06 | P1: Repo activity counts per window (malformed-row handling) | T2, T6 | Verified |
| FLA-07 | P1: Repo activity counts per window (checkpointing) | T3, T6 | Verified |
| FLA-08 | P1: Repo activity counts per window (exactly-once sink) | T2, T6 | Verified |
| FLA-09 | P1: Repo activity counts per window (keyed output) | T2, T6 | Verified |

**ID format:** `FLA-NN`

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 9 total, 9 mapped to tasks, 9 Verified (2026-08-25 - T6 live verification: FLA-09 agent-observed via Kafka UI screenshot, FLA-02/03/04/05/06/07/08 user-attested in chat, not independently observed by the agent - see `validation.md`'s evidence-tier note; consistent with this feature's documented live-only test strategy, Test Coverage Matrix in `tasks.md`)

---

## Success Criteria

- [x] `events-aggregated` receives one row per `repo_name` per closed 5-minute window, matching a
      hand-verified count against the source messages used in the Independent Test.
- [x] A job restart mid-window does not lose the in-flight window's partial count and does not
      duplicate an already-published window's row.
- [x] A malformed `events-normalized` message does not stop the job.
