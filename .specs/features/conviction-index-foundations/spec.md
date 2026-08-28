# Conviction Index Foundations Specification

## Problem Statement

`.specs/RFC-conviction-index.md` names five platform capabilities the pipeline needs before a
Conviction Index query can be written, none of which exist today: typed normalization fields, a
per-source watermark declaration, an envelope that tolerates sources with no actor, ingestion for
single-object/no-id/no-type responses, and a push transport. `tmp/platform-refactor-plan-v2.md`
independently arrived at two of the same gaps (field types, stream properties) from the platform
refactor's own inventory (F2). A sixth capability - a value-lookup transform in the normalization
DSL - was added after a follow-up gap analysis (2026-08-28, see below): the RFC's own join needs a
way to reconcile three exchanges' three different symbol formats onto one asset key, which nothing
in P1-P5 provides. This feature is the union of all of that: every piece of new Python and every
contract-format change needed to make the five new sources (Binance spot, Coinbase, Kraken,
mempool.space, Binance Futures) *representable* by the platform - stopping short of writing those
five sources' own `interface/sources/*/` files, the join query itself, OpenSearch, and Grafana,
all of which are configuration-only or separate stages once this lands.

**Formalized order** (user decision, 2026-08-27, extended 2026-08-28): this spec carries six
stories, P1-P6, in the order below. They are meant to be picked up **in this order** as separate
Design/Tasks/Execute passes - the next time "what's the next feature" comes up before all six are
done, the answer is the next `Pending` story here, not a fresh planning conversation.

| Story | One line | Depends on |
| --- | --- | --- |
| P1 | Normalization contract gains explicit field types; `as:` is replaced | none |
| P2 | Per-source stream properties (watermark lateness, idle timeout) | none (independent of P1) |
| P3 | Domain-neutral envelope tolerates sources with no actor (`entity_id`/`entity_name` optional) | none |
| P4 | Ingestion supports single-object responses and sources with no id/type field | none |
| P5 | A push/WebSocket ingestion transport, alongside the existing poll transport | P4 (shares the synthesized-id/type and non-array plumbing a WS source also needs for reuse of `RawEventFormatter`) |
| P6 | Normalization DSL gains a `map:` value-lookup transform (symbol → asset key) | P1 (the mapped field's output still declares a `type:`) |

**Deliberately left out of this spec** (2026-08-28 gap analysis, user decision): a pluggable
rate-limit strategy (RFC §5.1 item 6 - flagged as *potentially* blocking for `mempool.space`'s plain
`429`, but not built here) and a pluggable dedup policy for snapshot/push sources (RFC §5.1 item 7).
Both are self-contained platform capabilities that *could* have been added the same way P6 was, but
the user chose not to fold them in - they wait until a real source's config makes the need concrete.
Also out: the watermark spike itself and the shared-vs-per-source topic decision, both inherently
empirical (need real multi-cadence data to answer), not code this spec could pre-build.

## Goals

- [ ] Every field rule in a normalization contract declares an explicit destination type; `as:` no
      longer exists as a contract key
- [ ] A source's normalization contract declares how late its events may arrive and how long an idle
      partition holds the watermark, instead of that value being hand-written into an analytics `.sql`
- [ ] `NormalizedEvent` no longer requires `entity_id`/`entity_name` - a source with no actor
      concept can normalize without synthesizing a dishonest value
- [ ] Ingestion can represent a source whose response is a single JSON object and/or has no
      resolvable id/type field, without inventing a value that isn't in the payload
- [ ] A source that only offers a push transport (WebSocket) can be ingested through the same
      `RawEvent` → `events-raw` pipeline poll sources already use, reusing formatting/dedup/publishing
      rather than duplicating it
- [ ] A normalization field rule can substitute a source-specific raw value (an exchange's own
      symbol string) through a declared lookup table into a shared vocabulary value, so multiple
      sources can be joined on one common key

## Out of Scope

| Feature | Reason |
| --- | --- |
| The five Conviction Index sources' own `interface/sources/*/{ingestion,normalization}.yml` | Configuration-only once P1-P5 land, per `AD-006` - a separate pass, not a platform capability |
| `interface/analytics/conviction_index_5m.sql` (the join, the classification logic, thresholds) | Depends on the watermark spike (RFC §4.3/§5.3-Q2) and on real data flowing from the five sources - cannot be written before they exist |
| OpenSearch, Grafana | Explicitly excluded by the user this session - separate, larger stages, RFC-required but not part of this feature |
| Generating Flink SQL `CREATE TABLE`/views from normalization contracts (`platform-refactor-plan-v2.md` Phase 4-6) | A different feature (`analytics-view-generation`, F4) - this feature only makes the contract able to *carry* the information a generator would need later |
| Wiring stream properties (P2) into the analytics job's actual watermark strategy | No generator exists yet to consume it (same reason as above) - P2 only makes the value declarable and validated |
| A pluggable rate-limit strategy per source (mempool's plain-429, Binance's weight-based accounting) | RFC §5.1 item 6, not touched by any of P1-P5 - the existing GitHub-shaped `_is_rate_limited` is left as-is; the five sources' rate-limit config is part of their own `ingestion.yml` pass, later |
| A dedup policy change for snapshot/push sources | RFC §5.1 item 7 / §5.3-Q8 - `InMemoryDuplicateTracker` is reused unmodified for P5; whether it's the right policy for a non-replaying push stream is a question for when a real WS source is configured, not resolved here |
| Multi-endpoint composition (e.g. combining Binance Futures' two endpoints into one observation) | RFC §5.1 item 8 names the SQL-join route as "probably right" and needing no new code - not this feature's concern |
| The exact reconnect/backoff algorithm's tuning (retry counts, backoff curve shape) | Real values, not a platform capability - decided per adapter in Design/Execute for P5, using the connection rules already documented in the RFC (24h Binance disconnect, 5s Coinbase subscribe window, ping/pong) |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Story order and independence | P1-P6 as listed above; P5 depends on P4, P6 depends on P1, the rest are independent of each other | User decision, 2026-08-27/28 - fixes the answer to "what's next" without re-litigating priority each session | y |
| `map:` transform's declaration shape | An inline dict on the `FieldRule` itself (`map: {BTCUSDT: BTC, XBTUSD: BTC}`), not a reference to a shared external lookup file | Agent default - mechanical/plumbing (a config shape, not a trade-off); a shared cross-contract lookup file is a real extension but nothing in P1-P6 needs one yet - only per-field, per-contract substitution | n |
| `map:` mutual exclusivity with other `FieldRule` keys | `map:` requires `from:` (or `take:`, applied to the plucked scalar) and is mutually exclusive with `expression:` - the same shape restriction `take:`/`as:` already had relative to `expression:` | Agent default - P6's actual need (RFC §5.3-Q7) is a plain scalar substitution; combining `map:` with `expression:`'s arbitrary JMESPath is unneeded complexity not asked for | n |
| Unmapped raw value at runtime (a symbol not in the table) | Resolves to `None`, composing with `default:` exactly like every other transform - no contract-load-time validation of "every possible value is covered" (the table is static, the real-world values it must cover are not knowable at authoring time) | Consistent with the project's existing null-tolerant philosophy (`PLATFORM.md`: JMESPath returns null on a missing path rather than raising) - an unrecognized symbol degrades to a null joinable key instead of failing the whole event | n |
| `type:` is mandatory on every `FieldRule` | No inference tier - every field (`partition_key`, `envelope`, `common`, `event_types`) declares `type:` explicitly | User decision - no ambiguity, no two-sources-of-truth with `as:`, migration of `github.yml` is mechanical (visit each field once) | y |
| `as:` is removed from the contract format entirely | `FieldRule` no longer accepts `as:` (`extra="forbid"` already rejects unknown keys, so this is enforced for free once the field is deleted from the model) | User decision | y |
| How `type: BOOLEAN` and presence-derived booleans coexist | Two distinct type tokens: `type: BOOLEAN` compiles to a direct pass-through of the JSON value (today's `public` field - no transform); `type: PRESENCE` compiles to today's `as: boolean` behavior (`from != null`), and is representable downstream as `BOOLEAN` | User decision, 2026-08-27, resolving a real ambiguity found while specifying: `public` (native JSON boolean) and `issue_is_pull_request` (presence-of-field boolean) cannot share one token without the compiler guessing from the data shape | y |
| How `type: TIMESTAMP` replaces `as: timestamp` | `type: TIMESTAMP` always compiles to `iso_to_millis(...)` (today's `as: timestamp` behavior); its physical/downstream representation is `BIGINT` | No ambiguous case exists for this one the way `BOOLEAN` had - every `as: timestamp` field today is an ISO string needing conversion | y |
| Exact type-token vocabulary and its grammar (scalar names, `ARRAY<T>` / `ROW<field: TYPE, ...>` nesting syntax, how deep nesting is parsed) | Deferred to Design - this spec fixes the *capability* (must express scalars, arrays of scalars, and the `GollumEvent.pages`-shaped array-of-objects case), not the literal grammar | Mechanical/plumbing once the capability is fixed - a Design-phase call per `.claude/mentor-design-pairing.md`'s "config file shape" carve-out, not a product trade-off | n |
| `default: null` vs. no `default` declared | Becomes distinguishable: `FieldRule` must expose whether a `default` key was present in the source YAML at all (independent of its value), not just whether the resolved value is `None` | User decision, 2026-08-27 - matters once `type:` exists, because "no default" and "explicit null default" are different nullability statements a future DDL generator needs to tell apart; the mechanism (e.g. Pydantic's `model_fields_set`) is a Design-phase call | y |
| Where per-source stream properties live | A new top-level `stream:` block in `interface/sources/<name>/normalization.yml`, co-located with the rest of that source's normalization contract | Matches the existing co-location convention `interface-layout` set (ingestion + normalization already share a directory); a lateness bound is a property of how a source's events arrive, which is what the rest of that file already describes | n |
| Stream property keys and defaults | `watermark_lateness_seconds` (required, no default - every source must state this explicitly since it varies by source), `idle_timeout_seconds` (optional, `null` = no idleness timeout configured) | Matches the two concrete needs named in `platform-refactor-plan-v2.md` Part I and RFC §5.3-Q2/Findings; exact validation bounds (min/max seconds) left to Design | n |
| Whether `NormalizedEvent.event_time`/`partition_key` also become optional | No - only `entity_id`/`entity_name` loosen. `event_time` and `partition_key` stay required (every event still needs a watermark timestamp and a Kafka key; even a mempool snapshot has `RawEvent.observed_at` to derive `event_time` from, and a synthetic constant is an honest `partition_key` for a source with no natural partitioning dimension) | Confirmed by the user selecting only the envelope item, not a broader "everything optional" one | y |
| How `EventNormalizer`/`FlinkNormalizationPipeline` build `NormalizedEvent` once `entity_id`/`entity_name` are optional | Reads them from the evaluated contract via `.get(..., None)` instead of direct subscript; a contract that omits `envelope.entity_id`/`entity_name` entirely (not just resolves to `null`) is valid | Direct consequence of the fields becoming `Optional` - the current `KeyError`-on-missing-envelope-key behavior (`domain/normalizer.py:34-37`) no longer applies to these two keys specifically | n |
| Ingestion's non-array-response declaration | A new optional `ingestion.yml` key naming the dotted path to the event list within the response body; its absence means the whole response body is one event, wrapped into a single-element list before the existing pipeline runs | Covers both shapes (`mempool.space`'s bare object, and any future API that wraps its list in an envelope) with one mechanism, per RFC §5.1 item 4 | n |
| Ingestion's synthetic id/type mechanism | Deferred to Design - this spec fixes the requirement (`id_field`/`type_field` become optional; their absence must not crash `SourceConfig.get_event_id`/`get_event_type`, and must not produce two "different" observations from the platform's point of view when they're really the same reading) | RFC §5.1 item 5 names three candidate mechanisms without picking one (timestamp-derived id, a declared constant, a `synthesize:` block) - a real design trade-off, not plumbing | n |
| `EventStreamPort`'s exact method shape (start/stop/generator vs. callback, how many `RawEvent`s one inbound WS frame may yield) | Deferred to Design - `flink-normalization`'s STATE.md history (2026-08-19 entry) records a concrete lesson here: `NormalizationSourceBase`/`NormalizationSinkBase` were sketched with method shapes that didn't match the real PyFlink API and had to be corrected against verified docs before implementation. The same discipline applies here against the real `websockets`/equivalent library API, not guessed | n |
| Which WebSocket client library | Deferred to Design/Execute - resolved via the Knowledge Verification Chain (Context7/web search for the current recommended async WS client) when P5 starts, not guessed now | n |
| Long-lived entrypoint shape (new script vs. extending `ingestion/app.py`'s `main()`) | Deferred to Design - a real structural decision (`.claude/mentor-design-pairing.md` gate applies) | n |

**Open questions:** none - all resolved above or explicitly deferred to Design with a stated reason.

---

## Implicit-Requirement Dimensions Sweep

| Dimension | Coverage |
| --- | --- |
| Input validation & bounds | AC-covered - P1 AC1-AC3 (type vocabulary + mandatory declaration), P2 AC1-AC2 (stream property bounds), P4 AC2 (id/type absence handled without crashing) |
| Failure / partial-failure states | AC-covered - P1 AC5 (`as:`/`type:` contradiction impossible by construction, key removed), P5 AC4-AC5 (WS disconnect/reconnect) |
| Idempotency / retry / duplicate handling | N/A for P1-P4 (no new retry surface); P5 explicitly defers the dedup-policy question (Out of Scope) - the existing `InMemoryDuplicateTracker` is reused unmodified, covered by P5 AC6 |
| Auth boundaries & rate limits | N/A - none of the five Conviction Index sources need auth for the endpoints named in the RFC; rate-limit strategy pluggability explicitly Out of Scope |
| Concurrency / ordering | AC-covered - P5 AC3 (a single inbound WS frame may yield zero-to-many `RawEvent`s, Kraken's batched-trade case), P5 AC7 (the long-lived entrypoint doesn't block the existing poll entrypoint - they're independent processes) |
| Data lifecycle / expiry | N/A - no new topic or retention policy introduced by P1-P5 |
| Observability | AC-covered - P5 AC5 (a reconnect attempt is logged, matching the existing per-class logger convention) |
| External-dependency failure | AC-covered - P5 AC4-AC5 (WS connection drop is not a fatal error - reconnect, not crash) |
| State-transition integrity | AC-covered - P5 AC4 (connect → subscribed → receiving → disconnected → reconnecting is the state machine P5 pins) |

---

## User Stories

### P1: Normalization contract gains explicit field types ⭐ MVP

**User Story**: As the platform, I want every field rule in a normalization contract to declare its
destination type so that a future DDL generator (and any human reading the contract) knows a
field's shape without inferring it from `as:`/`take:`/`expression:`, and so `ARRAY`/`ROW`-shaped
fields (today only expressible via the `expression:` escape hatch, like `GollumEvent.pages`) are
representable with a declared shape.

**Why P1**: Every later story that touches the normalization contract (P2's `stream:` block sits in
the same file; the Conviction Index's five new sources will all need this vocabulary) is easier to
build once the type system exists than to retrofit under them.

**Acceptance Criteria**:

1. The system SHALL require every `FieldRule` (in `partition_key`, `envelope`, `common`, and every
   `event_types` entry) to declare a `type:` - a contract missing `type:` on any field SHALL fail
   validation at contract-load time, not at evaluation time.
2. The system SHALL support, at minimum, the following type vocabulary: `STRING`, `BOOLEAN`,
   `PRESENCE`, `TIMESTAMP`, `BIGINT`, `INT`, `DOUBLE`, `ARRAY<T>` (for any scalar `T`), and
   `ROW<field: TYPE, ...>` with at least one level of nesting (an `ARRAY<ROW<...>>`, covering
   `GollumEvent.pages`'s existing shape).
3. WHEN a field declares `type: PRESENCE` THEN the system SHALL compile it exactly as today's
   `as: boolean` (a presence check against its `from:` path, `!= null`), independent of the actual
   JSON type of the value at that path.
4. WHEN a field declares `type: TIMESTAMP` THEN the system SHALL compile it exactly as today's
   `as: timestamp` (ISO 8601 string → epoch milliseconds via `iso_to_millis`).
5. IF a contract declares an `as:` key on any `FieldRule` THEN the system SHALL reject the contract
   at load time (`as:` no longer exists in the format - this is `extra="forbid"`'s existing behavior
   once the field is removed from the model, not new logic).
6. WHEN a field declares `type: BOOLEAN` (not `PRESENCE`) THEN the system SHALL evaluate its `from:`/
   `take:`/`expression:` path and pass the resolved JSON value through unchanged (no presence
   coercion) - matching today's `public` field's untransformed behavior.
7. The system SHALL distinguish "no `default:` declared" from "`default: null` declared" on a
   `FieldRule`, exposing this distinction on the model (not only on the resolved value).
8. `interface/sources/github/normalization.yml` SHALL be migrated to the new vocabulary with
   identical `NormalizedEvent` output for every event type covered by
   `tests/flink/normalization/test_contracts_github.py` - this story changes the contract format,
   not any event's normalized shape.

**Independent Test**: Feed every fixture in `tests/fixtures/events.py` through the migrated
`github.yml` contract and confirm the produced `NormalizedEvent` for each is byte-for-byte
identical (field names, values, types) to what the pre-migration `as:`-based contract produced.

---

### P2: Per-source stream properties

**User Story**: As the platform, I want each source's normalization contract to declare how late its
events may arrive and how long an idle partition should hold back the watermark, so that value is
stated once per source instead of hand-written into every analytics query that reads that source.

**Why P2**: Independent of P1 - a config addition, not a type-system change. Needed before any
multi-source analytics query (Conviction Index's central risk, per RFC §4.3) can reason about
per-source watermark strategy instead of one blanket value.

**Acceptance Criteria**:

1. The system SHALL accept a `stream:` block in `interface/sources/<name>/normalization.yml`
   declaring `watermark_lateness_seconds` (a required positive integer).
2. The system SHALL accept an optional `idle_timeout_seconds` in the same block (a positive integer,
   or absent/`null` meaning no idleness timeout is configured for that source).
3. IF `interface/sources/<name>/normalization.yml` omits the `stream:` block entirely THEN the
   system SHALL fail contract validation with an error naming the missing block (no silent default -
   every source must state its own lateness bound, per the Assumptions table).
4. `interface/sources/github/normalization.yml` SHALL declare `stream: {watermark_lateness_seconds:
   30}`, matching `repo_counts_5m.sql`'s existing hardcoded `INTERVAL '30' SECOND` value (no
   behavior change to the running analytics job - this story only makes the value declarable
   alongside the source, per Out of Scope it does not wire it into the `.sql`).

**Independent Test**: Load `interface/sources/github/normalization.yml` through the updated
`NormalizationContract` model and assert `contract.stream.watermark_lateness_seconds == 30`. Load a
hand-crafted contract missing the `stream:` block and assert it fails validation.

---

### P3: Domain-neutral envelope tolerates sources with no actor

**User Story**: As the platform, I want `NormalizedEvent.entity_id`/`entity_name` to be optional so
that a source with no actor concept (a price tick, a mempool snapshot) can normalize honestly
instead of synthesizing a dishonest constant just to satisfy a required field.

**Why P3**: Independent of P1/P2 - a model + normalizer change. Named directly in
`flink-normalization/spec.md`'s P1 AC2 as the reason `entity_id`/`entity_name` were generalized
away from `actor_id`/`actor_login` in the first place; this is that anticipated case arriving.

**Acceptance Criteria**:

1. `NormalizedEvent.entity_id` and `NormalizedEvent.entity_name` SHALL be typed `Optional[str]`
   (default `None`), not required.
2. WHEN a normalization contract's `envelope` block declares `entity_id` and/or `entity_name` THEN
   the system SHALL evaluate and populate them exactly as before (no change for GitHub - this is an
   additive loosening, not a behavior change for existing sources).
3. WHEN a normalization contract's `envelope` block omits `entity_id` and/or `entity_name` entirely
   THEN the system SHALL normalize the event successfully with that field set to `None`, instead of
   raising `KeyError`.
4. `NormalizedEvent.partition_key` and `NormalizedEvent.event_time` SHALL remain required (unchanged
   from today) - only the two entity fields loosen, per the confirmed Assumption above.

**Independent Test**: A hand-crafted contract whose `envelope` block declares only `event_time`
(omitting `entity_id`/`entity_name`) normalizes a fixture event without raising, producing
`entity_id=None, entity_name=None` in the output. `tests/flink/normalization/test_contracts_github.py`
continues to pass unchanged (GitHub's contract still declares both).

---

### P4: Ingestion supports single-object responses and sources with no id/type field

**User Story**: As the platform, I want to configure a source whose API returns one JSON object
(not an array) and/or has no field identifying an individual event's id or type, so that
`mempool.space` and Binance Futures - which is every snapshot/polling source the Conviction Index
needs - are representable without inventing a shape they don't have.

**Why P4**: Independent of P1-P3 - an ingestion-side change. Prerequisite for P5's WebSocket
adapter to reuse `RawEventFormatter`/`SourceConfig` rather than duplicating id/type handling for
push sources that also lack a native id (documented in the dependency table above).

**Acceptance Criteria**:

1. WHEN `interface/sources/<name>/ingestion.yml` declares the (new, optional) events-list path key
   THEN the system SHALL extract the event list from that path in the response body instead of
   assuming the whole body is the array.
2. WHEN that key is absent THEN the system SHALL treat the entire response body as a single event,
   processing it as a one-element list through the existing pipeline (`RawEventFormatter`,
   dedup, publish) unchanged.
3. IF `interface/sources/<name>/ingestion.yml` omits `id_field` THEN the system SHALL synthesize
   `RawEvent.source_event_id` without raising, using a mechanism decided in Design (see Assumptions).
4. IF `interface/sources/<name>/ingestion.yml` omits `type_field` THEN the system SHALL synthesize
   `RawEvent.source_event_type` without raising, using a mechanism decided in Design.
5. WHEN both `id_field` and `type_field` are present (today's shape, e.g. `github`) THEN the system
   SHALL behave identically to today - this story is additive, not a rewrite of the existing path.

**Independent Test**: A hand-crafted `ingestion.yml` with neither `id_field` nor `type_field` and a
single-object mock response (shaped like `mempool.space`'s `/api/mempool`) produces exactly one
`RawEvent` with non-empty `source_event_id`/`source_event_type`, through the same
`IngestionPipeline.execute()` used today.

---

### P5: A push/WebSocket ingestion transport

**User Story**: As the platform, I want a source that only offers a push (WebSocket) transport to
flow into `events-raw` through the same `RawEvent` envelope and dedup/publish machinery poll
sources use, so ingestion has one internal representation regardless of transport.

**Why P5**: The largest of the five stories and the last in the order, since it depends on P4's
non-array/synthetic-id plumbing (a WS message is structurally closer to "one object, no wrapping
array, sometimes no id" than to GitHub's shape) and touches new domain (long-lived connections,
reconnect logic) `flink-normalization`'s own history flags as a place prior stub work drifted from
the real target API - Design must verify the real WS client library's API before any adapter code
is written, per the Knowledge Verification Chain.

**Acceptance Criteria**:

1. The system SHALL provide a port (e.g. `EventStreamPort`, exact name a Design call) alongside the
   existing `EventClientPort`, for sources with a push transport - per `AD-004`'s port-per-behavior
   principle, not a stretched `EventClientPort`.
2. The system SHALL provide at least one concrete adapter implementing that port over a real
   WebSocket connection: connect, send a subscribe frame, and yield inbound messages.
3. WHEN a single inbound WebSocket frame contains multiple trade/event records (Kraken's documented
   batching) THEN the system SHALL yield one `RawEvent` per record, not one `RawEvent` per frame.
4. WHEN the WebSocket connection drops (server-initiated close, network failure, or a
   contractually-expected disconnect such as Binance's 24-hour connection limit) THEN the system
   SHALL attempt to reconnect and resume receiving, rather than terminating the process.
5. WHEN a reconnect attempt occurs THEN the system SHALL log it (per-class logger, matching
   `shared/logger.py`'s existing convention) - this is the story's only new observability surface.
6. The system SHALL route every `RawEvent` produced from a push source through the existing
   `RawEventFormatter` → `DuplicateTracker` → `EventProducerPort` chain (`ingestion/use_case.py`),
   not a parallel one.
7. The system SHALL provide a long-lived entrypoint capable of running a push source continuously
   (connect once, receive indefinitely), distinct from the existing poll-sleep loop in
   `ingestion/app.py`'s `main()` - exact shape (new script vs. extended `main()`) a Design call.

**Independent Test**: Point the adapter at a real public WebSocket endpoint (e.g. Coinbase's
`matches` channel for `BTC-USD`, chosen for having no auth requirement) for a bounded window,
confirm `events-raw` receives one message per trade (or per batched record, for a source that
batches), and confirm a forced disconnect mid-window is followed by resumed messages without the
process exiting.

---

### P6: Normalization DSL gains a `map:` value-lookup transform

**User Story**: As the platform, I want a field rule to substitute a source-specific raw value
through a declared lookup table so that three exchanges' three different symbol formats
(`BTCUSDT`, `BTC-USD`, `BTC/USD`) can all normalize to one shared asset key (`BTC`), making a
cross-source join possible on that key.

**Why P6**: Named directly by RFC §5.3-Q7 as needed for the Conviction Index join and unmet by any
of P1-P5 - `take:`/`as:`/`type:` express shape and type, not value substitution, and hand-writing a
lookup as raw `expression:` JMESPath is exactly the "bad programming language written in YAML"
`PLATFORM.md` warns the escape hatch must not become. Depends on P1 only, since the mapped field's
output still declares a `type:` like any other field.

**Acceptance Criteria**:

1. The system SHALL accept a `map:` key on a `FieldRule`, an inline table of raw source value →
   normalized value (e.g. `map: {BTCUSDT: BTC, XBTUSD: BTC}`).
2. WHEN a field rule declares `map:` THEN the system SHALL resolve `from:`/`take:` first, then
   substitute the resolved value through the table before applying `type:`.
3. IF a resolved value has no entry in the declared `map:` table THEN the system SHALL fall through
   to `default:` if declared, or produce `None` otherwise - never raise.
4. `map:` SHALL be mutually exclusive with `expression:` on the same `FieldRule` (contract validation
   rejects both declared together), matching the existing `from:`/`expression:` exclusivity pattern.
5. `interface/sources/github/normalization.yml` SHALL be unaffected - `map:` is new capability, not a
   requirement on any existing field.

**Independent Test**: A hand-crafted contract with a field declaring
`map: {BTCUSDT: BTC, BTC-USD: BTC}` normalizes two fixture events - one carrying `BTCUSDT`, one
carrying `BTC-USD` - to the identical output value `BTC`. A third fixture carrying an unmapped
symbol (`ETHUSDT`) with no `default:` declared normalizes to `None` without raising.

---

## Edge Cases

- IF a `ROW<...>` type declaration nests a field whose own type is invalid (unknown token) THEN
  contract validation SHALL fail with an error identifying the offending nested field, not a generic
  parse failure (P1).
- IF `watermark_lateness_seconds` or `idle_timeout_seconds` is declared as zero or negative THEN
  contract validation SHALL fail (P2).
- IF a contract declares `entity_id` but omits `entity_name` (or vice versa) THEN the system SHALL
  normalize successfully, leaving only the omitted one `None` - the two fields loosen independently,
  not as a pair (P3).
- IF the events-list path declared in `ingestion.yml` (P4 AC1) resolves to something other than a
  list (e.g. the path is wrong and hits a nested object) THEN the system SHALL fail the same way an
  unresolvable `id_field`/`type_field` path fails today (surfaced, not silently swallowed) (P4).
- IF a WebSocket source's subscribe frame is not acknowledged within that exchange's documented
  window (e.g. Coinbase's 5 seconds) THEN the system SHALL treat it as a failed connection attempt
  and retry, not hang indefinitely (P5).
- IF a field rule declares both `map:` and `expression:` THEN contract validation SHALL fail (P6).

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| CVF-01 | P1: Mandatory `type:` on every field | Design | Pending |
| CVF-02 | P1: Type vocabulary (scalars, `ARRAY<T>`, `ROW<...>`) | Design | Pending |
| CVF-03 | P1: `type: PRESENCE` compiles as `as: boolean` did | Design | Pending |
| CVF-04 | P1: `type: TIMESTAMP` compiles as `as: timestamp` did | Design | Pending |
| CVF-05 | P1: `as:` removed, rejected if present | Design | Pending |
| CVF-06 | P1: `type: BOOLEAN` passes through untransformed | Design | Pending |
| CVF-07 | P1: `default:` declared vs. absent is distinguishable | Design | Pending |
| CVF-08 | P1: `github.yml` migrated, output unchanged | Design | Pending |
| CVF-09 | P2: `stream:` block, `watermark_lateness_seconds` | Design | Pending |
| CVF-10 | P2: optional `idle_timeout_seconds` | Design | Pending |
| CVF-11 | P2: missing `stream:` block fails validation | Design | Pending |
| CVF-12 | P2: `github.yml` declares `stream.watermark_lateness_seconds: 30` | Design | Pending |
| CVF-13 | P3: `entity_id`/`entity_name` become `Optional[str]` | Design | Pending |
| CVF-14 | P3: declared entity fields still evaluate as before | Design | Pending |
| CVF-15 | P3: omitted entity fields normalize to `None`, no `KeyError` | Design | Pending |
| CVF-16 | P3: `partition_key`/`event_time` stay required | Design | Pending |
| CVF-17 | P4: configurable events-list path in `ingestion.yml` | Design | Pending |
| CVF-18 | P4: absent path treats response as one event | Design | Pending |
| CVF-19 | P4: `id_field` optional, synthesized if absent | Design | Pending |
| CVF-20 | P4: `type_field` optional, synthesized if absent | Design | Pending |
| CVF-21 | P4: existing `id_field`+`type_field` sources unaffected | Design | Pending |
| CVF-22 | P5: `EventStreamPort` alongside `EventClientPort` | Design | Pending |
| CVF-23 | P5: concrete WebSocket adapter (connect, subscribe, yield) | Design | Pending |
| CVF-24 | P5: one frame may yield multiple `RawEvent`s | Design | Pending |
| CVF-25 | P5: reconnect on drop, no process termination | Design | Pending |
| CVF-26 | P5: reconnect attempts are logged | Design | Pending |
| CVF-27 | P5: reuses `RawEventFormatter`/`DuplicateTracker`/`EventProducerPort` | Design | Pending |
| CVF-28 | P5: long-lived entrypoint for continuous receipt | Design | Pending |
| CVF-29 | P6: `map:` key, inline lookup table | Design | Pending |
| CVF-30 | P6: `map:` resolves after `from:`/`take:`, before `type:` | Design | Pending |
| CVF-31 | P6: unmapped value falls to `default:` or `None` | Design | Pending |
| CVF-32 | P6: `map:`/`expression:` mutual exclusivity | Design | Pending |
| CVF-33 | P6: `github.yml` unaffected | Design | Pending |

**ID format:** `CVF-NN`

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 33 total, 0 mapped to tasks, 33 unmapped (Design phase not yet run)

---

## Success Criteria

- [ ] `github.yml` normalizes identically before and after P1-P3's contract-format changes (no
      regression in any existing test)
- [ ] A hand-crafted contract for a source with no actor and no per-type shape (mempool-like)
      validates and normalizes successfully end-to-end (P1-P4 composed)
- [ ] A real public WebSocket endpoint's messages reach `events-raw` as valid `RawEvent`s over a
      sustained window that includes at least one forced reconnect (P5)
- [ ] A field rule with a declared `map:` table normalizes two differently-formatted raw symbols to
      the same output value, and an unmapped symbol degrades to `None`/`default:` without raising (P6)
- [ ] Every P1-P6 requirement above reaches `Verified` in this table, or is explicitly deferred with
      a reason recorded here (not silently dropped)
