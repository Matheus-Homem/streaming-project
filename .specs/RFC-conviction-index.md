# RFC: Conviction Index

**Status**: Exploration. No spec, no design, no task list, no `.mentor/` mapping. Nothing in this
document is a committed decision - it exists to establish what the platform would have to become
to support this idea, and what is still unknown.

**Relationship to `.specs/RFC.md`**: that RFC states *why the project exists* (learning goals:
Kafka, Flink, OpenSearch, Grafana, architecture practice). This one states *what the platform
should be able to prove it can do*. It does not replace it; it gives it a concrete target.

---

## 1. The idea

A **Conviction Index**: a composite metric that classifies a crypto price movement as **real**
(on-chain network activity is elevated, leverage is contained) or **artificial** (price is moving
on its own, with leverage stretched and the network idle).

Three signals, crossed:

| Signal | Question it answers |
| --- | --- |
| Spot price across 3 exchanges | Is the price actually moving, and consistently across venues? |
| Bitcoin mempool congestion | Is the network backing the move - are people competing for block space? |
| Futures leverage | Is the move being pushed by leveraged positions rather than spot demand? |

The point is not three metrics on one dashboard. The *same price delta* means something different
depending on what the other two are doing **in the same window**:

- price up + mempool congested + funding rate near-neutral → plausibly organic demand
- price up + mempool idle + funding rate spiking → leverage-driven, thin conviction

That requires the three signals to be **co-present in one window, joined, before the classification
runs**. It is a join, not a fan-in.

### Why this is the right capstone for the platform

It exercises the platform's own claims rather than restating them:

- **`AD-006`** claims a new source is data (YAML), not a Python deploy. Two of the five sources
  test that claim positively; three test where it breaks.
- **`AD-004`** claims the envelope is domain-neutral. GitHub events all have an actor who did
  something. A price tick and a mempool snapshot do not. This is the first real challenge to that
  claim.
- **`AD-009`** claims the Flink SQL file *is* the aggregation contract. Today that contract is a
  `COUNT(*)`. A multi-source join with derived fields and filters is the first query that would
  make the claim mean something.
- The pipeline has only ever carried **one shape of event from one API**. Five sources across two
  transport models is the first time the "API-agnostic platform" framing gets tested.

---

## 2. What the platform expects today

Extracted from the repo's own specs and code, not from streaming practice in general.

### 2.1 Ingestion

Per `streaming-ingestion/spec.md` (P1 AC1-AC14, all Verified) and confirmed in code:

**Configuration format** - `interface/sources/<name>/ingestion.yml`, validated by `SourceYamlEntry`
(`ingestion/models.py`):

```yaml
endpoints:      # required: map of variant name -> URL template
  default: "https://..."
headers:        # required: NOT request headers - see below
  rate_limit_remaining: "X-RateLimit-Remaining"
  rate_limit_reset: "X-RateLimit-Reset"
auth:           # optional
  env_var: GITHUB_TOKEN
  header: Authorization
  value_template: "Bearer {token}"
id_field: "id"      # required: dotted path to the event's unique id
type_field: "type"  # required: dotted path to the event's type
```

**Assumptions baked into the contract** (each confirmed in code, file cited):

1. **The HTTP response body is a JSON array of events.** `RequestsClientAdapter._parse_response`
   returns `response.json()` typed `list[dict[str, Any]]`
   ([ingestion/adapters/client.py:47](ingestion/adapters/client.py#L47)), and
   `ValidatingRawEventFormatter._format_events` iterates it directly
   ([ingestion/domain/formatter.py:21](ingestion/domain/formatter.py#L21)). A single JSON object
   response is not a supported shape.
2. **Every event carries a resolvable id and type.** `SourceConfig.get_event_id` /
   `get_event_type` walk the declared dotted path and `raise ValueError` when it doesn't resolve
   ([ingestion/models.py](ingestion/models.py)); `EventModel` requires both as `str`.
3. **`headers:` is a rate-limit header-name map, not request headers.** Its only consumers are
   `SourceConfig.rate_limit_remaining` / `rate_limit_reset`, which index the dict directly
   (`self.headers["rate_limit_remaining"]`) - a missing key is a `KeyError`, not a default. Those
   properties are read by `RequestsClientAdapter._is_rate_limited`
   ([ingestion/adapters/client.py:26](ingestion/adapters/client.py#L26)), which is called on every
   response. **This is a misnomer in the config format worth noting** - a reader would reasonably
   expect `headers:` to mean HTTP request headers.
4. **Rate limiting is modelled as GitHub models it**: status 403/429 **and** a remaining-count
   header equal to `"0"`, with a reset-epoch header driving the backoff.
5. **Transport is pull, top to bottom.** `EventClientPort.get_events()` is one bounded call
   returning a list ([ingestion/ports.py](ingestion/ports.py)); `IngestionPipeline.execute()` runs
   `get_events → process → dedup → publish` once ([ingestion/use_case.py](ingestion/use_case.py));
   `app.py`'s `main()` is a `while True:` loop calling `execute()` then sleeping
   ([ingestion/app.py](ingestion/app.py)). There is no callback, listener, or long-lived-connection
   concept anywhere in the port or its one adapter.
6. **One process per source.** `--source` is a required CLI arg; `--poll-interval` is one value for
   the whole process.
7. **Dedup is by `source_event_id` within a run**, `InMemoryDuplicateTracker(120)`.

**Output**: `RawEvent` ([shared/models.py](shared/models.py)) - `source`, `source_event_id`,
`source_event_endpoint`, `source_event_type`, `observed_at` (UTC ISO 8601, set at ingestion time),
`schema_version=1`, `payload` (the whole source event dict). Published to the single shared
`events-raw` topic; `source` is what distinguishes events downstream, not the topic.

### 2.2 Normalization

Per `flink-normalization/spec.md` (P1 AC1-AC7) and `AD-006`:

**Contract format** - `interface/sources/<name>/normalization.yml`, validated by
`NormalizationContract` ([flink/normalization/models.py](flink/normalization/models.py)). Four
blocks, **all required**:

```yaml
source: <name>
partition_key: <FieldRule>              # required, single rule
envelope: {<field>: <FieldRule>, ...}   # required
common:   {<field>: <FieldRule>, ...}   # required
event_types:                            # required
  <SourceEventType>: {<field>: <FieldRule>, ...}
```

`FieldRule` (`extra="forbid"`, so an unknown key is a validation error) accepts exactly:

| Key | Meaning |
| --- | --- |
| `from` | dotted path, evaluated against `RawEvent.payload` |
| `take` | pluck a field from each element of a list |
| `as` | `boolean` (presence check) or `timestamp` (ISO → epoch millis) |
| `default` | fallback literal, composes on top of `from`/`take`/`as` |
| `expression` | raw JMESPath escape hatch; mutually exclusive with `from` |

**What the Domain-Neutral Envelope requires** - this is the constraint that matters most for this
idea. `NormalizedEvent` declares `entity_id: str`, `entity_name: str`, `event_time: int` and
`partition_key: str` as **non-optional**, and `EventNormalizer.normalize` reads all four out of the
evaluated contract by direct subscript, not `.get()`
([flink/normalization/domain/normalizer.py](flink/normalization/domain/normalizer.py)):

```python
partition_key=evaluated["partition_key"],
entity_id=str(evaluated["entity_id"]),
entity_name=evaluated["entity_name"],
event_time=evaluated["event_time"],
```

**A contract that omits any of these four raises `KeyError`.** Everything else the contract
evaluates lands as an opaque extra (`_EXTRACTED_FIELDS` filter + `extra="allow"`).

`event_types` is keyed by `RawEvent.source_event_type`. An undeclared type degrades to
envelope + common with an empty per-type block rather than raising
([domain/evaluator.py](flink/normalization/domain/evaluator.py), `_collect_field_rules`'s
`except KeyError`) - per P1 AC5.

**Output**: `NormalizedEvent` → `events-normalized`, Kafka record keyed by `partition_key`
(`FlinkTransformerAdapter.flat_map` yields `Row(key, value)`, and `KafkaSinkAdapter` wires
`RowFieldExtractorSchema(0)`/`(1)` as key/value serializers).

**One job, one topic pair, hardcoded.** `flink/normalization/app.py` names `events-raw` and
`events-normalized` inline and reads contracts from `interface/sources/`. Every source's normalized
output shares `events-normalized`.

### 2.3 Aggregation

Per `AD-009` and `flink-aggregation/spec.md`: the `.sql` file **is** the contract.

Confirmed in code: `flink/analytics/app.py` is a **generic SQL statement runner** with no domain
logic whatsoever - it reads the file named by `ANALYTICS_QUERY_FILE` from `interface/analytics/`,
splits on `;`, and executes each statement in order through `StreamTableEnvironment.execute_sql()`,
with `env.enable_checkpointing(60000)`.

What it expects as input is therefore **whatever the SQL file declares**. Today
`interface/analytics/repo_counts_5m.sql` declares a source table over `events-normalized` with only
`partition_key` and `event_time`, a `TO_TIMESTAMP_LTZ` computed column, a 30s bounded-out-of-orderness
watermark, `'json.ignore-parse-errors' = 'true'`, and one `TUMBLE`-windowed `COUNT(*)` into
`events-analytics` with an exactly-once transactional sink.

Nothing in `app.py` constrains the file to one source table, one statement, or one aggregation.

### 2.4 Topics and infrastructure

`infra/docker/scripts/create-topics.sh` provisions a **hardcoded array** of exactly three topics -
`events-raw`, `events-normalized`, `events-analytics` - each 3 partitions, RF 3, 7-day retention.

Compose services: `kafka-ui`, `topic-init`, `ingestion`, `normalization`,
`taskmanager-normalization`, `analytics`, `taskmanager-analytics`. **No OpenSearch, no Grafana** -
confirmed by grep across `infra/`, `Makefile`, and `requirements/`.

---

## 3. What each external API actually provides

Researched against official documentation; the three REST endpoints were additionally **called live**
to confirm the response shape rather than trusting the docs.

### 3.1 Binance Spot - trade stream (WebSocket)

- **Endpoint**: `wss://stream.binance.com:9443/ws/<symbol>@trade` (raw), or
  `wss://stream.binance.com:9443/stream?streams=...` (combined), or connect and send a `SUBSCRIBE`
  message.
- **Auth**: none for public market streams.
- **Transport**: push. Documented as "Real-time" - one message per trade executed.
- **Payload**: `e` (event type), `E` (event time, ms), `s` (symbol), `t` (trade id), `p` (price,
  string), `q` (quantity, string), `T` (trade time, ms), `m` (buyer is market maker), `M` (ignore).
- **Connection rules**: a connection is valid for **24 hours** and is disconnected at that mark; the
  server sends a ping every 20s and disconnects if no pong within a minute; **max 1024 streams per
  connection**; **5 incoming messages/second** limit (10 for futures/options), with repeated
  violations risking an IP ban.
- **Volume**: BTCUSDT trades on Binance are the highest-frequency of the three - easily tens to
  hundreds of messages per second in active markets.

### 3.2 Coinbase Exchange - ticker / matches (WebSocket)

- **Endpoint**: `wss://ws-feed.exchange.coinbase.com` (public, unauthenticated). A separate
  `wss://ws-direct.exchange.coinbase.com` exists but requires auth.
- **Auth**: none for the public feed.
- **Transport**: push. **A subscribe message must be sent within 5 seconds of connecting or the
  server disconnects.**
- **Subscribe**: `{"type":"subscribe","product_ids":["BTC-USD"],"channels":[{"name":"matches","product_ids":["BTC-USD"]}]}`
- **`match` payload**: `type`, `trade_id`, `sequence`, `maker_order_id`, `taker_order_id`, `time`
  (RFC3339), `product_id`, `size`, `price`, `side`.
- **`ticker` payload**: `type`, `sequence`, `product_id`, `price`, `open_24h`, `volume_24h`,
  `low_24h`, `high_24h`, `volume_30d`, `best_bid`, `best_bid_size`, `best_ask`, `best_ask_size`,
  `side`, `time`, `trade_id`, `last_size`.
- **Note**: `matches` is per-trade (comparable to Binance `@trade`); `ticker` is a per-trade snapshot
  that also carries book/24h context. **These are two different shapes** - which one to use is a real
  choice, not a detail.

### 3.3 Kraken - trade channel (WebSocket v2)

- **Endpoint**: `wss://ws.kraken.com/v2`
- **Auth**: none for the public trade channel.
- **Transport**: push, real-time.
- **Subscribe**: `{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"],"snapshot":false}}`
- **Payload fields**: `symbol`, `side`, `qty` (float), `price` (float), `ord_type`
  (`limit`/`market`), `trade_id` (int), `timestamp` (RFC3339).
- **Notable**: **multiple trades may be batched into a single message**, and an initial snapshot
  (most recent 50 trades) is sent unless `snapshot: false`. So one WebSocket message ≠ one event -
  unlike Binance.

### 3.4 mempool.space (REST, polling)

**Live-verified responses**, not from docs:

```
GET https://mempool.space/api/v1/fees/recommended
{"fastestFee":3,"halfHourFee":1,"hourFee":1,"economyFee":1,"minimumFee":1}

GET https://mempool.space/api/mempool
{"count":84888,"vsize":42146199,"total_fee":9420806,"fee_histogram":[[6.018,50298],[4.66,50259],...]}
```

- **Auth**: none.
- **Transport**: polling only - no push variant for this data.
- **Fields**: fees in sat/vB by priority tier; `count` (tx in mempool), `vsize` (total virtual size),
  `total_fee`, `fee_histogram` (array of `[feerate, vsize]` pairs).
- **Rate limits**: documented as enforced with HTTP 429 on breach, but **no specific numeric limit is
  published**, and the 429 response is not documented as carrying GitHub-style
  remaining/reset headers.
- **Natural cadence**: Bitcoin blocks average ~10 minutes; mempool state is meaningful at ~30s
  granularity. Polling faster is waste.
- **Critical shape note**: both responses are a **single JSON object**, with **no id field and no
  type field**.

### 3.5 Binance Futures (REST, polling)

**Live-verified responses**:

```
GET https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT
{"symbol":"BTCUSDT","markPrice":"80065.24544928","indexPrice":"80098.66195652",
 "estimatedSettlePrice":"80078.15296679","lastFundingRate":"0.00008390",
 "interestRate":"0.00010000","nextFundingTime":1787875200000,"time":1787866534004}

GET https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT
{"symbol":"BTCUSDT","openInterest":"109039.496","time":1787866530443}
```

- **Auth**: none for either endpoint.
- **Weight**: `premiumIndex` = 1 with `symbol`, 10 without; `openInterest` = 1 (`symbol` required).
- **Transport**: polling. Binance does offer a futures WebSocket mark-price stream, but **open
  interest has no push variant** - so this leg is polling regardless.
- **Natural cadence**: funding settles every 8 hours (`nextFundingTime`), but `lastFundingRate` and
  `markPrice` move continuously; open interest is meaningful at ~1 minute granularity.
- **Shape note**: single JSON objects, with a `symbol` but **no id and no type field**. Two separate
  endpoints must be combined to get one "leverage observation".

### 3.6 Summary of transport and cadence spread

| Source | Transport | Natural rate | Has id? | Has type? | Response shape |
| --- | --- | --- | --- | --- | --- |
| Binance spot trade | push (WS) | 10-100s/sec | `t` | `e` | object per message |
| Coinbase matches | push (WS) | 1-20/sec | `trade_id` | `type` | object per message |
| Kraken trade | push (WS) | 1-10/sec | `trade_id` | — (channel-level) | **batched array** |
| mempool.space | poll (REST) | ~30s | **no** | **no** | single object |
| Binance Futures | poll (REST) | ~60s | **no** | **no** | single object (×2 endpoints) |

The cadence spread across sources is roughly **four orders of magnitude**. That is the central
technical difficulty of this idea, and it is a watermark problem, not an ingestion one.

---

## 4. Gap analysis by stage

### 4.1 Ingestion

**Confirmable from the docs/specs alone**: `streaming-ingestion/spec.md` describes a poll loop with
a rate-limit backoff and per-run dedup. Nothing in it mentions push transports.

**Verified in code** (files opened: [ingestion/ports.py](ingestion/ports.py),
[ingestion/adapters/client.py](ingestion/adapters/client.py),
[ingestion/domain/formatter.py](ingestion/domain/formatter.py),
[ingestion/models.py](ingestion/models.py), [ingestion/use_case.py](ingestion/use_case.py),
[ingestion/app.py](ingestion/app.py)):

| Gap | Affects | Evidence |
| --- | --- | --- |
| **No push transport exists.** `EventClientPort.get_events()` is a bounded call returning a list; `IngestionPipeline.execute()` runs once per call; `app.py` is a sleep loop. | 3 WS sources | `ports.py`, `use_case.py`, `app.py` |
| **Response must be a JSON array.** `_parse_response` returns `response.json()` and the formatter iterates it. | mempool, Binance Futures | `client.py:47`, `formatter.py:21` |
| **`id_field`/`type_field` are mandatory and must resolve per event.** A snapshot has neither. | mempool, Binance Futures | `models.py` `get_event_id`/`get_event_type`, `formatter.py` `EventModel` |
| **Rate-limit model is GitHub-shaped.** `_is_rate_limited` requires 403/429 **plus** a remaining-count header `== "0"`; `SourceConfig.rate_limit_remaining` indexes `headers` directly → `KeyError` if the YAML omits it. mempool returns 429 with no such headers. | mempool, Binance Futures | `client.py:26`, `models.py` properties |
| **Dedup by `source_event_id` is wrong for snapshots.** Two identical consecutive mempool readings are two valid observations, not a duplicate. | mempool, Binance Futures | `use_case.py`, `InMemoryDuplicateTracker` |
| **One endpoint per process.** A "leverage observation" needs `premiumIndex` **and** `openInterest` combined. `SourceConfig.url` is one resolved URL. | Binance Futures | `models.py`, `source_config_repository.py` |
| **No WS reconnect/backpressure story.** Binance drops the connection at 24h by contract; Coinbase drops if no subscribe within 5s. Nothing in the codebase reconnects anything. | 3 WS sources | absent from `adapters/` |

**Honest framing**: the polling sources were the "easy" ones in the earlier sketch of this idea.
They are not. `EventClientPort` fits them *as an interface*, but three of its concrete assumptions
(array response, mandatory id/type, GitHub rate-limit headers) do not hold for either.

### 4.2 Normalization

**Confirmable from the specs**: `flink-normalization/spec.md` P1 AC2 explicitly says
`entity_id`/`entity_name` were named generically "so the envelope stays meaningful for a future
non-actor source (e.g. a financial feed keyed by an account, not a person/bot)". This idea is exactly
that case arriving.

**Verified in code** ([flink/normalization/models.py](flink/normalization/models.py),
[domain/normalizer.py](flink/normalization/domain/normalizer.py),
[domain/evaluator.py](flink/normalization/domain/evaluator.py),
[adapters/transformer.py](flink/normalization/adapters/transformer.py),
[app.py](flink/normalization/app.py)):

| Gap | Severity | Evidence |
| --- | --- | --- |
| **`entity_id`/`entity_name` are required, read by direct subscript.** A price tick has no actor. A mempool snapshot has no entity at all. | Design decision needed | `normalizer.py`, `models.py` |
| **A literal-valued field IS expressible** - `expression: "'BTC'"` (JMESPath raw string literal), or `from:` a nonexistent path with a `default:`. So a synthetic `entity_name` is mechanically unblocked. | Not a blocker | `evaluator.py` `_compile_rule` |
| **`event_types` block is required even when a source has one shape.** mempool has no type concept; the contract would need a single synthetic key matching whatever `type_field` the ingestion side invented. | Config awkwardness, not a blocker | `models.py` `NormalizationContract` |
| **`as: timestamp` handles ISO 8601 only** (`datetime.fromisoformat`). Binance gives epoch millis (`T`), Kraken/Coinbase give RFC3339. Epoch-millis input has no transform - it would need `expression:` or a new `as:` value. | Small platform gap | `evaluator.py` `NormalizationFunctions._func_iso_to_millis` |
| **All sources share `events-normalized`.** Five contracts → five different extra-field shapes in one topic. | Consequence for §4.3, not itself a bug | `app.py` |
| **`schema_version != 1` is still not rejected** - a known open item (`FLK-11`), not introduced by this idea. | Pre-existing | `transformer.py`, `shared/models.py` |

**The real question is not mechanical.** A synthetic `entity_id: "system"` would work and would be
dishonest - the field would carry no meaning for two-fifths of the platform's sources. The
alternative is loosening the envelope (`Optional`) for a class of *observation* events that have no
actor. That is an `AD-004`-level decision affecting every future non-VCS source, not a per-contract
workaround.

### 4.3 Aggregation

**This is where the platform is in the best shape, and it should be said plainly.**

Confirmed in code ([flink/analytics/app.py](flink/analytics/app.py)): the job is a generic SQL
runner. Multiple `CREATE TABLE` statements, a join, derived columns, and filters are all just more
statements in the same file. **No new Python is required for the aggregation stage.**

The join is expressible without new topics: define three source tables over the same
`events-normalized` topic with different `properties.group.id` and different column sets, each
filtered by `WHERE source = '<name>'`. `'json.ignore-parse-errors' = 'true'` is already set, and
JSON-format columns absent from a given row arrive as `NULL` - so heterogeneous rows in one topic
are tolerable by construction.

What a Conviction Index query would exercise that `repo_counts_5m.sql` does not:

- multiple source tables from one topic, discriminated by `source`
- a **windowed join** across three streams
- **derived fields** (`CASE WHEN ... THEN 'artificial' ...`, a numeric score)
- **filters** (`WHERE` - e.g. discard windows with too few ticks to classify)
- aggregations beyond `COUNT(*)` (`AVG(funding_rate)`, `MAX(fee)`, `STDDEV` across exchanges)

**The genuine open risk is watermarks, not SQL.** Flink advances a watermark from the slowest
partition. With price ticks at ~100/sec and funding rate at ~1/min sharing one topic and one
watermark strategy, the fast stream's windows are gated by the slow stream's progress. Whether this
works with a single tumbling window, needs interval joins, needs per-source idleness timeouts
(`table.exec.source.idle-timeout`), or needs the slow sources on their own topic with their own
watermark - **this cannot be answered from the docs or this repo. It needs a spike.**

**Join key**: price and leverage both key naturally on the asset (`BTCUSDT` / `BTC-USD` / `BTC/USD`
- note the three exchanges use three different symbol formats, which normalization would have to
reconcile). **Mempool state has no asset at all** - it is global Bitcoin network state. Its join key
must be a synthetic constant (`BTC`), which is expressible but is a modelling statement worth making
consciously.

### 4.4 Storage

**Nothing exists.** Confirmed by grep across `infra/`, `Makefile`, `requirements/`: no OpenSearch,
no Elasticsearch, no service, no dependency. `.specs/RFC.md` lists OpenSearch as a **required**
learning goal; `README.md` and `CLAUDE.md` both mark it "planned, not yet built".

This is not a gap this idea creates - it is the project's largest unbuilt stage, and the Conviction
Index would be its first real consumer. The only idea-specific note: the output shape here is a
classification plus supporting scores per asset per window, not a flat count - relevant to index
mapping design whenever that feature is specified.

### 4.5 Visualization

**Nothing exists.** Same evidence. Grafana is a **required** RFC learning goal, entirely unbuilt.

Idea-specific note: a conviction classification is more naturally a state-over-time panel (with the
three input signals as context) than a leaderboard - a different dashboard class than
`repo_counts_5m` would produce. Not a blocker, an input to that feature's design.

---

## 5. What to do with this

### 5.1 Needs new Python (platform capability)

These are platform capabilities, not source configuration. Per `AD-006`'s division, these are
legitimately developer work.

1. **A push/streaming source port.** A second port alongside `EventClientPort` - something like
   `EventStreamPort` with a start/stop lifecycle and a callback or generator, rather than a bounded
   `get_events()`. `AD-004`'s "port per behavior" principle argues for a new port over stretching
   the existing one.
2. **A WebSocket client adapter** implementing it: connect, send a subscribe frame, yield messages,
   handle ping/pong, reconnect on drop (mandatory - Binance disconnects at 24h **by contract**), and
   apply backpressure when Kafka production falls behind.
3. **A long-lived process entrypoint.** `app.py`'s `while True: execute(); sleep()` shape does not
   fit "hold a connection and receive". Either a second entrypoint or a generalized one.
4. **Support for a non-array response body.** A declarative way to say "this response is one event",
   or "the events are at `<path>` in the response" - a single config key would cover both this and
   any future API that wraps its list in an envelope.
5. **Synthetic id/type for sources that have neither.** Options: generate from the observation
   timestamp, make `id_field`/`type_field` optional with a declared constant, or add a
   `synthesize:` block. This is a contract-format decision as much as a code one.
6. **A pluggable rate-limit strategy.** The GitHub header protocol is currently hardcoded into
   `_is_rate_limited`. mempool.space needs plain-429-with-backoff; Binance needs weight-based
   accounting. This is the third distinct protocol, which is usually the point where it earns an
   abstraction.
7. **A dedup policy that isn't "same id = duplicate".** Snapshots need either no dedup or
   content-based dedup ("unchanged since last poll").
8. **Multi-endpoint composition** (optional, could be avoided). Combining `premiumIndex` +
   `openInterest` into one observation - or accept them as two separate sources and join them in
   SQL, which needs no new code at all. **The SQL route is probably right** and worth stating as the
   default.
9. **Epoch-millis timestamp support in the normalization DSL** - a new `as:` value, or accept that
   `expression:` covers it.
10. **The entire OpenSearch and Grafana stages** - pre-existing, not created by this idea.

### 5.2 Configuration only (no Python deploy, per `AD-006`)

Assuming §5.1's capabilities exist:

- `interface/sources/<name>/ingestion.yml` × 5 (or 6, if the two futures endpoints stay separate) -
  endpoints, no auth for any of them, id/type declaration, rate-limit strategy selection.
- `interface/sources/<name>/normalization.yml` × 5 - envelope mapping, per-source fields, symbol
  normalization to a common asset key.
- `interface/analytics/conviction_index_5m.sql` × 1 - **the entire aggregation stage**, including
  the three source tables, the join, the derived classification, and the filters. No Python.
- A new topic in `create-topics.sh` if the analytics output gets its own (the script's topic list is
  hardcoded; adding one is a one-line edit, though "editing a shell script" is arguably not
  "configuration" in `AD-006`'s sense - worth noticing).

**This split is itself the demonstration.** Roughly: one new transport capability plus some contract
flexibility in Python, and then five sources and a non-trivial multi-source analytic land as data.
That is the platform claim, made concrete.

### 5.3 Open questions blocking design

Ordered by how much they'd change the design if answered differently.

1. **Does the Domain-Neutral Envelope loosen, or do observation sources synthesize an entity?**
   `entity_id`/`entity_name` are required today (verified). Making them `Optional` is an
   `AD-004`-level amendment affecting every future source. Synthesizing is cheap and dishonest.
   Needs a decision, not a workaround.
2. **Can three streams with a ~10,000× cadence spread share one topic and one watermark strategy?**
   Not answerable from this repo or the docs. Needs a Flink spike: single tumbling window vs.
   interval join vs. per-source idleness timeout vs. separate topics.
3. **Should slow snapshot sources share `events-normalized` at all?** The one-topic convention was
   set when every source was a GitHub-shaped event stream. Three orders of magnitude of rate
   difference in one topic is a new situation. Changing it means changing
   `flink/normalization/app.py`'s hardcoded sink - a real architectural decision, not config.
4. **What is the classification's actual logic?** "Congested" and "high leverage" need thresholds.
   Absolute (fee > 20 sat/vB)? Relative to a trailing window (needs state Flink SQL may or may not
   express cleanly)? This is the analytic core and is entirely undefined - and it determines whether
   the SQL stays a single query.
5. **`ticker` or `matches` for Coinbase?** Different shapes and different semantics. Similarly:
   Kraken batches multiple trades per message - does one WS message become one `RawEvent` or many?
   This affects the push port's contract (`yield` one or many per message).
6. **Do trade ticks even belong in Kafka one-per-message?** At Binance BTCUSDT volume this is a
   meaningful throughput question for a local single-broker dev stack. Pre-aggregating at ingestion
   would contradict "raw" - but so does dropping messages.
7. **How do three symbol formats (`BTCUSDT`, `BTC-USD`, `BTC/USD`) become one join key?** The
   normalization DSL has no string-manipulation transform beyond `expression:`'s raw JMESPath. Is
   `expression:` enough, or does this justify a `map:`/lookup primitive?
8. **Does dedup apply to trade ticks at all?** Exchange trade ids are unique per symbol per venue,
   so `InMemoryDuplicateTracker(120)` would work but is pointless overhead on a push stream that
   doesn't replay.
9. **Is `headers:` renamed?** It means rate-limit header names, not request headers (verified). Four
   new sources would each carry a confusing block. Renaming is a breaking change to an existing
   contract format - small, but it is the kind of thing that gets harder with every source added.

---

## 6. Where this leaves the project

If this becomes the target, the remaining project work has a shape:

| Stage | Work |
| --- | --- |
| Ingestion | New push port + WS adapter + long-lived entrypoint; contract-format flexibility for non-array/id-less/differently-rate-limited sources |
| Normalization | One envelope decision (`entity_id`/`entity_name`); small DSL additions; possibly a topic-routing decision |
| Aggregation | **No new Python.** One ambitious `.sql` file, gated on the watermark spike |
| Storage | OpenSearch, entirely unbuilt (RFC-required) |
| Visualization | Grafana, entirely unbuilt (RFC-required) |

It also touches the RFC's opportunistic goals honestly: a stack with 5 ingestion processes, 2 Flink
clusters, Kafka, OpenSearch and Grafana is plausibly the point `AD-005` identified as the Kubernetes
trigger ("the local stack grows large/interdependent enough that Compose's orchestration starts to
strain") - without having to invent an excuse for it.

## 7. Related decisions

- **`AD-004`** / [.claude/rules/architecture.md](.claude/rules/architecture.md) - domain-neutral
  envelope and port-per-behavior. Both are tested by this idea; the envelope one may need amending.
- **`AD-006`** / [.specs/PLATFORM.md](.specs/PLATFORM.md) - contracts as data. §5.1/§5.2 above are
  exactly this decision's dividing line, applied.
- **`AD-009`** - SQL-as-aggregation-contract. Already sufficient for the join; §4.3 is the evidence.
- **`AD-005`** - Kubernetes/Terraform trigger conditions. §6 notes this idea plausibly trips the K8s
  one naturally.

## Sources

- [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [binance-spot-api-docs/web-socket-streams.md](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
- [Binance USDⓈ-M Futures - Mark Price](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price)
- [Binance USDⓈ-M Futures - Open Interest](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest)
- [Coinbase Exchange WebSocket Feed Overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview)
- [Coinbase Exchange WebSocket Channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)
- [Kraken WebSocket v2 - Trade channel](https://docs.kraken.com/api/docs/websocket-v2/trade)
- [mempool.space REST API](https://mempool.space/docs/api/rest)
- [mempool.space fee estimation](https://www.mintlify.com/mempool/mempool/features/fee-estimation)
