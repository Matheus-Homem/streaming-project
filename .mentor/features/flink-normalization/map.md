# Feature map — flink-normalization

<!-- Written in English. Created by /mentor-map, one per feature.
     Scope of learning for this activity. Not rewritten after close. -->

- **Opened**: 2026-08-16
- **Closed**: open
- **Spec source**: `.specs/features/flink-normalization/{spec,design,tasks,concepts,context}.md`
- **Last review point**: `04a7bc3` (T1-T4 diff reviewed 2026-08-16: `a73e453^..04a7bc3`, covering `docker/docker-compose.yml`, `docker/scripts/create-topics.sh`, `ingestion/Dockerfile`)
- **study_hours_total at open**: 2

## Bucket sort

<!-- Every candidate piece of required knowledge, sorted once at open.
     See references/knowledge-model.md for the test. -->

### 🎯 Decide — becomes objectives, target `decide`

| id | statement |
|---|---|
| K-01 | Given a Kafka topic's traffic/durability/ordering needs, decide partition count, replication factor, and message key together |
| K-02 | Given a multi-source event pipeline, decide envelope vs source-specific field placement |
| K-03 | Given a transform that must sometimes emit zero records, decide flat-map over map |
| K-04 | Given a Flink job's reuse/operational needs, decide Application Mode vs Session Mode |
| K-11 | Given a data model consumed by more than one service in a monorepo, decide shared module vs. cross-package import vs. duplication (emergent, added mid-Execute at T5) |
| K-12 | Given a declarative field-mapping grammar, translate it into an executable query-language expression while keeping raw query syntax confined to a single escape hatch (emergent, added mid-Execute at T9, post-`AD-006`) |

### 📖 Explain — becomes objectives, target `explain`

| id | statement |
|---|---|
| K-05 | Given a Compose service that must fully complete first, decide/explain how to enforce that ordering (capped from `decide` to `explain` — see "Ceiling override" below) |
| K-06 | DataStream pipelines are lazy graphs; nothing runs until `execute()` |
| K-07 | JobManager vs TaskManager roles |
| K-08 | pip package vs JVM connector JAR — two separate dependency worlds |
| K-09 | At-least-once is safe for a stateless, idempotent transform |
| K-10 | Registry-dict dispatch extends ports/adapters into a per-message hot path |

### 📦 Delegate — routed to /mentor-example, no objective created

| item | why it's delegate |
|---|---|
| Exact `kafka-topics.sh` CLI invocation syntax/flags | Lookup-able tool syntax, no trade-off — already implemented in `docker/scripts/create-topics.sh` |
| Per-event-type curated field lists (translating `spec.md`'s Normalization Mapping table rows into `_extract_*` methods) | `spec.md`'s table is already the binding contract; writing 16 event-type extractors is mechanical translation, not a decision — keeping these off the objective list is what keeps K-01..K-10 sharp instead of ballooning past 15 |
| ISO 8601 → epoch-millis conversion mechanics | Standard library lookup, no trade-off today (becomes real once windowed aggregation actually uses watermarks — revisit then) |
| Exact Application Mode CLI entrypoint flags (`standalone-job -py ...`) | Tool syntax lookup, not a decision (the mode *choice* is K-04; the flags aren't) |
| Flink Kafka connector JAR filename/version, exact classpath path | Configuration/lookup, no trade-off |
| Docker base image tag pinning (`flink:2.3.0-scala_2.12-java17`) and Dockerfile layering mechanics | Already decided in `design.md`; mechanical to apply |
| `apache-flink`/connector version pinning in `requirements.txt` | Lookup/config |
| Compose YAML mechanics (volumes, build context/dockerfile fields, port mappings) | Plumbing between services, already implemented for T1-T4 |
| `Makefile` coverage-target updates (`--cov=ingestion --cov=flink`, `neat` paths) | Mechanical config change, no trade-off |

## Task authorship levels

<!-- Retrofitted 2026-08-18, after the mechanism landed in the skill (v2.4).
     Derivation rules live in the skill's references/code-policy.md — not restated here.
     T1-T10 are closed and predate the mechanism; they are deliberately not classified. -->

- **task key used**: explicit id (`tasks.md` uses `T<n>`)

| task | level | objectives | why |
|---|---|---|---|
| T11 (reopened item) | `deliver` | — | The open item is spec conformance: `event_id`/`event_type`/`ingested_at` naming, epoch-millis conversion, `schema_version` as a fixed constant, `source_event_endpoint` leaking. `spec.md`'s envelope table is already the binding answer — transcription, no trade-off. K-02 is *about* envelope-vs-source-specific placement, but that decision was made when the table was written, not here |
| T12 (open items) | `deliver` | — | Remaining work is the missing `test_contracts_github.py`. K-12 is at `decides` = target; writing assertions against an already-correct contract carries no open objective |
| T13 | `deliver` | — | Per-event-type curated field lists — this feature's own 📦 delegate bucket item ("mechanical translation, not a decision") |
| T14 | `deliver` | — | Same as T13, seven more types |
| **T15** | **`paired`** | **K-03** (`unassessed`) | `flat_map` over `map` for a transform that may emit zero records is the decision. The rest — parse, validate, log-and-skip, `json.dumps` — is boilerplate the user would type without thinking |
| **T16** | **`paired`** | **K-04** (`unassessed`) | Application Mode vs. Session Mode is the decision. Source/sink wiring and the env-var plumbing are mechanical, and the exact `standalone-job -py` flags are already 📦 delegate |
| **T17** | **`paired`** | **K-08** (`unassessed`) | The pip-package-vs-connector-JAR split is the concept. JAR filename/version and Dockerfile layering are explicitly 📦 delegate |
| **T18** | **`paired`** | **K-07** (`unassessed`) | JobManager vs. TaskManager as non-interchangeable roles is the concept; K-05 (compose completion-ordering) is already at `decides`, so the `depends_on` half is settled. Compose YAML mechanics are 📦 delegate |
| **T19** | **`own`** | **K-06, K-09** (both `unassessed`) | The one task that is mostly reasoning rather than typing: predicting what the running job should do, then reading reality against it. K-06 (lazy graph, nothing runs until `execute()`) and K-09 (at-least-once is safe for a stateless transform) are only genuinely exercised against live traffic |
| T20 | `deliver` | — | Capturing a real-traffic sample for six event types is data collection |
| T21 | `deliver` | — | Contract entries, same as T13/T14 |
| T22 | `deliver` | — | Contract entries, same as T13/T14 |

**Split across open work: 7 `deliver`, 4 `paired`, 1 `own`.**

T15-T19 carry five of the six `unassessed` `pyflink` objectives (K-03, K-04, K-06, K-07, K-08). K-10 is the sixth and has no task of its own — it is exercised incidentally by T15's dispatch path, so it stays a `/mentor-review` target rather than driving a level.

## Limiting objective

<!-- The transversal concept most of this feature's `decide` objectives depend
     on, if one is visible. Gets deliberate drill in /mentor-eval instead of
     the normal rotation. Leave empty if none is clear — do not force it. -->

- **id**: K-01
- **why**: Ordering-and-locality-in-a-partitioned-log is the concept that quietly determines whether K-01 itself (partition/RF/key choice) and the next feature's windowed aggregation can be reasoned about correctly. It's also the item `streaming-ingestion/spec.md` explicitly deferred ("must be decided before Flink windowing... relies on ordering per key") — this feature is where it gets resolved.

## Carried in

<!-- Objectives arriving already fragile or with an open misconception, and what
     that misconception is. These get priority in the first assessment round. -->

None — first-ever `/mentor-map` run in this repo, so no prior feature history exists to carry fragile/misconception state in from.

## Notes during the feature

<!-- Anything worth remembering that is not an evidence line: a decision the user
     made and deferred understanding, a tool they leaned on heavily, a topic
     they asked to postpone. -->

- **Bootstrap timing**: `.mentor/` did not exist before this run, but Phase 1-2 of `tasks.md`
  (T1-T4: topic-creation script, `topic-init` compose wiring, ingestion `Dockerfile`,
  `ingestion` compose service) were already implemented and committed on this branch
  before mentor tracking started (see `docker/scripts/create-topics.sh`,
  `docker/docker-compose.yml`). No evidence exists for K-01 or K-05 from that work because
  it predates this file — their `declared`/`unassessed` states reflect the triage
  self-report only, not a review of that code. `/mentor-review` on that diff (even
  retroactively) would be a reasonable first assessment action for K-01/K-05.
- **Path drift from `design.md`**: `design.md`/`tasks.md` describe target paths under
  `infra/docker/...` (per `AD-005`), but the code actually implementing T1-T4 lives at
  `docker/...` (the pre-`AD-005` path) — the rename hasn't happened yet. Not a knowledge
  gap, just worth flagging so a future `/mentor-review` isn't confused by the mismatch.
- **Ceiling override**: `docker-compose`'s declared target (`explain`) capped K-05 down
  from the `decide` bucket-sort default. Recorded in `profile.md` Notes too.
- **2026-08-16, `/mentor-review` on T1-T4**: K-05 promoted `declared` → `decides` — two
  clean, unaided, correctly-justified answers (why retry-loop over bare `depends_on`;
  why `ingestion` depends on `topic-init` rather than the brokers directly), both including
  the rejected-alternative reasoning. One open misconception on the *mechanism* of
  `--if-not-exists` (see `knowledge.md`) — not re-tested yet, doesn't block the `decides`
  state since it's a sub-detail, not the core decision K-05 covers. K-01 stayed
  `unassessed`: asked why 3 partitions/RF3/7d retention for this cluster, user answered
  honestly that the values were copied from the spec-driven recommendation with no
  independent model yet. Confirms the triage self-report (`distributed-systems: never`) —
  next step is a dedicated teaching pass on `concepts.md` §1 before continuing to T5+.
- **2026-08-16, teaching pass on K-01**: walked through `concepts.md` §1 live, then produced two
  extra study artifacts at the user's request — `.mentor/classes/flink-normalization/
  kafka-partitioning-replication-retention.{md,mp3}` (mp3 via `edge-tts`, cloud) and
  `.mentor/classes/flink-normalization/kafka-terms-diagram.html` (single SVG diagram
  correlating all 9 terms: kafka, tópico, partição, replication factor, retenção, ordenação,
  paralelismo, broker, controller). Followed by a single-question `/mentor-review` re-test on
  K-01, scoped to the same `04a7bc3` diff: asked why 1 partition would have been wrong for
  this pipeline. Answered correctly, unaided, high confidence, decision-justified — **K-01
  promoted `unassessed` → `decides`**, closing the feature's limiting objective's first
  decides-level evidence. Not `fluent` yet (needs a second, time-separated correct review
  per the 14-day rule).
- **2026-08-16, T5 — emergent objective K-11**: user, starting `NormalizerBase`, noticed
  `flink/` would need to import `RawEvent` from `ingestion/models.py` and asked whether
  that's the right call before writing any code. Discussed three options (direct cross-package
  import — what `design.md` had recorded, move to `shared/models.py`, duplicate in
  `flink/models.py`) and their trade-offs (dependency direction vs. duplication-drift risk vs.
  refactor blast radius on the closed `streaming-ingestion` feature). User chose to move
  `RawEvent` to `shared/models.py` and is implementing it themselves; agent updated
  `design.md`/`tasks.md` references only (no production code touched, per mentor mode).
  Logged as new objective `K-11` (`architecture-patterns`), state `declared` — reasoning
  behind the choice wasn't restated back, so not yet `decides`; revisit at the next
  `/mentor-review` once `shared/models.py` lands.
- **2026-08-17, `shared/models.py` landed, plus more**: `RawEvent` move landed as part of a
  broader `refactor/yaml-driven-source-config` (PR #5) done outside a `/mentor-review` pass —
  it also replaced `ingestion/models.py`'s typed `GitHubEvent`/`SourceType`/`GitHubEventType`/
  `SOURCE_REGISTRY` with a generic YAML-declared `SourceConfig`/`EventModel`. This goes beyond
  the K-11 discussion's scope (which was only about *where* `RawEvent` lives, not about
  removing the typed per-source models `design.md`'s `GitHubNormalizer` was designed against).
  `flink-normalization/spec.md`/`design.md` were left as-is at the user's direction (2026-08-17
  session) — a Design-phase revisit is needed before Execute resumes on T5+, since the
  typed-dispatch assumption (`GitHubEventType` enum, `GitHubEvent.actor.id` attribute access)
  no longer holds. K-11 stays `declared`, not `decides` — the move landed, but not via the
  user restating the reasoning back under review conditions.
- **2026-08-17, `/mentor-eval` round (15 min)**: `study_hours_total` 2 → 6. Due reviews closed
  K-05's `--if-not-exists` misconception (rephrased retest, correct) and promoted `K-11`
  `declared → decides` (shared/models.py landed, user rejected both alternatives with distinct
  reasons). `K-01` got its 2nd clean `decides` evidence via an out-of-project scenario (fraud
  pipeline, `card_id` partitioning) — not yet `fluent`, needs a review ≥14 days after 2026-08-16.
  A new emergent objective `K-12` (`AD-006` contract vocabulary vs. `expression:` escape hatch,
  tied to `functions.py`) went straight `unassessed → decides` on first evidence. `K-03` and `K-06`
  were explicitly deferred by the user (`NormalizationFunction` still in progress; no PyFlink base
  yet) — both routed to a teaching pass instead of being forced.
- **2026-08-17, teaching pass on K-06/K-07/K-08 (PyFlink fundamentals)**: produced
  `.mentor/classes/flink-normalization/pyflink-fundamentals.md` (three sections: lazy `DataStream`
  graph, `JobManager` vs. `TaskManager`, pip package vs. connector JAR — each with an analogy and
  its stated breaking point, plus a project-grounded example), a matching narration
  (`pyflink-fundamentals-narration.txt`) and TTS audio (`pyflink-fundamentals.mp3`, `edge-tts`,
  voice `pt-BR-AntonioNeural`, cloud), and a Claude Artifact
  (`pyflink-fundamentals-diagram.html`, published at
  `https://claude.ai/code/artifact/822126d2-765d-46d3-b612-c2c5b47c2d0f`) with one SVG diagram per
  objective, built on the same design-token system as the `K-01` `kafka-terms-diagram.html`
  artifact at the user's explicit request. No evidence logged yet — a `/mentor-review` re-test on
  K-06/K-07/K-08 is the natural next step once the user has gone through this material.
- **2026-08-18, `/resume` state refresh (not an assessment pass, no bucket/evidence changes)**: found
  Execute had progressed to T11 with a structural split not recorded anywhere - `NormalizerBase`
  shipped as two ABCs (`NormalizationEngineBase`/`NormalizationEvaluatorBase`) instead of one, and
  `ContractNormalizer` split into `NormalizationEngine` (orchestration, `adapters/engine.py`) +
  `NormalizationJmespathEvaluator` (compile/evaluate, `adapters/evaluator.py`, also absorbing
  `iso_to_millis` in place of a standalone `functions.py`). This is exactly the shape `K-11`
  (shared module vs. cross-package import vs. duplication) and `K-12` (contract → query-language
  compilation) are about, but no `/mentor-review` was run against this specific diff, so neither
  objective's state changed here - flagging as the next natural review target once the user is ready,
  since the orchestrator/evaluator split is itself a `decide`-shaped question (single responsibility
  vs. one port) that wasn't visibly discussed under review conditions. Also found and reproduced a
  live bug while re-verifying T11's done-when items: `NormalizationEngine.normalize` raises `KeyError`
  for an undeclared `source_event_type` instead of degrading to an empty per-type block (spec P1 AC5)
  - `.specs/features/flink-normalization/tasks.md`/`STATE.md` updated with the repro. Fixing it is a
  reasonable near-term review opportunity for `K-02` (envelope vs. source-specific field placement)
  or `K-10` (registry-dict-style per-message dispatch, since the fix touches the same
  `contract.event_types[event_type]` lookup K-10 is about).
- **2026-08-18, `github.yml` (T12) review**: `study_hours_total` 6 → 24 (8h on 2026-08-17 + 10h on
  2026-08-18, self-reported). Reviewed the real `common` block: `org_id`/`public` were missing on
  the first pass, and `public` was wrongly declared as `is_public: {from: public, as: boolean}` -
  mentor traced `as: boolean`'s compiled form (`{from} != \`null\``, `adapters/evaluator.py:42-43`)
  and showed it always evaluates `True` for an always-present field, silently discarding the real
  value. User fixed it to plain `from: "public"` and, unprompted, also correctly generalised that
  `from` alone already preserves a value's native JSON/YAML type - no `as:` needed when the source
  is already the target type. Logged as `E-12` (`K-12`, kind `debug`, `hint_rung: 2` since the
  root-cause mechanism was mentor-explained before the user answered - see evidence line for the
  full reasoning). `K-12` stays `decides` (already there via `E-10`); this adds a second,
  independent-context data point but not a fresh promotion. T12's `common` block is now correct
  against `spec.md` (`org_id`, `org_login`, `public`, `repo_id`, `repo_name` all present, org fields
  default to null). T13's four `event_types` entries are still not started - per-type curated field
  lists are the project's own `delegate`-bucket item (see Bucket sort above), so this was handled as
  an informational checklist (missing field names per type, sourced from `spec.md`'s mapping table),
  not a `/mentor-class` or an assessment round.
- **Timestamp convention changed 2026-08-18**: going forward `evidence.jsonl` timestamps use UTC-3
  (`America/Sao_Paulo`) per the user's request, recorded in `profile.md` Notes. `E-01`..`E-11` stay
  in UTC as originally written.
- **2026-08-18, task authorship levels retrofitted** (mechanism shipped in the skill as v2.4): every
  open task classified `own`/`paired`/`deliver` from the current `state` vs `target` in
  `knowledge.md` — see the new section above. No objective state, id, or evidence changed; this is
  purely a levels pass. Motivation: 16-18/08 spent ~18 study hours producing a single evidence line
  (`E-12`) while all six `pyflink` objectives stayed `unassessed`, because hand-authoring the
  contract compiler and its YAML entries cost the hours the open objectives needed. The seven
  `deliver` tasks (T13/T14/T20/T21/T22 plus the two spec-conformance clean-ups) come off the user's
  queue; T15-T19 become the work.
