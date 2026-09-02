# Platform Vision: Contract-Driven Configuration

**Status**: Direction agreed 2026-08-17. Recorded as `AD-006` in `.specs/STATE.md`.
**Scope**: Project-wide. Governs how sources, normalization, transformation, and aggregation are
declared. Not a feature spec - no delivery date, no tasks. Features implement slices of this.

---

## The goal in one sentence

**Developers grow the Python codebase to add platform capabilities; users register a source and
declare how its events are normalized, transformed, and aggregated by filling in a contract that
is not Python.**

Two audiences, two surfaces:

| Audience | Writes | To do what |
| --- | --- | --- |
| Developers | Python | Add platform capabilities: new engines, new transform primitives, new sinks |
| Users (non-technical) | A contract (data, not code) | Register a source/endpoint and declare its normalization, transformation, aggregation |

A new *source* must not require a Python deploy. A new *transform primitive* may.

---

## The reframe: model vs. medium

A configuration model can be fully declarative and still be *expressed* in Python. Many extraction
platforms are built that way - the user assembles typed objects whose attributes describe the
desired output, and the platform interprets those objects. Nothing about that model is imperative;
it just happens to be serialized as a `.py` file, which means only people who can write Python can
author it.

This project deliberately takes the other path: the same declarative spirit, but the contract is
**data** - parsed, validated, and interpreted by the platform - never a Python module the user
edits.

The distinction has a name: an **internal DSL** is hosted inside a general-purpose language
(configuration-as-Python); an **external DSL** is its own format with its own parser and validator
(configuration-as-data). This project commits to an external DSL.

> **Explicitly not planned**: a hierarchy of Python configuration classes for the user to
> instantiate. Being declarative is the goal; a class-per-concept object model is not the way this
> project gets there. Python-side classes exist only as the *interpreter* of the contract, not as
> the thing the user fills in.

---

## Three layers

The contract never exposes the execution engine. This is what "JMESPath, but abstracted" means:

| Layer | Owner | Content |
| --- | --- | --- |
| **Contract** | User | `field: issue_labels`, `from: issue.labels`, `take: name` |
| **Compiler / validator** | Platform (devs) | Parses + validates the contract, resolves it to an execution plan |
| **Engine** | Libraries | JMESPath expression (`issue.labels[].name`), Flink operators, named transform functions |

The engine's syntax is an implementation detail of layer 2. A user who never learns JMESPath must
still be able to author a working contract.

### Tiers of expressiveness

Every declarative platform faces the same pressure: the vocabulary is never quite enough, and the
temptation is to keep bolting on keywords until the format is a bad programming language written in
YAML. The way out is to decide the tiers deliberately, up front:

- **Plain dotted path** - `from: payload.issue.title`. Reads as a field path to anyone, and is valid
  engine syntax as-is, so the common case costs the platform nothing.
- **Friendly vocabulary** - a small, fixed, documented set of intent keywords (`take:`, `as:`) that
  the compiler *translates into* engine expressions. See the finding below: this tier is mostly a
  presentation layer, not an extension of the engine.
- **Raw expression escape hatch** - one documented key accepting a raw engine expression, for cases
  the vocabulary cannot reach.

The escape hatch is the part most platforms get wrong by omission. Without it, users hit a wall and
the platform is blamed. With it *undisciplined* (arbitrary conditionals, loops, variables spread
across the schema), the format rots into an unreadable pseudo-language. Confining the extra power to
a single, clearly-marked key - using a third-party expression syntax rather than an invented one -
keeps both failure modes closed.

**Note (`AD-009`, 2026-08-24)**: this tier discipline assumes an engine the user should not have to
learn directly - true of JMESPath, which is genuinely obscure to a non-technical author. It does not
hold when the engine is already a lingua franca the target user is assumed to know, which is the case
for aggregation's SQL. There, the raw-expression tier *is* the product, not an escape hatch - see
"Aggregation is SQL, not a contract over SQL" below.

### Verified: how much the engine already does (JMESPath 1.1.0, 2026-08-17)

Probed against real captured GitHub payloads before committing to this design (the capture file is a local artifact, untracked; the events are inlined in `tests/fixtures/events.py`). Findings:

| Need | Resolved by | Evidence |
| --- | --- | --- |
| Nested path | native | `actor.id` → `181008794` |
| Pluck attribute from a list of objects | native projection | `payload.issue.labels[].name` → `['area/test', 'sig/scheduling', ...]` |
| Missing path | native, returns null - **never raises** | `payload.naoexiste.nada` → `None` |
| Presence as boolean | native comparison | ``payload.issue.pull_request != `null` `` → `True` |
| Default value | native `\|\|` | `payload.naoexiste \|\| 'FALLBACK'` → `'FALLBACK'` |
| List length | native function | `length(payload.issue.labels)` → `17` |
| ISO 8601 → epoch millis | **custom function** (Python, dev-added) | `iso_to_millis(created_at)` → `1784290892000` |

**Consequence for the design**: only genuine computation (timestamp conversion) needs a Python
function. Everything else the normalization mapping requires is native engine syntax. So the friendly
vocabulary tier exists to *hide syntax the user should not have to learn*, not to *add capability the
engine lacks* - the compiler translates `take: name` into `[].name`, it does not implement plucking.

That the engine returns null instead of raising on a missing path matters independently: a contract
that points at a field some payloads lack degrades to a null field rather than killing the record.

Adding a new custom function remains a **developer** task, which is the correct boundary - it is a
platform capability, not a source registration.

---

## The four stages are not equally hard

A single uniform contract format across all four stages would be a mistake - their expressive needs
differ by an order of magnitude.

| Stage | Nature | Declarative fit | Compiles to |
| --- | --- | --- | --- |
| **Source / endpoint** | URL template + a couple of field locations | Trivial - already solved | `SourceConfig` (`ingestion/config/sources.yml`) |
| **Normalization** | Field mapping, light per-field transforms, nested extraction | Good fit - expression language territory | Flink DataStream (nested JSON flattening is awkward in SQL) |
| **Transformation** | Filters, derived fields, conditionals, enrichment | Medium - the tier discipline above matters most here | TBD |
| **Aggregation** | Group-by, windows, aggregate functions | **This is SQL, and SQL is the contract.** | Flink Table API / SQL |

### Aggregation is SQL, not a contract over SQL

An aggregation requirement - "group by `repo_name`, 5-minute tumbling window, count events" - is a
relational query. Flink ships a Table API and SQL layer built for exactly that. The original framing
here ("compile a YAML contract to Flink SQL") would have meant inventing a vocabulary for windows and
aggregations - reinventing SQL, worse, for an audience that already knows SQL.

**This inverts a decision already recorded in `flink-normalization/design.md`**, whose Tech Decisions
table chose the DataStream API over Table API/SQL on the grounds that per-event-type field extraction
is "arbitrary Python branching logic, not a relational transformation." That rationale was sound when
normalization meant hand-written Python methods per event type. Under a contract-driven design it no
longer holds as a blanket rule.

The resolution is **hybrid, decided per stage**: normalization compiles to DataStream (flattening
deeply nested, per-type-varying JSON is genuinely poor in SQL); aggregation stays on Table API/SQL -
but as `AD-009` (2026-08-24) settles, there is no YAML layer compiling *to* that SQL. The `.sql` query
itself, as a file the platform reads and interprets (never a Python string built at runtime), is the
declarative contract for this stage. It satisfies `AD-006` (data, not code, authored by the user) the
same way a YAML contract would - it just skips the compiler, because the compiler's only job would
have been hiding syntax an assumed-SQL-literate user does not need hidden.

One consequence surfaced while designing `flink-aggregation`, worth recording since it would have
shaped a YAML wrapper badly had one been built: Flink SQL's `CREATE TABLE` needs a **fixed column
list**, but `NormalizedEvent`'s schema is open (`extra="allow"`, fields varying by `event_type`). A
YAML-to-DDL compiler would need a union-of-all-event-types column scheme to stay generic - real
complexity that a query touching only the envelope fields (as most aggregations do) never needs to
pay for. Left as a per-feature decision: hand-write the `CREATE TABLE` for the columns actually
queried, or derive it from the normalization contract, decided when a feature's query shape is known
rather than speculatively.

---

## Where the contract lives

**The contract format is the API. Storage and UI are clients of it.** Fixing the format early and the
storage late is what keeps this cheap.

| Option | Gains | Costs |
| --- | --- | --- |
| **Files in the repo** (chosen now) | Versioning, review, and diffs free via git; zero infra | Requires git access - not yet reachable for a genuinely non-technical user |
| **Database + API + UI** (later) | Real self-service; runtime changes without redeploy | Needs schema, migrations, an admin surface; loses git history unless rebuilt |

**Decision for now**: YAML files in the repo, validated by a Pydantic model.

The bridge between the two: a Pydantic model that validates the contract can also **emit JSON
Schema**, and a future UI can generate its form from that schema. The contract shape is defined once,
by developers, in Python - and the user still never writes Python. Moving from files to a database
later changes *where the bytes live*, not the format, not the validator, and not the interpreter.

Note the honest limit of the current step: YAML-in-git is not yet self-service for a non-technical
user. It is the correct first increment because it fixes the format - the expensive, hard-to-reverse
part - while deferring the UI, which is additive.

---

## Open question: contract changes while a job is running

A Flink job's topology is fixed at submission time. A contract edited at 15:00 is invisible to a job
submitted at 09:00 unless something is built for it. This matters more as storage moves toward a
database, where the whole point is runtime change.

Flink's answer to this is the **Broadcast State** pattern: a control stream carrying configuration
changes, broadcast into the running job alongside the data stream - the standard approach for dynamic
rule engines.

Not solved here, and not required while contracts are files applied at deploy time. Flagged because
it constrains the storage decision: adopting a database for self-service without a reload mechanism
would deliver a self-service surface whose changes do not take effect.

---

## What is deliberately deferred

- A transformation contract - direction still open (`TBD`)
- Database-backed contract storage
- Any UI
- Broadcast State / hot reload
- Non-VCS sources (weather, financial) - the reason this vision exists (`AD-004`), still unbuilt

**No longer deferred, resolved by `AD-009`**: aggregation does not get a YAML contract - it was never
going to need one once the audience was reframed as SQL-literate. The `flink-aggregation` feature is
this stage's first (and only planned) increment: a hand-authored `.sql` query, no compiler.

The project's incremental-MVP approach (`.specs/STATE.md`, note under `AD-006`) is explicit: start
from a basic MVP and add incrementally rather than designing the full target architecture up front.
Designing all four contract stages before building one would violate that. The normalization contract
is the first increment; the rest is shaped by what that one teaches.
