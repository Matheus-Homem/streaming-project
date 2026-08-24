# Feature: flink-aggregation

<!-- Written by /mentor-map. Rewritten on every remap.
     The Task x Knowledge crossing for this feature, and the verdict for each task.
     See references/task-matrix.md. -->

- Opened: 2026-08-24
- Closed:
- Spec source: `.specs/features/flink-aggregation/spec.md` (task list proposed in conversation during Design/Tasks handshake; `tasks.md` not yet written — pending the design-pairing STOP POINT 2 confirmation, per `.claude/mentor-design-pairing.md`)
- Snapshot: 2026-08-21T09:57:09Z (3 days old, within the 14-day budget - not stale)
- Task key used: explicit id (T1-T6, as proposed in chat; `tasks.md` will carry the same ids when written)

## Tasks

<!-- verdict: own | paired | delegated
     deciding node: the node that produced the verdict, per the aggregation rule
     active nodes: how many survived the `waived` filter, out of how many required
     flags: contested | unverified | class-first | manual-override | empty -->

| task | verdict | deciding node | active nodes | flags |
|---|---|---|---|---|
| T1 - provision `events-aggregated` topic (`create-topics.sh`) | delegated | - | 0/0 | |
| T2 - `repo_counts_5m.sql` (source DDL+watermark, sink DDL+exactly-once, windowed `INSERT...SELECT`) | own | `StreamProcessing.ApacheFlink.EventTime.BoundedOutOfOrdernessWatermarks` | 5/5 | class-first |
| T3 - `app.py` (generic SQL script runner + checkpoint config) | own | `StreamProcessing.ApacheFlink.FaultTolerance.CheckpointingAndStateBackend` | 1/1 | class-first |
| T4 - `Dockerfile` | delegated | - | 0/0 | |
| T5 - `docker-compose.yml` (`aggregation`/`taskmanager-aggregation` services) | delegated | - | 0/0 | |
| T6 - live verification (docker compose up, publish events, inspect `events-aggregated`) | own | `StreamProcessing.ApacheFlink.FaultTolerance.ExactlyOnceSinkViaKafkaTransactions` | 6/6 | class-first |

## Knowledge

<!-- Every node this feature requires, with its resolved triple.
     Each resolution names its origin — a declaration, a date, a derivation.
     A value without its origin is not written. -->

| node | domain (origin) | comprehension (date) | application (source) | tasks |
|---|---|---|---|---|
| `StreamProcessing.ApacheFlink.TableApiSql.DynamicTablesVsDataStream` | developing (default - no declaration in `domain.md`) | unknown (no Gemini Notebook sync since node creation) | theoretical (derived, `nodes.md`) | T2, T6 |
| `StreamProcessing.ApacheFlink.TableApiSql.AppendOnlyVsRetractStreams` | developing (default) | unknown (no sync) | practical (derived, `nodes.md`) | T2, T6 |
| `StreamProcessing.ApacheFlink.EventTime.BoundedOutOfOrdernessWatermarks` | developing (default) | unknown (no sync) | practical (derived, `nodes.md`) | T2, T6 |
| `StreamProcessing.ApacheFlink.Windowing.TumblingWindowViaTvf` | developing (default) | unknown (no sync) | practical (derived, `nodes.md`) | T2, T6 |
| `StreamProcessing.ApacheFlink.FaultTolerance.CheckpointingAndStateBackend` | developing (default) | unknown (no sync) | practical (derived, `nodes.md`) | T3, T6 |
| `StreamProcessing.ApacheFlink.FaultTolerance.ExactlyOnceSinkViaKafkaTransactions` | developing (default) | unknown (no sync) | practical (derived, `nodes.md`) | T2, T6 |

## Gaps

<!-- Required nodes absent from the Gemini Notebook snapshot, or present with
     comprehension `no`. This is the study list — what to take to Gemini Notebook. -->

| node | why it is required | tasks affected |
|---|---|---|
| `StreamProcessing.ApacheFlink.TableApiSql.DynamicTablesVsDataStream` | The paradigm shift behind the whole `.sql` file - a Kafka topic mapped as a queryable "table" | T2, T6 |
| `StreamProcessing.ApacheFlink.TableApiSql.AppendOnlyVsRetractStreams` | Decides why the plain `kafka` sink connector is correct for a *windowed* aggregation | T2, T6 |
| `StreamProcessing.ApacheFlink.EventTime.BoundedOutOfOrdernessWatermarks` | The `WATERMARK FOR ... INTERVAL '30' SECOND` clause and why the bound matters | T2, T6 |
| `StreamProcessing.ApacheFlink.Windowing.TumblingWindowViaTvf` | The `TABLE(TUMBLE(...))` syntax and window boundary semantics | T2, T6 |
| `StreamProcessing.ApacheFlink.FaultTolerance.CheckpointingAndStateBackend` | Checkpoint interval + state backend config in `app.py` | T3, T6 |
| `StreamProcessing.ApacheFlink.FaultTolerance.ExactlyOnceSinkViaKafkaTransactions` | The two-phase-commit sink config and the broker transaction-timeout gotcha | T2, T6 |

All six: conceptual classes already produced this session (`classes/index.md` - `table-api-vs-datastream`, `watermark-e-janela-tumbling`, `checkpoint-e-exactly-once-kafka`), but none have been tested in Gemini Notebook yet - that is the actual remaining gap, not the absence of material.

## Trace

T2  own
    ← `StreamProcessing.ApacheFlink.EventTime.BoundedOutOfOrdernessWatermarks`
        domain        = developing  (default — no declaration on any prefix)
        comprehension = unknown     (snapshot fetched 2026-08-21, before this node existed; class given 2026-08-24, not yet synced)
        application   = practical   (derived: becomes the `WATERMARK FOR col AS col - INTERVAL 'n' SECOND` clause)
    other active nodes: `TableApiSql.AppendOnlyVsRetractStreams` (own), `Windowing.TumblingWindowViaTvf` (own), `FaultTolerance.ExactlyOnceSinkViaKafkaTransactions` (own), `TableApiSql.DynamicTablesVsDataStream` (paired)

T3  own
    ← `StreamProcessing.ApacheFlink.FaultTolerance.CheckpointingAndStateBackend`
        domain        = developing  (default)
        comprehension = unknown     (class given 2026-08-24, not yet synced)
        application   = practical   (derived: becomes checkpoint interval + state backend choice in `app.py`)
    other active nodes: none

T6  own
    ← `StreamProcessing.ApacheFlink.FaultTolerance.ExactlyOnceSinkViaKafkaTransactions`
        domain        = developing  (default)
        comprehension = unknown     (class given 2026-08-24, not yet synced)
        application   = practical   (derived: verifying no duplicate row after a live restart is a direct exercise of this node)
    other active nodes: all 5 remaining nodes are active here too (T6 is the live-verification task, so every node this feature requires gets exercised in it); 4 own, 1 paired (`TableApiSql.DynamicTablesVsDataStream`)

T1, T4, T5  delegated
    ← no active node - each mirrors an existing pattern in this repo (topic provisioning, Flink `Dockerfile`, jobmanager/taskmanager compose pair) with no new taxonomy node

## Notes

- 2026-08-24: first `/mentor-map` for this feature. `.mentor/` was v3-bootstrapped 2026-08-21 with an empty `nodes.md`/`snapshot.json` (no map had run under v3 before this one) - all 6 required nodes were created this session via `/mentor-class`, not pre-existing. `class-first` flags on T2/T3/T6 reflect that Comprehension is still `unknown` for all of them; the material itself (the offer the flag exists to make) has already been delivered - what is actually still open is testing it in Gemini Notebook and running `/mentor-sync`, not producing more classes.
