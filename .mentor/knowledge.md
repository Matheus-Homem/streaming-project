# Knowledge registry

<!-- Written in English. One row per learning objective in this project.
     This is the only file that grows for the whole project life.
     Ids are never reused and never renumbered. Rows are never deleted —
     objectives that stop being required get the `archived:` state prefix.
     See references/knowledge-model.md before adding rows.

     Items sorted into the `delegate` bucket at /mentor-map do NOT get a row
     here — they are not learning objectives. See references/worked-examples.md. -->

| id | statement | tags | target | state | evidence | misconception | last_seen | last_seen_hours |
|---|---|---|---|---|---|---|---|---|
| K-01 | Given a Kafka topic's expected traffic, durability need, and downstream ordering need, decide its partition count, replication factor, and message key together — ordering is guaranteed only within a partition, and the key is the only lever over what lands together | kafka, distributed-systems | decide | decides | E-03,E-05,E-09 | | 2026-08-17 | 6 |
| K-02 | Given a multi-source event pipeline, decide which fields belong in a domain-neutral envelope versus a source-specific fields block, so a new source is additive rather than a schema rewrite | architecture-patterns | decide | declared | | | | |
| K-03 | Given a stream transform that must sometimes emit zero output records for one input, decide to use a flat-map-shaped operation instead of a one-in-one-out map | pyflink, distributed-systems | decide | unassessed | | | | |
| K-04 | Given a Flink job's reuse and operational needs, decide between Application Mode (job embedded at cluster startup) and Session Mode (long-lived cluster, jobs submitted separately) | pyflink | decide | unassessed | | | | |
| K-05 | Given a Compose service that must fully complete before a dependent starts, decide how to enforce that when the tool's built-in dependency only waits for container start, not readiness | docker-compose | explain | decides | E-01,E-02,E-04,E-07 | | 2026-08-17 | 6 |
| K-06 | A DataStream pipeline is a lazy graph definition — nothing runs until execute() is called, so wiring code is constructible/testable without a live cluster | pyflink | explain | unassessed | | | | |
| K-07 | In a Flink cluster, the JobManager coordinates and tracks work while TaskManagers execute it — they are not interchangeable roles | pyflink | explain | unassessed | | | | |
| K-08 | A PyFlink pip package provides only the Python API; a connector's actual implementation (e.g. Kafka) is a separate Java JAR that must be present on the cluster's classpath | pyflink | explain | unassessed | | | | |
| K-09 | At-least-once reprocessing of a message is safe for a stateless transform, because re-applying it to the same input always yields the same output | distributed-systems | explain | unassessed | | | | |
| K-10 | A per-message registry-dict dispatch (type → handler) extends the ports-and-adapters pattern into a hot path, resolving the concrete adapter from data read at runtime rather than once at startup | architecture-patterns, pyflink | explain | unassessed | | | | |
| K-11 | Given a data model consumed by more than one deployable service in a monorepo, decide whether it lives in a shared module, is imported directly across the service boundary, or is duplicated per service | architecture-patterns | decide | decides | E-06,E-08 | | 2026-08-17 | 6 |
| K-12 | Given a declarative field-mapping grammar (`from`/`take`/`as`/`default`/`expression`) that must compile to an executable query-language expression, translate each declared field into the correct expression form, keeping the raw query language confined to a single documented escape hatch rather than let it leak into the friendly vocabulary | architecture-patterns | decide | decides | E-10,E-12 | | 2026-08-18 | 24 |

<!-- state: unassessed | declared | fragile | explains | decides | fluent
            prefix with `archived:` when no longer required (archived:explains)

     evidence: comma-separated evidence ids from the feature logs, oldest first.
               Keep them even after the feature is closed — they are the audit
               trail back to report.md.

     misconception: one line, the wrong model currently believed to be open.
                    Cleared only by evidence that contradicts it, not by silence.

     last_seen / last_seen_hours: set from the most recent evidence line's
                    `ts` and `study_hours_total`. There is no stored review
                    date — due-ness is computed fresh every time from these
                    two facts plus the current date and study_hours_total.
                    See references/retention.md. -->

## Limiting objectives by feature

<!-- One line per feature that had a clear limiting objective marked at
     /mentor-map. Optional — not every feature has one. -->

| feature | id | why it's limiting |
|---|---|---|
| flink-normalization | K-01 | Ordering-and-partitioning reasoning underlies both the topic-config decision and `partition_key=repo_name`; it's the deferred item from `streaming-ingestion/spec.md` and the exact concept the next feature (windowed aggregation) needs to already be solid |

## Origins

<!-- Optional index mapping objectives back to where the requirement came from,
     when the table row gets too wide to carry it. -->

| id | origin |
|---|---|
| K-01 | `spec.md` Assumptions (topic config, partition key) + `concepts.md` §1, §7 |
| K-02 | `AD-004` (`STATE.md`) + `design.md` Data Models / Components |
| K-03 | `design.md` Components (`NormalizationFunction`) + `concepts.md` §4 |
| K-04 | `design.md` Tech Decisions (job deployment mode) + `concepts.md` §6 |
| K-05 | `design.md` Docker/compose additions (T1/T2, already implemented) + `concepts.md` §2 |
| K-06 | `concepts.md` §4 (PyFlink fundamentals) |
| K-07 | `concepts.md` §4 (PyFlink fundamentals) |
| K-08 | `concepts.md` §5 (two dependency worlds) |
| K-09 | `spec.md` Assumptions (processing semantics) |
| K-10 | `design.md` Components (`NORMALIZER_REGISTRY`) + `concepts.md` §3 |
| K-11 | emergent — T5 discussion, 2026-08-16 (user noticed `flink/` importing `ingestion/models.py` for `RawEvent` and asked about the trade-off before implementing `NormalizerBase`); see `map.md`'s 2026-08-17 follow-up entry — the move landed as part of a broader refactor that also removed the typed `GitHubEvent`/`SourceType` models, a Design-phase revisit is pending |
| K-12 | emergent — T9, 2026-08-17 (`AD-006`'s tiered contract vocabulary reached its compiler step; `design.md`'s compilation-rule table is the binding spec — this objective didn't exist at the original `/mentor-map`, which predates `AD-006`'s contract-driven redesign) |
