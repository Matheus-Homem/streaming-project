# Flink Normalization - Concepts Guide

**Purpose**: the background you need *before* each phase of `tasks.md`, not a replacement for reading the official docs. Each section leads with an analogy, gives the mental model, flags common misconceptions, then points at exactly which tasks it unlocks. No ready-made code here - that's for you to write and me to review, per mentor mode.

Read a section right before starting the phase it maps to - don't front-load all of it in one sitting.

---

## 1. Kafka topic administration (unlocks: T1, T2)

**Analogy**: today, `events-raw` exists the way a shared Google Doc exists the moment someone types in a URL that didn't exist before - auto-created, default settings, nobody decided anything on purpose. What you're about to do is more like provisioning a filing cabinet: you decide up front how many drawers it has, how many backup copies exist, and how long documents sit in it before shredding.

**Mental model**: a Kafka topic is a *log*, not a table. Three knobs matter here:

- **Partitions** - the log is actually split into N independent, ordered sub-logs. Order is only guaranteed *within* a partition, never across partitions of the same topic. This is why the partition-key decision (`repo_name`) matters: it's the only lever you have over which sub-log an event lands in, and therefore what ordering guarantee you get "for free."
- **Replication factor** - how many brokers hold a copy of each partition. RF3 on a 3-broker cluster means every partition is fully mirrored - you can lose 2 brokers and still not lose data (though you'd lose availability with only 1 left, depending on `min.insync.replicas`, which you're not touching here).
- **Retention** - how long a message stays in the log before Kafka is free to delete it, regardless of whether anyone consumed it. This is time-based here (`retention.ms`), not size-based. Kafka doesn't "remove after read" like a queue - a topic is more like a replayable tape than a mailbox.

**Common misconception**: people assume more partitions = more parallelism = always better. More partitions also means more open file handles per broker, more replication traffic, and - critically for you - a partition count you basically can't shrink later without recreating the topic. 3 partitions on a 3-broker cluster is a deliberately conservative starting point.

**The tool**: `kafka-topics.sh --bootstrap-server <host:port> --create --topic <name> --partitions 3 --replication-factor 3 --config retention.ms=604800000`. The `apache/kafka:latest` image you already run ships this script; it lives under `/opt/kafka/bin/`. Re-running `--create` on an existing topic fails unless you add `--if-not-exists` - worth knowing before you write a retry loop that might otherwise crash on its second attempt.

**Docs**: [Kafka topic operations](https://kafka.apache.org/documentation/#basic_ops_add_topic)

---

## 2. Docker Compose orchestration patterns (unlocks: T1, T2, T3, T4, T20, T21)

**Analogy**: `depends_on` alone is like telling a chef "start cooking the sauce after the chef prepping vegetables walks into the kitchen" - not "after they finish chopping." Compose's plain `depends_on` waits for a container to *start*, not to be *ready*. For long-running services (your brokers) that's fine for now (they're already handled), but for something that must *finish successfully first* - like your topic-creation script - you need a different shape.

**Mental model - the one-shot init container**: a service whose whole job is to run once, do something idempotent, and exit 0. Nothing "depends_on completion" by default in plain Compose without `condition: service_completed_successfully` (a newer Compose feature) - the more portable, battle-tested pattern is: the one-shot script itself retries against the thing it needs (brokers being reachable) until it succeeds, and *other* services that need topics to exist just start a little later or handle "topic doesn't exist yet" gracefully. Read `infra/docker/docker-compose.yml`'s existing `depends_on: [controller-1, controller-2, controller-3]` on the brokers for the pattern already in this repo - you're extending the same idea one layer up.

**Mental model - custom images vs official images**: the official `flink` image is a *base* - it has the JVM and Flink's Java runtime, but not Python, not PyFlink, not your job code, not the Kafka connector JAR. A `Dockerfile` that starts `FROM flink:2.3.0-...` and adds those things isn't "configuring" the image, it's *building a new one that layers your requirements on top*. Same idea for `ingestion/Dockerfile`, just simpler (a plain Python base image + `pip install -r requirements.txt`).

**Common misconception**: assuming `docker compose up` rebuilds images automatically when you change a Dockerfile. It doesn't by default - you need `docker compose up --build` (or `docker compose build` first) whenever a Dockerfile or its build context changes.

**Docs**: [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/), [Compose Dockerfile reference](https://docs.docker.com/reference/dockerfile/)

---

## 3. Ports & Adapters, one more time (unlocks: T5)

You already built this shape once in `ingestion/ports.py` - this section is a fast refresher, not a re-teach, plus the one new wrinkle.

**The refresher**: an ABC (`NormalizationEngineBase`) describes *what* a normalizer does (`normalize(event) -> dict`), never *how*. Nothing in the Flink job is allowed to know GitHub-specific details - it only ever talks to the ABC's contract.

> **Revised 2026-08-19 for `AD-006`.** This section was written before normalization became contract-driven, and described a `GitHubNormalizer` class selected from a `NORMALIZER_REGISTRY` dict. Neither exists. The "how" is now a YAML contract (`config/sources/github.yml`) interpreted by one source-agnostic `NormalizationEngine`, which delegates value extraction to a second port, `NormalizationEvaluatorBase`. The pedagogical point below is unchanged - only the thing being dispatched changed, from a class to a contract.

**The new wrinkle - why this matters more here than it did in ingestion**: in `ingestion/app.py`, you pick the concrete engine by `--source` at the command line, once, at startup. Here, a *single running job* processes events from potentially multiple sources mixed together on the same topic (today only GitHub publishes, but the topic and the ABC don't know that) - the dispatch (`get_contract(event.source)`, `lru_cache`d) happens **per message**, inside the hot path. This is the same pattern, but exercised differently: not "pick one engine and go," but "look up the right adapter, every single time, based on data you just read."

**Common misconception**: thinking an ABC needs to anticipate every future source's shape. It doesn't - it only needs to describe the *contract* (one method, one input type, one output type). the GitHub contract earning its keep by being useful today is what justifies the ABC; a hypothetical weather source doesn't need to exist, or even be designable yet, for the pattern to be worth it (`AD-004`'s whole bet).

---

## 4. PyFlink fundamentals (unlocks: T6-T19)

**Analogy**: a Flink cluster is a factory floor, not a single machine. The **JobManager** is the floor supervisor - it doesn't touch the product, it just decides who does what and tracks progress. **TaskManagers** are the workstations that actually process records. Your job (`app.py`) is the work order: "take material from this conveyor belt (`KafkaSource`), run it through this station (`NormalizationFunction`), put the output on that conveyor belt (`KafkaSink`)."

**Mental model - DataStream API**: you're building a *pipeline definition*, not running a loop. `env.from_source(...).flat_map(NormalizationFunction()).sink_to(...)` doesn't process a single record when you write it - it builds a graph. Nothing runs until `env.execute()` is called (Application Mode calls this for you once the job starts). This is why T19's "Done when" says the wiring code must be importable/testable *without* calling `execute()` - constructing the graph and running it are two separate steps, and only the first one is safely unit-testable outside a real cluster.

**Mental model - why `FlatMapFunction`, not `MapFunction`**: a factory station that's contractually obligated to always output exactly one item, even for scrap material, is a bad station design - it forces you to invent a fake "empty" product just to satisfy the contract. `MapFunction.map(value) -> value'` has exactly that obligation: one input, one output, always. `FlatMapFunction.flat_map(value) -> Iterator[value']` can emit zero, one, or many outputs per input - which is exactly "skip this malformed message" (zero) vs "here's your normalized event" (one). You're not choosing `FlatMapFunction` for flexibility's sake; `MapFunction` is structurally incapable of expressing "drop it."

**Mental model - Source/Sink (`KafkaSource`/`KafkaSink`)**: these are the modern (FLIP-27/FLIP-143) unified connector APIs, replacing an older `FlinkKafkaConsumer`/`FlinkKafkaProducer` you'll see in older tutorials and Stack Overflow answers - if a snippet you find uses those older classes, it's out of date for Flink 2.x. The builders you actually want (`KafkaSource.builder()`, `KafkaSink.builder()`) configure bootstrap servers, topic, group ID, and (for the sink) a `KafkaRecordSerializationSchema` that controls both the message *key* and *value*.

**Common misconception**: assuming a Python `MapFunction`/`FlatMapFunction` subclass runs "in Python" the way a normal script does. It runs inside a Python process that a TaskManager spawns and talks to via Py4J (a Java↔Python bridge) - which is why the connector JAR (Java) and the `apache-flink` pip package (Python) are two *separate* dependencies solving two different halves of the same job (see section 5).

**Docs**: [DataStream API intro](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/execution_mode/), [Kafka connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)

---

## 5. Two dependency worlds: pip package vs JVM connector JAR (unlocks: T18, T20)

**Analogy**: `pip install apache-flink` gets you the *steering wheel and dashboard* - the Python API you write against. It does not get you the *engine* - the actual Kafka connector code, which is Java, and has to be physically present as a `.jar` file on the Flink cluster's classpath (`/opt/flink/lib`). You can have a perfect steering wheel and no engine; the job will fail at submission time with a "class not found" error that has nothing to do with your Python code being wrong.

**Why design.md flags this as a real risk (not just a note)**: this is the single most common first-timer PyFlink failure mode, precisely because Python developers expect `pip install` to be sufficient the way it usually is. It isn't, here.

**What this means for T18 specifically**: importing `pyflink` and subclassing `FlatMapFunction` to unit-test `flat_map()` directly does **not** need the connector JAR or a running cluster - that's pure Python object construction and method calls. You only hit the JAR requirement when the job actually tries to *connect to Kafka* through `KafkaSource`/`KafkaSink`, which happens at `env.execute()` time (T19/T21), not at class-definition/unit-test time. If `pytest` fails on `import pyflink` itself, that's a different problem (package not installed) - don't confuse the two failure modes while debugging.

---

## 6. Flink deployment modes: Application Mode vs Session Mode (unlocks: T21, future improvement)

**Analogy**: Session Mode is a shared print shop - the JobManager/TaskManager cluster stays up, and you can submit different jobs to it over time, the way you'd send different documents to a shop's printer whenever you need something printed. Application Mode is more like a single-purpose vending machine - the JobManager starts up *already loaded* with one specific job, runs it, and if you want a different job, you build a different machine.

**What you're building (confirmed decision)**: Application Mode. The JobManager container's command is `standalone-job -py /opt/flink/usrlib/app.py` - Flink treats "start the cluster" and "run this job" as the same event. This is why T21's Done-When cares about the job showing as running in the Flink Web UI *without a manual submission step* - in Application Mode, there's no separate submission step to forget.

**The deferred alternative, for when you build it later**: Session Mode + a one-shot `job-submitter` service (same shape as your `topic-init` service from T1/T2 - wait for the JobManager's REST API to answer, then run `flink run -py ...`, then exit). Worth knowing *why* someone would choose this over Application Mode: a session cluster can host several unrelated jobs without rebuilding an image per job, which matters once you have more than one Flink job in this project. Recorded in `context.md` as a known next step, not built now.

**Common misconception**: thinking Application Mode is "the simple/toy option" and Session Mode is "the real one." Both are legitimate, documented production deployment modes - the trade-off is reusability-of-cluster vs simplicity-of-single-job-startup, not maturity.

**Docs**: [Flink deployment modes](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/overview/#application-mode)

---

## 7. Kafka message keys and what `partition_key` actually buys you (unlocks: T6, T19)

**Analogy**: an unkeyed Kafka producer is like a deli counter that hands each new customer to whichever server is free - fast, but customer #4 and customer #9 (same person, different visits) could easily be served by different people with no memory of each other. A keyed producer is more like "always send this customer to the server assigned to their last name" - same server, every time, for that customer.

**Mental model**: Kafka hashes the message key to deterministically pick a partition. Every message with `partition_key = "octocat/repo"` lands in the *same* partition, in publish order, forever (as long as partition count doesn't change). That's the entire guarantee - and it's exactly what a future windowed aggregation "per repo" needs: without it, events for the same repo could be scattered across partitions and processed out of order relative to each other.

**Common misconception**: assuming a key gives *global* ordering. It doesn't - it gives ordering *within the partition that key hashes to*, which is shared with every other key that happens to hash to the same partition. Two different repos' events are never ordered relative to *each other*, only each repo's own events are ordered relative to themselves.

---

## Quick reference: which section unlocks which task

| Tasks | Read first |
| --- | --- |
| T1, T2 | §1 (Kafka topic administration), §2 (Compose one-shot pattern) |
| T3, T4 | §2 (Compose custom images) |
| T5 | §3 (Ports & Adapters refresher) |
| T6-T17 | §3, §7 (partition key) |
| T18 | §4 (FlatMapFunction), §5 (pip vs JVM dependency) |
| T19 | §4 (DataStream graph-building), §7 (Kafka key) |
| T20 | §5 (pip vs JVM dependency) |
| T21 | §6 (deployment modes) |
| T22-T29 | No new concepts - applies everything above to real traffic |
