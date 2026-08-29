# streaming-project

A hands-on learning project for building a streaming data pipeline end to end — Kafka, Flink, distributed storage/search (OpenSearch), observability (Grafana), and orchestration (Kubernetes, ArgoCD) — while actually planning the architecture and choosing components, rather than having them handed over working. Terraform (IaC) is an opportunistic addition if a natural chance to use it comes up.

See [`docs/VISION.md`](docs/VISION.md) for what the platform is and why it exists, [`docs/USE-CASES.md`](docs/USE-CASES.md) for the problem it's currently pointed at, and [`.specs/STATE.md`](.specs/STATE.md) for project-wide decisions and the current handoff state.

## Architecture

**Today**: [`ingestion/`](ingestion/) polls a source's public events API (GitHub wired, GitLab config-only), formats payloads into a `RawEvent` envelope ([Pydantic](https://docs.pydantic.dev/), [`shared/models.py`](shared/models.py)), and publishes them to a single Kafka topic (`events-raw`, shared across sources — the `source` field is what tells events apart downstream). [`flink/normalization/`](flink/normalization/) consumes `events-raw`, applies a per-source declarative contract (YAML, no source-specific Python), and publishes `NormalizedEvent`s, keyed by `partition_key`, to `events-normalized`. [`flink/analytics/`](flink/analytics/) runs a Flink SQL job (Table API) over `events-normalized`, windowing counts into `events-analytics`. Local infra (3 controllers + 3 brokers + Kafka UI on `localhost:8080`, Flink job/task managers per stage) runs via [`infra/docker/docker-compose.yml`](infra/docker/docker-compose.yml).

Both ingestion sources and the normalization contract for each source are declared as data, not Python — see [`interface/`](interface/) below and [`.specs/PLATFORM.md`](.specs/PLATFORM.md) for why.

**Planned, not yet built**: OpenSearch as the metrics/search store, Grafana dashboards on top. See [`.specs/features/`](.specs/features/) for each stage's exact scope — `streaming-ingestion` (ingestion), `flink-normalization` (normalization), `flink-aggregation` (analytics), `interface-layout` (the `interface/` contract layout itself).

```
GitHub/GitLab Events API --> ingestion (client -> formatter -> producer) --> Kafka (events-raw)
                                                                                  |
                                                          flink/normalization (contract-driven, per-source YAML)
                                                                                  |
                                                                       Kafka (events-normalized)
                                                                                  |
                                                              flink/analytics (Flink SQL, windowed counts)
                                                                                  |
                                                                       Kafka (events-analytics)
                                                                                  |
                                                                    [planned] OpenSearch --> Grafana
```

## Repository layout

- [`ingestion/`](ingestion/) — the ingestion service: `ports.py` (`EventClientPort`, `EventProducerPort`), `adapters/` (`RequestsClientAdapter`, `KafkaProducerAdapter`), `domain/` (`ValidatingRawEventFormatter`, `YamlSourceConfigRepository`, `InMemoryDuplicateTracker`), `models.py` (`SourceConfig`/`EventModel`), wired by `use_case.py`'s `IngestionPipeline` and run via `app.py`
- [`flink/normalization/`](flink/normalization/) — the normalization job: `ports.py` (`TransformerPort`), `domain/` (`EventNormalizer`, `NormalizationRulesEventEvaluator`, `YamlContractRepository`), `models.py` (`FieldRule`/`NormalizationContract`/`NormalizedEvent`), `adapters/transformer.py` (`FlinkTransformerAdapter`, the PyFlink `FlatMapFunction`), run via `app.py`
- [`flink/analytics/`](flink/analytics/) — the aggregation job: reads a `.sql` file from `interface/analytics/` and runs it as Flink SQL/Table API statements (no Python transform logic — the SQL file is the contract, per `AD-009`)
- [`flink/common/`](flink/common/) — Kafka source/sink building shared by both Flink jobs: `ports.py` (`EventSourcePort`, `EventSinkPort`), `adapters/` (`KafkaSourceAdapter`, `KafkaSinkAdapter`), `factory.py` (`KafkaFactory`)
- [`interface/`](interface/) — declarative contracts, not Python: `sources/<name>/ingestion.yml` (endpoints, auth, id/type fields) and `sources/<name>/normalization.yml` (field-mapping rules per source) for ingestion/normalization, `analytics/*.sql` for the aggregation stage
- [`shared/`](shared/) — cross-cutting utilities: `models.py` (`RawEvent`, the domain-neutral envelope), `logger.py` (per-module logging setup)
- [`tests/`](tests/) — mirrors `ingestion/`/`flink/`/`shared/` 1:1, one test file per source module
- [`infra/docker/`](infra/docker/) — local infra: `docker-compose.yml` (Kafka cluster, Kafka UI, ingestion/normalization/analytics containers, Flink job/task managers), `docker-compose.dev.yml` (lighter single-broker overlay), `scripts/create-topics.sh`
- [`docs/`](docs/) — [`VISION.md`](docs/VISION.md) (what the platform is and why) and [`USE-CASES.md`](docs/USE-CASES.md) (what it's used for) — never references `.specs/` or `.mentor/`
- [`.specs/`](.specs/) — the spec-driven workflow: `STATE.md` (decisions + handoff), `PLATFORM.md` (the contract-driven-authoring decision), `features/[name]/` (spec, design, tasks per feature)

## Requirements

- Python 3.12
- Docker (with Docker Compose) for the local Kafka + Flink stack

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt
```

Dependencies are split by target under [`requirements/`](requirements/): `base.txt` (shared runtime),
`ingestion.txt` and `flink.txt` (per-service runtime, what each Docker image installs), and `dev.txt`
(everything plus the `make test` / `make neat` tooling).

All `make` targets below assume `.venv` is activated — the Makefile calls bare `pytest`/`python`/`autoflake`/`isort`/`black`, which resolve to `.venv/bin` only while it's active.

## Usage

Start the local stack (Kafka + Flink):

```bash
make kafka-up
```

Kafka UI is then available at `localhost:8080`. Pass `MODE=dev` for a lighter single-broker/single-controller footprint (`docker-compose.dev.yml` overlay):

```bash
make kafka-up MODE=dev
```

Run the ingestion service against GitHub:

```bash
make ingestion-default
```

Stop the stack:

```bash
make kafka-down
```

## Development

```bash
make test    # full test suite with coverage
make neat    # format/clean: autoflake + isort + black
make clean   # remove __pycache__ / .pyc files
```

## Conventions

- Abstract base classes + constructor injection for testability (`EventClientPort`/`EventProducerPort` in `ingestion/ports.py`, `TransformerPort` in `flink/normalization/ports.py`, `EventSourcePort`/`EventSinkPort` in `flink/common/ports.py`) — new components follow this shape instead of hard-wiring dependencies.
- Pydantic models for anything crossing a boundary (`RawEvent`, `NormalizedEvent`, `SourceConfig`).
- One logger per class via `getLogger(self.__class__.__name__)` (see `shared/logger.py`) — never the bare root `logging` module.
- Tests are `unittest.TestCase` + `unittest.mock`, one file per source module, named `test_<module>.py` under a mirrored path in `tests/`.
- [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, ...), one atomic commit per logical change.

## Project workflow

This project follows a spec-driven workflow under [`.specs/`](.specs/): each feature has a `spec.md` (requirements and decisions, authoritative for that feature), plus design and tasks artifacts. Cross-feature decisions and the paused-work snapshot live in [`.specs/STATE.md`](.specs/STATE.md).

Note for contributors: per project decision `AD-008` in `STATE.md`, an AI agent working in this repository has its authorship over production/test code decided per task (`own`/`paired`/`deliver`), derived from the project author's tracked knowledge state — it is not a blanket "agent never writes code" rule. See `.claude/rules/interaction-protocol.md` for the exact scope.
