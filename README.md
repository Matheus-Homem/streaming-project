# streaming-project

A hands-on learning project for building a streaming data pipeline end to end — Kafka, Flink, distributed storage/search (OpenSearch), and observability (Grafana) — while actually planning the architecture and choosing components, rather than having them handed over working. Kubernetes, Drone (CI/CD), and Terraform (IaC) are opportunistic additions if a natural chance to use them comes up.

See [`.specs/RFC.md`](.specs/RFC.md) for the full motivation and learning goals, and [`.specs/STATE.md`](.specs/STATE.md) for project-wide decisions and the current handoff state.

## Architecture

**Today**: [`ingestion/`](ingestion/) polls the GitHub public events API, normalizes payloads into a `RawEvent` envelope ([Pydantic](https://docs.pydantic.dev/)), and publishes them to a single Kafka topic (`events-raw`, shared across sources — GitLab support exists as an unwired model stub). Local Kafka runs via [`docker/docker-compose.yml`](docker/docker-compose.yml) (3 controllers + 3 brokers + Kafka UI on `localhost:8080`).

**Planned, not yet built**: Flink jobs consuming `events-raw` for normalization/aggregation, OpenSearch as the metrics/search store, Grafana dashboards on top. See [`.specs/features/streaming-ingestion/spec.md`](.specs/features/streaming-ingestion/spec.md) for the ingestion feature's exact scope, and [`.specs/features/flink-normalization/`](.specs/features/flink-normalization/) for the normalization layer currently being planned.

```
GitHub Events API --> ingestion (client -> engine -> publisher) --> Kafka (events-raw)
                                                                          |
                                                                    [planned] Flink normalization/aggregation
                                                                          |
                                                                    [planned] OpenSearch --> Grafana
```

## Repository layout

- [`ingestion/`](ingestion/) — the GitHub/GitLab ingestion service (client → engine → publisher → pipeline, wired by `app.py`)
- [`shared/`](shared/) — cross-cutting utilities (`logger.py` for per-module logging setup, `timer.py` for `RetryTimer` backoff)
- [`tests/`](tests/) — mirrors `ingestion/`/`shared/` 1:1, one test file per source module
- [`docker/`](docker/) — local infra (`docker-compose.yml`)
- [`flink/`](flink/), [`infra/`](infra/) — scaffolding for future work, currently empty
- [`.specs/`](.specs/) — the spec-driven workflow: `RFC.md` (why this project exists), `STATE.md` (decisions + handoff), `features/[name]/` (spec, tasks, validation per feature)

## Requirements

- Python 3.12
- Docker (with Docker Compose) for the local Kafka stack

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

All `make` targets below assume `.venv` is activated — the Makefile calls bare `pytest`/`python`/`autoflake`/`isort`/`black`, which resolve to `.venv/bin` only while it's active.

## Usage

Start the local Kafka stack:

```bash
make kafka-up
```

Kafka UI is then available at `localhost:8080`.

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

- Abstract base classes + constructor injection for testability (`IngestionClientBase`, `IngestionEngineBase`, `IngestionPublisherBase`) — new components follow this shape instead of hard-wiring dependencies.
- Pydantic models for anything crossing a boundary (`GitHubEvent`, `RawEvent`).
- One logger per class via `getLogger(self.__class__.__name__)` (see `shared/logger.py`) — never the bare root `logging` module.
- Tests are `unittest.TestCase` + `unittest.mock`, one file per source module, named `test_<module>.py` under a mirrored path in `tests/`.
- [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, ...), one atomic commit per logical change.

## Project workflow

This project follows a spec-driven workflow under [`.specs/`](.specs/): each feature has a `spec.md` (requirements and decisions, authoritative for that feature), plus tasks and validation artifacts. Cross-feature decisions and the paused-work snapshot live in [`.specs/STATE.md`](.specs/STATE.md).

Note for contributors: per project decision `AD-001` in `STATE.md`, an AI agent working in this repository operates in mentor mode and does not author production or test code directly — implementation is done by the project's author, with the agent explaining concepts and reviewing the result.
