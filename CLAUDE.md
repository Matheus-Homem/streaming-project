# streaming-project

## Purpose

This is a hands-on learning project. See [`docs/VISION.md`](docs/VISION.md) for what the platform is, why it exists, and its technology list, and [`docs/USE-CASES.md`](docs/USE-CASES.md) for the problem it's currently pointed at (the Conviction Index).

## Interaction & conventions

See `.claude/rules/interaction-protocol.md` (per-task authorship levels, no-agent-commits, AD-001/002/003/008) and `.claude/rules/python-conventions.md` (code style). These load automatically; this file stays as project overview and architecture.

Authorship is not blanket: each task is `own`, `paired`, or `deliver`, derived from the knowledge state and recorded in `.mentor/features/<slug>/map.md`. Read that file's levels before writing anything for a task.

## Language

Chat: PT-BR, matching the user. Everything else is English by default (code, identifiers, logs, docstrings, commits, and every project doc - this file, `.claude/rules/*.md`, `.claude/ref/*`, `.specs/**`, and `.mentor/`'s bookkeeping files, including node ids and the `aliases`/`note` columns). One exception: class material meant for the user to read, inside `.mentor/**/classes/<topic-slug>/`, stays PT-BR.

## Source of truth

`.specs/features/[feature]/spec.md` is authoritative for that feature's requirements and decisions - it supersedes any older planning notes if they ever disagree. `.specs/STATE.md` holds project-wide decisions (`## Decisions`, `AD-NNN`) and the paused-work snapshot (`## Handoff`). Read both before resuming work on a feature.

## Architecture

**Today**: `ingestion/` polls a source's public events API (GitHub wired, GitLab config-only), formats payloads into a `RawEvent` envelope (Pydantic, `shared/models.py`), and publishes them to a single Kafka topic (`events-raw`, shared across sources). `flink/normalization/` consumes `events-raw`, applies a per-source declarative YAML contract (`interface/sources/<name>/normalization.yml`, no source-specific Python), and publishes `NormalizedEvent`s to `events-normalized`, keyed by `partition_key`. `flink/analytics/` runs a Flink SQL/Table API job (the query file itself is the contract, per `AD-009`) over `events-normalized`, windowing counts into `events-analytics`. Local infra (Kafka cluster + Kafka UI on `localhost:8080`, Flink job/task managers per stage) runs via `infra/docker/docker-compose.yml` (`docker-compose.dev.yml` overlay for a lighter single-broker footprint, `MODE=dev`).

**Planned, not yet built**: OpenSearch as the metrics/search store, Grafana dashboards on top. See `.specs/features/` for each stage's exact scope (`streaming-ingestion`, `flink-normalization`, `flink-aggregation`, `interface-layout`).

## Repository layout

- `ingestion/` - the ingestion service: `ports.py` (`EventClientPort`, `EventProducerPort`), `adapters/`, `domain/` (formatter, source-config repository, duplicate tracker), wired by `use_case.py`'s `IngestionPipeline`, run via `app.py`
- `flink/normalization/` - the normalization job: `ports.py` (`TransformerPort`), `domain/` (`EventNormalizer`, contract evaluator/repository), `models.py` (`FieldRule`/`NormalizationContract`/`NormalizedEvent`), `adapters/transformer.py` (the PyFlink `FlatMapFunction`)
- `flink/analytics/` - the aggregation job: runs a `.sql` file from `interface/analytics/` as Flink SQL/Table API statements
- `flink/common/` - Kafka source/sink building shared by both Flink jobs (`ports.py`, `adapters/`, `factory.py`)
- `interface/` - declarative contracts, not Python: `sources/<name>/{ingestion,normalization}.yml`, `analytics/*.sql`
- `shared/` - cross-cutting utilities (`models.py` for `RawEvent`, `logger.py` for per-module logging setup)
- `tests/` - mirrors `ingestion/`/`flink/`/`shared/` 1:1, one test file per source module
- `infra/docker/` - local infra (`docker-compose.yml`, `docker-compose.dev.yml`, topic-init script)
- `docs/` - `VISION.md` (what the platform is and why) and `USE-CASES.md` (what it's used for) - never references `.specs/` or `.mentor/`
- `.specs/` - the spec-driven workflow: `STATE.md` (decisions + handoff), `PLATFORM.md` (contract-driven-authoring decision), `features/[name]/` (spec, design, tasks per feature)

## Temporary files

Any temporary file related to this project (scratch scripts, packaged archives, exported samples, etc.) goes in `tmp/` at the repo root - not the harness's scratchpad, not `/tmp`. `tmp/` already holds working artifacts like `sample_analysis.py`, `dedup_challenge.ipynb`, `action_plan.md`.

## Dev commands

**Always run with `.venv` activated** (`source .venv/bin/activate`) - the Makefile calls bare `pytest`/`python`/etc. and relies on `PATH`, so without activation it silently falls back to the system interpreter/tools instead of failing loud. Fix by activating, never by hardcoding `.venv/bin/...` into the Makefile.

- `make test` - full suite with coverage (`pytest ... --cov=ingestion --cov=flink --cov-report=term-missing tests`)
- `make neat` - format/clean (`autoflake` + `isort` + `black` over `ingestion flink shared tests`)
- `make clean` - remove `__pycache__`/`.pyc`
- `make kafka-up` / `make kafka-down` - local Kafka + Flink stack via Docker Compose (`MODE=dev` for the lighter single-broker overlay)
- `make ingestion-default` - run the ingestion service against GitHub (`python -m ingestion.app --source github`)

## Code conventions

See `.claude/rules/python-conventions.md` (loads automatically on `.py` files) and `.claude/rules/flink-contract-dsl.md` (Flink normalization contract-compiler gotchas, loads on `flink/normalization/**`).

<!-- BEGIN technical-learning-mentor -->
@.claude/mentor-design-pairing.md
<!-- END technical-learning-mentor -->
