# streaming-project

## Purpose

This is a hands-on learning project (see `.specs/RFC.md`). The point is for the user to build a streaming data pipeline while actually learning Kafka, Flink, distributed storage/observability tooling (OpenSearch, Grafana), and how to plan an architecture and choose components - not to have those things handed to them working. Kubernetes, Drone (CI/CD), and Terraform (IaC) are opportunistic additions if a natural chance to use them shows up; they are not required.

## Interaction & conventions

See `.claude/rules/interaction-protocol.md` (per-task authorship levels, no-agent-commits, AD-001/002/003/008) and `.claude/rules/python-conventions.md` (code style). These load automatically; this file stays as project overview and architecture.

Authorship is not blanket: each task is `own`, `paired`, or `deliver`, derived from the knowledge state and recorded in `.mentor/features/<slug>/map.md`. Read that file's levels before writing anything for a task.

## Language

Chat responses: Portuguese (PT-BR), matching how the user writes. Code, identifiers, log messages, docstrings, and commit messages: English - this is already the codebase's convention (logs were deliberately migrated PT→EN; class/function/variable names have always been English). Project documents that state rules, record decisions, or summarize an external API are English too: this file, `.claude/rules/*.md`, `.claude/ref/*`, everything under `.specs/`, and `.mentor/`'s bookkeeping files (`profile.md`, `knowledge.md`, `map.md`) - they quote English identifiers throughout and `.specs/STATE.md` is their canonical source. The one exception is prose written as teaching material for the user to read: `.mentor/**/classes/` notes stay PT-BR.

## Source of truth

`.specs/features/[feature]/spec.md` is authoritative for that feature's requirements and decisions - it supersedes any older planning notes if they ever disagree. `.specs/STATE.md` holds project-wide decisions (`## Decisions`, `AD-NNN`) and the paused-work snapshot (`## Handoff`). Read both before resuming work on a feature.

## Architecture

**Today**: `ingestion/` polls the GitHub public events API, normalizes payloads into a `RawEvent` envelope (Pydantic), and publishes them to a single Kafka topic (`events-raw`, shared across sources - GitLab support exists as an unwired model stub). Local Kafka runs via `infra/docker/docker-compose.yml` (3 controllers + 3 brokers + Kafka UI on `localhost:8080`).

**Planned, not yet built**: Flink jobs consuming `events-raw` for normalization/aggregation, OpenSearch as the metrics/search store, Grafana dashboards on top. See `.specs/features/streaming-ingestion/spec.md` for the current feature's exact scope and what's still open (P2: poll-loop resilience, GitHub rate-limit handling, dedup, configurable poll interval).

## Repository layout

- `ingestion/` - the GitHub/GitLab ingestion service (client → engine → publisher → pipeline, wired by `app.py`)
- `shared/` - cross-cutting utilities (`logger.py` for per-module logging setup, `timer.py` for `RetryTimer` backoff)
- `tests/` - mirrors `ingestion/`/`shared/` 1:1, one test file per source module
- `infra/docker/` - local infra (`docker-compose.yml`, topic-init script) - moved here from `docker/` per `AD-005` (`.specs/STATE.md`)
- `flink/` - the Flink normalization job, in progress (`flink-normalization` feature); `infra/` also reserved for future `k8s/`/`terraform/` per `AD-005`
- `.specs/` - the spec-driven workflow: `RFC.md` (why this project exists), `STATE.md` (decisions + handoff), `features/[name]/` (spec, tasks, validation per feature)

## Temporary files

Any temporary file related to this project (scratch scripts, packaged archives, exported samples, etc.) goes in `tmp/` at the repo root - not the harness's scratchpad, not `/tmp`. `tmp/` already holds working artifacts like `sample_analysis.py`, `dedup_challenge.ipynb`, `action_plan.md`.

## Dev commands

**Always run these with `.venv` activated** (`source .venv/bin/activate`, prompt shows `(.venv)`). The Makefile targets call bare `pytest`/`python`/`autoflake`/`isort`/`black` and rely on `PATH` resolving to `.venv/bin`; without activation they silently fall back to the system interpreter/tools (missing dev deps like `pytest-cov`, or "command not found"). If a `make` command fails this way, the fix is to activate the venv, not to hardcode `.venv/bin/...` paths into the Makefile.

- `make test` - full suite with coverage (`pytest ... --cov=. --cov-report=term-missing tests`)
- `make neat` - format/clean (`autoflake` + `isort` + `black` over `shared ingestion tests`)
- `make clean` - remove `__pycache__`/`.pyc`
- `make kafka-up` / `make kafka-down` - local Kafka stack via Docker Compose
- `make ingestion-default` - run the ingestion service against GitHub (`python -m ingestion.app --source github`)

## Code conventions

See `.claude/rules/python-conventions.md` (loads automatically on `.py` files) and `.claude/rules/flink-contract-dsl.md` (Flink normalization contract-compiler gotchas, loads on `flink/normalization/**`).

<!-- BEGIN technical-learning-mentor -->
@.claude/mentor-design-pairing.md
<!-- END technical-learning-mentor -->
