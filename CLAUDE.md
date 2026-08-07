# streaming-project

## Purpose

This is a hands-on learning project (see `.specs/RFC.md`). The point is for the user to build a streaming data pipeline while actually learning Kafka, Flink, distributed storage/observability tooling (OpenSearch, Grafana), and how to plan an architecture and choose components - not to have those things handed to them working. Kubernetes, Drone (CI/CD), and Terraform (IaC) are opportunistic additions if a natural chance to use them shows up; they are not required.

## Critical interaction rule

**Inside the `tlc-spec-driven` workflow** (Specify/Design/Tasks/Execute for any feature under `.specs/features/`): mentor mode applies. The agent never authors production or test code - it explains concepts, points at the approach, gives isolated examples/pseudocode on request, and reviews what the user writes. The agent still runs the gate (tests) and creates the commit once the user's code passes. This is recorded as `AD-001` in `.specs/STATE.md` - read it before starting Design or Execute on any feature.

**Outside that formal flow**: small, explicitly-requested and user-confirmed fixes (a rename, a translation pass, a mechanical cleanup) are fine for the agent to execute directly - the user has already made that call in-session (e.g. the PT→EN log-message migration in commit `a819a55`). When in doubt about which mode applies, ask.

**Teaching style**: whenever the agent is explaining, reviewing, or debugging with the user, follow `.claude/skills/technical-learning-mentor` - analogies before definitions, strengths before gaps, guided hints over ready-made answers, adaptive to what the user already knows. This applies regardless of which mode above is active.

## Language

Chat responses: Portuguese (PT-BR), matching how the user writes. Code, identifiers, log messages, docstrings, and commit messages: English - this is already the codebase's convention (logs were deliberately migrated PT→EN; class/function/variable names have always been English).

## Source of truth

`.specs/features/[feature]/spec.md` is authoritative for that feature's requirements and decisions - it supersedes any older planning notes if they ever disagree. `.specs/STATE.md` holds project-wide decisions (`## Decisions`, `AD-NNN`) and the paused-work snapshot (`## Handoff`). Read both before resuming work on a feature.

## Architecture

**Today**: `ingestion/` polls the GitHub public events API, normalizes payloads into a `RawEvent` envelope (Pydantic), and publishes them to a single Kafka topic (`events-raw`, shared across sources - GitLab support exists as an unwired model stub). Local Kafka runs via `docker/docker-compose.yml` (3 controllers + 3 brokers + Kafka UI on `localhost:8080`).

**Planned, not yet built**: Flink jobs consuming `events-raw` for normalization/aggregation, OpenSearch as the metrics/search store, Grafana dashboards on top. See `.specs/features/github-ingestion/spec.md` for the current feature's exact scope and what's still open (P2: poll-loop resilience, GitHub rate-limit handling, dedup, configurable poll interval).

## Repository layout

- `ingestion/` - the GitHub/GitLab ingestion service (client → engine → publisher → pipeline, wired by `app.py`)
- `shared/` - cross-cutting utilities (`logger.py` for per-module logging setup, `timer.py` for `RetryTimer` backoff)
- `tests/` - mirrors `ingestion/`/`shared/` 1:1, one test file per source module
- `docker/` - local infra (`docker-compose.yml`)
- `flink/`, `infra/` - scaffolding for future work, currently empty
- `.specs/` - the spec-driven workflow: `RFC.md` (why this project exists), `STATE.md` (decisions + handoff), `features/[name]/` (spec, tasks, validation per feature)

## Temporary files

Any temporary file related to this project (scratch scripts, packaged archives, exported samples, etc.) goes in `tmp/` at the repo root - not the harness's scratchpad, not `/tmp`. `tmp/` already holds working artifacts like `sample_analysis.py`, `event_sample.json`, `dedup_challenge.ipynb`.

## Dev commands

**Always run these with `.venv` activated** (`source .venv/bin/activate`, prompt shows `(.venv)`). The Makefile targets call bare `pytest`/`python`/`autoflake`/`isort`/`black` and rely on `PATH` resolving to `.venv/bin`; without activation they silently fall back to the system interpreter/tools (missing dev deps like `pytest-cov`, or "command not found"). If a `make` command fails this way, the fix is to activate the venv, not to hardcode `.venv/bin/...` paths into the Makefile.

- `make test` - full suite with coverage (`pytest ... --cov=. --cov-report=term-missing tests`)
- `make neat` - format/clean (`autoflake` + `isort` + `black` over `shared ingestion tests`)
- `make clean` - remove `__pycache__`/`.pyc`
- `make kafka-up` / `make kafka-down` - local Kafka stack via Docker Compose
- `make ingestion-default` - run the ingestion service against GitHub (`python -m ingestion.app --source github`)

## Code conventions already in place

- Abstract base classes + constructor injection for testability (`IngestionClientBase`, `IngestionEngineBase`, `IngestionPublisherBase`) - follow this shape for new components instead of hard-wiring dependencies.
- Pydantic models for anything crossing a boundary (`GitHubEvent`, `RawEvent`).
- One logger per class via `getLogger(self.__class__.__name__)` (see `shared/logger.py`) - never the bare root `logging` module.
- Tests are `unittest.TestCase` + `unittest.mock`, one file per source module, named `test_<module>.py` under a mirrored path in `tests/`.
- Conventional Commits (`feat:`, `fix:`, ...), one atomic commit per logical change.
