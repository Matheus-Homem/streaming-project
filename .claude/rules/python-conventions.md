---
description: Python code conventions for this project
path: "**/*.py"
---

## Code conventions already in place

- Abstract base classes + constructor injection for testability (`IngestionClientBase`, `IngestionEngineBase`, `IngestionProducerBase`, `IngestionTrackerBase` - see `ingestion/ports.py`) - follow this shape for new components instead of hard-wiring dependencies.
- Pydantic models for anything crossing a boundary (`EventModel`/`SourceConfig` in `ingestion/models.py`, `RawEvent` in `shared/models.py`).
- One logger per class via `getLogger(self.__class__.__name__)` (see `shared/logger.py`) - never the bare root `logging` module.
- Tests are `unittest.TestCase` + `unittest.mock`, one file per source module, named `test_<module>.py` under a mirrored path in `tests/`.
- Conventional Commits (`feat:`, `fix:`, ...), one atomic commit per logical change.
