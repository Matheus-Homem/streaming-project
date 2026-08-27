---
description: Architecture rule - adapters behind ports, domain-neutral internal envelope
path: "ingestion/**|flink/**"
---

## Every adapter sits behind a port/ABC

Any component that translates an external, source-specific shape (a GitHub payload, a GitLab one, a future API's) into the pipeline's internal common shape is built behind a port/ABC (`ingestion/ports.py`: `EventClientPort`, `EventProducerPort`; `ingestion/domain/__init__.py`: `SourceConfigRepository`, `RawEventFormatter`, `DuplicateTracker`; `flink/normalization/ports.py`: `TransformerPort`; `flink/normalization/domain/__init__.py`: `ContractRepository`, `EventEvaluator`) - never with a specific source's shape hard-coded directly into shared pipeline code.

## The internal common envelope is domain-neutral

`RawEvent` (`shared/models.py`) never carries VCS-specific vocabulary (`repo_name`, `org_login`, etc.) as a top-level field - it holds only the generic: `source`, `source_event_id`, `source_event_type`, `source_event_endpoint`, `observed_at`, `schema_version`, and an opaque `payload`. `NormalizedEvent` (`flink/normalization/models.py`), built from `RawEvent` plus a per-source YAML contract, adds the pipeline's generic downstream fields - `partition_key`, `entity_id`, `entity_name`, `event_time` - never a source-specific name for the same concept (e.g. `actor_login`). Source-specific fields are declared per contract and land as opaque extras on `NormalizedEvent`, never hard-coded into shared pipeline code.

## Full context and history

This is the current operative rule, condensed. The original decision, its reason (the long-term vision of an API-agnostic platform), and every amendment (`AD-004`, `AD-006`, `AD-009`) live in `.specs/STATE.md`. Check `STATE.md` before proposing a change to this rule - it is edited there, not here.
