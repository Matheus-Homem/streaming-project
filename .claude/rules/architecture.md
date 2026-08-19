---
description: Architecture rule - adapters behind ports, domain-neutral internal envelope
path: "ingestion/**|flink/**"
---

## Every adapter sits behind a port/ABC

Any component that translates an external, source-specific shape (a GitHub payload, a GitLab one, a future API's) into the pipeline's internal common shape is built behind a port/ABC (`ingestion/ports.py`: `IngestionClientBase`, `IngestionEngineBase`, `IngestionProducerBase`, `IngestionTrackerBase`; `flink/normalization/ports.py`: `NormalizerBase`) - never with a specific source's shape hard-coded directly into shared pipeline code.

## The internal common envelope is domain-neutral

`RawEvent` (`shared/models.py`) and any equivalent envelope in future stages never carry VCS-specific vocabulary (`repo_name`, `org_login`, etc.) as top-level fields. That data lives in a source-specific fields block, populated by that source's normalizer/engine. The envelope itself holds only the generic: event id, event type, actor, observed timestamp, schema version, `partition_key`.

## Full context and history

This is the current operative rule, condensed. The original decision, its reason (the long-term vision of an API-agnostic platform), the trade-off and the amendments are in `.specs/STATE.md` (`AD-004`, mechanism amended by `AD-006` - today the Flink normalization layer uses a single `ContractNormalizer` driven by a declarative contract instead of one implementation per source, but the domain-neutral envelope principle still holds, now served by the contract-driven design rather than by the port being the only tool). Check `STATE.md` before proposing a change to this rule - it is edited there, not here.
