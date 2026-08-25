# Interface Layout Specification

## Problem Statement

The platform's three stages (ingestion, normalization, analytics) each read their user-authored
configuration from a different, unrelated location (`ingestion/config/sources.yml`,
`flink/normalization/sources/*.yml`, a query file named by an env var inside
`flink/aggregation/queries/`), and the analytics stage's directory is still named `aggregation`
even though the platform-refactor plan (`tmp/platform-refactor-plan-v2.md`) scopes it to grow beyond
aggregation. This is F1 of that plan: a pure relocation - no new behavior, no new contract fields -
that puts every source's configuration under one root (`interface/sources/<name>/`) and every
analytics query under another (`interface/analytics/`), and renames the stage to match. It goes
first so later features (contract field types, per-source topics, view generation) are built on the
final file layout instead of migrating twice.

## Goals

- [ ] Every source's ingestion and normalization configuration lives under
      `interface/sources/<name>/`, one file per concern
- [ ] Every analytics query lives under `interface/analytics/`
- [ ] The analytics stage's code, image, and compose services are named `analytics`, not
      `aggregation`
- [ ] Every existing behavior is bit-for-bit unchanged: ingestion publishes the same events,
      normalization emits the same normalized events, the analytics job produces the same
      aggregated output as `flink-aggregation`'s live-verified 2026-08-25 run

## Out of Scope

Explicitly excluded - each belongs to a later feature in the platform-refactor plan.

| Feature | Reason |
| --- | --- |
| `type:` field vocabulary / removing `as:` | F2 (`contract-field-types`) |
| Per-source normalized Kafka topic | F3 (`normalized-topic-per-source`) |
| Generated source tables / views for analytics | F4 (`analytics-view-generation`) |
| `make validate` | Built incrementally in F2/F4, not here |
| Any change to `RawEvent`, `NormalizedEvent`, or the JMESPath compiler's behavior | This feature touches only *where* config is read from, never how it is interpreted |
| GitLab normalization contract | Does not exist today (GitLab is an unwired ingestion stub per `CLAUDE.md`); only its `ingestion.yml` is relocated |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Per-source ingestion file name | `ingestion.yml` | Matches `normalization.yml`'s naming inside the same source directory; mirrors the approved plan's own example | y (plan, 2026-08-25) |
| Per-source normalization file name | `normalization.yml` | Same as above | y (plan) |
| Analytics query directory name | `interface/analytics/` (flat, no per-source subdirectory) | Decided in the plan: analytics is per-*question*, not per-source - it may cross sources, so it does not nest under `interface/sources/` | y (plan) |
| Env var rename | `AGGREGATION_QUERY_FILE` → `ANALYTICS_QUERY_FILE` | Follows the directory/stage rename; no reason to keep the old name once the module is `analytics` | y (plan) |
| Compose service/image naming | `aggregation` → `analytics`, `apache/flink:2.3-aggregation` → `apache/flink:2.3-analytics` | Same rename, applied consistently | y (plan) |
| GitLab's `ingestion.yml` | Relocated even though GitLab has no normalization contract and is unwired | `ingestion/config/sources.yml` today holds both `github` and `gitlab` entries; relocating only one would leave the loader half-migrated | Assumption - not explicitly discussed, low risk since GitLab is inert |
| Unknown-source error contract | Both loaders keep raising `NotImplementedError` (ingestion) - `ValueError`/`NotImplementedError` behavior otherwise untouched | Preserves existing call-site contracts (`ingestion/app.py`, `NormalizationFlatMapFunction.flat_map`) with zero behavior change | y (plan - "no behavior change") |
| Whether `flink/aggregation/`'s Python module itself (not just the directory) needs internal renames (class names, docstrings) | Only the directory/file path and the Dockerfile/compose references are renamed; `flink/analytics/app.py`'s internal content is otherwise untouched in this feature | F1 is relocation only; anything beyond path changes belongs to F4, which rewrites `app.py`'s body anyway | y (plan - "mechanical, no behavior change") |

**Open questions:** none - all resolved above or already decided in the approved plan.

---

## User Stories

### P1: Ingestion reads per-source configuration ⭐ MVP

**User Story**: As the platform, I want each source's ingestion configuration in its own file under
`interface/sources/<name>/ingestion.yml` so that a source's full configuration (ingestion +
normalization) lives in one directory instead of a shared file.

**Why P1**: Everything else in this feature depends on the same per-source loading pattern; this is
the first stage to prove it end-to-end.

**Acceptance Criteria**:

1. WHEN `get_source_config("github", ...)` is called THEN the system SHALL load
   `interface/sources/github/ingestion.yml` and return a `SourceConfig` with the same fields
   (`endpoints`, `headers`, `auth`, `id_field`, `type_field`) as today's
   `ingestion/config/sources.yml`'s `github` entry produces.
2. WHEN `get_source_config("gitlab", ...)` is called THEN the system SHALL load
   `interface/sources/gitlab/ingestion.yml` and return the equivalent `SourceConfig` for `gitlab`.
3. IF `get_source_config` is called with a source that has no `interface/sources/<source>/ingestion.yml`
   THEN the system SHALL raise `NotImplementedError` (unchanged from today's behavior for an unknown
   source key).
4. The system SHALL NOT read `ingestion/config/sources.yml` after this feature is complete (the file
   is removed, not left as a second source of truth).

**Independent Test**: Run `make ingestion-default` (`python -m ingestion.app --source github`)
against the running Kafka stack and confirm it starts polling exactly as before - same endpoint URL
resolved, same auth header applied.

---

### P2: Normalization reads contracts from the same source directory

**User Story**: As the platform, I want the normalization contract for a source to live at
`interface/sources/<name>/normalization.yml`, next to that source's ingestion config, so both halves
of a source's configuration are co-located.

**Why P2**: Depends on P1's directory existing; delivers the second half of the co-location goal.

**Acceptance Criteria**:

1. WHEN `YamlContractRepository.get("github")` is called THEN the system SHALL load
   `interface/sources/github/normalization.yml` and return a `NormalizationContract` identical in
   content to today's `flink/normalization/sources/github.yml`.
2. IF `YamlContractRepository.get(source)` is called for a source with no
   `interface/sources/<source>/normalization.yml` THEN the system SHALL raise `NotImplementedError`
   (unchanged from today).
3. WHEN the normalization Flink job processes a `github` `RawEvent` THEN the resulting
   `NormalizedEvent` (field names, values, types) SHALL be identical to what today's contract at
   `flink/normalization/sources/github.yml` produces for the same input.

**Independent Test**: Feed the same hand-crafted GitHub events used in
`tests/flink/normalization/test_contracts_github.py` through the relocated contract and confirm
identical `NormalizedEvent` output.

---

### P3: The analytics stage is renamed and its queries relocated

**User Story**: As the platform, I want the directory, Docker image, compose services, and env var
that today say "aggregation" to say "analytics", and the query files to live under
`interface/analytics/`, so the stage's name matches its future scope (filters, joins, derived
fields - not only aggregation) and its query files sit alongside the other authored content.

**Why P3**: Completes F1's rename; every later feature (F2-F4) is written against `flink/analytics/`
and `interface/analytics/`, not the old names.

**Acceptance Criteria**:

1. The system SHALL expose the analytics job's code at `flink/analytics/` (not
   `flink/aggregation/`), with the same `app.py` behavior (statement-split, checkpointing, generic
   script runner) as today's `flink/aggregation/app.py`.
2. WHEN the analytics container starts THEN the system SHALL read the query file named by the
   `ANALYTICS_QUERY_FILE` environment variable (replacing `AGGREGATION_QUERY_FILE`) from
   `interface/analytics/`.
3. The system SHALL build the analytics Docker image from `flink/analytics/Dockerfile`, which
   copies `flink/analytics/`, `shared/`, and `interface/` (not `flink/aggregation/`).
4. The `docker-compose.yml` service pair SHALL be named `analytics`/`taskmanager-analytics`
   (replacing `aggregation`/`taskmanager-aggregation`), with the taskmanager image tagged
   `apache/flink:2.3-analytics` (replacing `-aggregation`).
5. WHEN `repo_counts_5m.sql` runs under the renamed stage with `ANALYTICS_QUERY_FILE=repo_counts_5m.sql`
   THEN the system SHALL produce output in `events-aggregated` identical to `flink-aggregation`'s
   T6 live-verified result (2026-08-25): correct per-window counts, late event dropped, malformed
   message skipped without killing the job, mid-window restart resumes state, post-window restart
   produces no duplicate.

**Independent Test**: `docker compose -f infra/docker/docker-compose.yml up`, re-run
`flink-aggregation`'s T6 procedure exactly (publish the same hand-crafted `events-normalized`
messages, inspect `events-aggregated` in Kafka UI), confirm identical results.

---

## Edge Cases

- IF a source directory exists under `interface/sources/` but is missing `ingestion.yml` (only
  `normalization.yml` present) THEN `get_source_config` SHALL raise `NotImplementedError` exactly as
  it does for a source with no directory at all (no special-cased partial state).
- IF a source directory is missing `normalization.yml` (only `ingestion.yml` present) THEN
  `YamlContractRepository.get` SHALL raise `NotImplementedError` exactly as it does for a source with
  no contract at all today (GitLab is exactly this case, on purpose - Out of Scope table).
- WHEN both compose service pairs (`normalization`/`taskmanager-normalization` and
  `analytics`/`taskmanager-analytics`) start against the same `interface/` mount THEN neither
  SHALL fail to resolve its files due to a shared-mount race (both are read-only `COPY`, baked at
  build time - no runtime mount contention exists in the current architecture, verified in
  Out-of-Scope's "no behavior change" constraint).

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| ILO-01 | P1: Ingestion per-source config | Tasks | Pending |
| ILO-02 | P1: Ingestion per-source config | Tasks | Pending |
| ILO-03 | P1: Ingestion per-source config | Tasks | Pending |
| ILO-04 | P1: Ingestion per-source config | Tasks | Pending |
| ILO-05 | P2: Normalization contract relocation | Tasks | Pending |
| ILO-06 | P2: Normalization contract relocation | Tasks | Pending |
| ILO-07 | P2: Normalization contract relocation | Tasks | Pending |
| ILO-08 | P3: Analytics stage rename | Tasks | Pending |
| ILO-09 | P3: Analytics stage rename | Tasks | Pending |
| ILO-10 | P3: Analytics stage rename | Tasks | Pending |
| ILO-11 | P3: Analytics stage rename | Tasks | Pending |
| ILO-12 | P3: Analytics stage rename | Tasks | Pending |

**Coverage:** 12 total, 0 mapped to tasks yet, 12 unmapped ⚠️ (mapped during Tasks phase)

---

## Success Criteria

- [ ] `make test` passes with no regression (same pass count as `flink-aggregation`'s last run, 188
      passed + 11 subtests, adjusted only for path-dependent test updates)
- [ ] `docker compose -f infra/docker/docker-compose.yml up` reproduces `flink-aggregation` T6's
      live-verified result exactly, under the renamed stage
- [ ] `ingestion/config/sources.yml` and `flink/normalization/sources/github.yml` no longer exist -
      `interface/sources/*/` is the only place either configuration is read from
- [ ] `flink/aggregation/` no longer exists - `flink/analytics/` is the only analytics module
