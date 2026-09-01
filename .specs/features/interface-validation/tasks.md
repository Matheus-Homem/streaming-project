# Interface Validation Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is
the source of truth for the full flow (per-task cycle, sub-agent delegation, adequacy review,
Verifier, discrimination sensor).

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

**Design**: skipped for this feature (Medium scope per the auto-sizing table - design decided
inline). Every structural choice below (module split under `scripts/`, the TVF-masking approach,
GitHub Actions as the CI platform, the Makefile fix) was already surfaced as an explicit trade-off
and confirmed by the user during Specify - not re-litigated here.

**Authorship** (`AD-008`): no `.mentor/features/interface-validation/map.md` exists, and none of
these tasks advance a `knowledge.md` objective - this is infrastructure/tooling work (a validator
CLI, a CI workflow, a one-line Makefile fix), not the streaming-pipeline domain the mentor mapping
tracks. Per `AD-008`'s scope gate, every task here is `deliver` (agent writes, user reviews) -
derived and confirmed in-session, not a blanket default.

**Prerequisite**: `conviction-index-foundations`' remaining stories (P2-P6) run first. Those stories
keep changing files under `interface/` (a new `stream:` block, five new sources' own
`ingestion`/`normalization.yml`, a `map:` key); executing T1-T6 below before that work lands would
validate a moving target and need re-verification anyway. `interface-validation`'s own place in the
project-wide sequence is `STATE.md`'s Handoff, not this file.

---

**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase sampling (`tests/flink/normalization/`, `tests/ingestion/`) and the
> Makefile. No dedicated testing-guidelines file found (`AGENTS.md` absent) - coverage expectations
> follow the strong default (1:1 to spec ACs, every listed edge case) plus the existing suite's own
> depth as a floor. `scripts/` is a new top-level tree (no prior tests to sample there); its test
> location mirrors the project's existing `tests/<domain>/` 1:1 convention.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| YAML contract validator (`scripts/validate_yaml_contracts.py`) | unit | 1:1 to IVL-01..06 + both listed P1 edge cases | `tests/scripts/test_validate_yaml_contracts.py` | `python -m pytest tests/scripts/test_validate_yaml_contracts.py` |
| Windowing-TVF mask + shape check (`scripts/validate_sql_contracts.py::mask_windowing_tvfs`) | unit | 1:1 to IVL-08/IVL-10, both fixture behaviors already prototyped this session (real syntax error outside a TVF; malformed TVF shape) | `tests/scripts/test_validate_sql_contracts.py` | `python -m pytest tests/scripts/test_validate_sql_contracts.py` |
| SQL contract validator (`scripts/validate_sql_contracts.py::validate_sql_contracts`) | unit | 1:1 to IVL-07/09/11 + both listed P2 edge cases | `tests/scripts/test_validate_sql_contracts.py` | `python -m pytest tests/scripts/test_validate_sql_contracts.py` |
| Aggregator entrypoint (`scripts/validate.py`) | unit | 1:1 to IVL-12..16 (exit code, aggregated report, both-fail case) | `tests/scripts/test_validate.py` | `python -m pytest tests/scripts/test_validate.py` |
| `Makefile`'s `test` target | none | build gate only - verified by an intentional-fail smoke check, not a unit test | - | `make test` |
| CI workflow (`.github/workflows/ci.yml`) | none | build gate only - verified live via an actual PR per the spec's Independent Test, not a unit test | - | manual PR verification |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After T1, T2, T3, T4 | `python -m pytest tests/scripts` |
| Full | After T4 (closes P1-P3) | `make test && make validate` |
| Build | After T5, T6 | `make test` (smoke-check exit code), then open a PR (manual, per spec Independent Test) |

---

## Execution Plan

Phases are ordered and run sequentially - each phase completes before the next begins.

### Phase 1: YAML validation

```
T1
```

### Phase 2: SQL validation

```
T2 → T3
```

### Phase 3: Aggregator + `make validate`

```
T4
```

### Phase 4: Makefile fix

```
T5
```

### Phase 5: CI pipeline

```
T6
```

---

## Task Breakdown

### T1: `scripts/validate_yaml_contracts.py`

**What**: `validate_yaml_contracts(sources_dir: Path) -> list[str]` - iterates every directory under
`sources_dir`, loads `ingestion.yml` (if present) against `SourceYamlEntry` and `normalization.yml`
(if present) against `NormalizationContract`, checks a loaded `normalization.yml`'s `source:` value
against its parent directory name, and returns one human-readable failure string per problem found
(empty list = all pass). Never raises - a `ValidationError` becomes a formatted string in the
returned list, not an uncaught exception.
**Where**: `scripts/validate_yaml_contracts.py`
**Depends on**: None
**Reuses**: `SourceYamlEntry` (`ingestion/models.py`), `NormalizationContract` (`flink/normalization/models.py`) - both already exist and already do the real validation work; this task only orchestrates calling them and collecting results
**Requirement**: IVL-01, IVL-02, IVL-03, IVL-04, IVL-05, IVL-06

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] Every `interface/sources/<name>/ingestion.yml` that exists is loaded and validated
- [ ] Every `interface/sources/<name>/normalization.yml` that exists is loaded and validated
- [ ] A source directory with only one of the two files does not produce a failure for the missing one
- [ ] A `normalization.yml` whose `source:` does not match its directory name produces a failure
      naming both values
- [ ] A Pydantic `ValidationError` on either file becomes a failure string carrying the file path and
      the original error message, not an uncaught exception
- [ ] Every directory under `interface/sources/` is checked, regardless of whether a real
      client/normalizer adapter exists for it (e.g. `gitlab`, ingestion-only today)
- [ ] `interface/sources/` with no subdirectories returns an empty failure list, does not raise
- [ ] A source directory with neither file present is skipped without producing a failure
- [ ] Gate check passes: `python -m pytest tests/scripts/test_validate_yaml_contracts.py`
- [ ] Test count recorded

**Tests**: unit
**Gate**: quick

**Commit**: `feat(scripts): add YAML contract validator for interface/sources`

---

### T2: `scripts/validate_sql_contracts.py` - windowing-TVF mask + shape check

**What**: `mask_windowing_tvfs(sql: str) -> tuple[str, list[str]]` - finds every
`TABLE(TUMBLE(...)|HOP(...)|CUMULATE(...)|SESSION(...))` span in the given SQL text via paren-depth
scanning (not regex alone - the call nests parens), replaces each with a syntactically-inert
placeholder identifier, and separately checks each found span's shape against Flink's documented
`TVF(TABLE data, DESCRIPTOR(timecol), size[, offset])` grammar. Returns the masked text plus a list
of shape-violation strings (empty = all found TVF calls are well-formed).
**Where**: `scripts/validate_sql_contracts.py`
**Depends on**: None
**Reuses**: The paren-counting + regex-shape approach prototyped and verified this session against
`interface/analytics/repo_counts_5m.sql` (real file - passes) and two injected-fault fixtures (a
real syntax typo outside the TVF; a malformed TVF missing `DESCRIPTOR(...)`'s parens) - both
correctly failed in the prototype
**Requirement**: IVL-08, IVL-10

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] `TABLE(TUMBLE(...))`, `HOP`, `CUMULATE`, `SESSION` spans are found and masked out of the
      returned text, regardless of internal newlines/whitespace
- [ ] A well-formed TVF call (matching `interface/analytics/repo_counts_5m.sql`'s real shape)
      produces zero shape-violation strings
- [ ] A malformed TVF call (missing `DESCRIPTOR(...)`, missing/malformed `INTERVAL '<n>' <unit>`)
      produces a shape-violation string naming the offending call text
- [ ] SQL with no windowing TVF calls at all returns the text unchanged and an empty violation list
- [ ] Gate check passes: `python -m pytest tests/scripts/test_validate_sql_contracts.py`
- [ ] Test count recorded

**Tests**: unit
**Gate**: quick

---

### T3: `scripts/validate_sql_contracts.py` - `sqlfluff` integration

**What**: `validate_sql_contracts(analytics_dir: Path) -> list[str]` - iterates every `.sql` file
under `analytics_dir`, runs each through T2's `mask_windowing_tvfs`, runs the masked text through
`sqlfluff parse --dialect flink` (subprocess, matching the prototype), and merges T2's shape
violations with any `sqlfluff` parse violations (file path + line + position from `sqlfluff`'s own
output) into one returned failure list.
**Where**: `scripts/validate_sql_contracts.py`
**Depends on**: T2
**Reuses**: T2's `mask_windowing_tvfs`; the `sqlfluff parse --dialect flink` subprocess invocation
prototyped this session (verified clean exit against the real, masked `repo_counts_5m.sql`, and
non-zero against an injected syntax error outside the TVF span)
**Requirement**: IVL-07, IVL-09, IVL-11

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] Every `.sql` file under `interface/analytics/` is run through the masked-`sqlfluff` check
- [ ] A real syntax error outside any TVF call produces a failure string carrying `sqlfluff`'s file,
      line, and position
- [ ] `interface/analytics/repo_counts_5m.sql` (the real file) produces zero failures
- [ ] `interface/analytics/` with no `.sql` files returns an empty failure list, does not raise
- [ ] An empty `.sql` file produces zero failures
- [ ] Gate check passes: `python -m pytest tests/scripts/test_validate_sql_contracts.py`
- [ ] Test count recorded (includes T2's cases - same file)

**Tests**: unit
**Gate**: quick

**Commit**: `feat(scripts): add SQL contract validator with windowing-TVF workaround`

---

### T4: `scripts/validate.py` aggregator + `make validate`

**What**: `main() -> int` - calls T1's `validate_yaml_contracts` against `interface/sources/` and
T3's `validate_sql_contracts` against `interface/analytics/`, prints every collected failure (file
context included, one per line) or a pass summary (count of files checked) when there are none,
and returns `0` on an empty combined failure list or `1` otherwise. Wired as
`if __name__ == "__main__": sys.exit(main())`. A new `validate` target is added to the `Makefile`
(alongside `test`/`neat`, added to `.PHONY`) invoking `python -m scripts.validate`. `scripts/`
gains an `__init__.py` so it is importable as a package, matching how `ingestion`/`flink` already
run via `python -m`.
**Where**: `scripts/validate.py`, `scripts/__init__.py` (new), `Makefile`
**Depends on**: T1, T3
**Reuses**: T1's `validate_yaml_contracts`, T3's `validate_sql_contracts`; the `Makefile`'s existing
`test`/`neat` target shape (`@echo` banner line, one recipe line)
**Requirement**: IVL-12, IVL-13, IVL-14, IVL-15, IVL-16

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] `make validate` exists and runs both checks across all of `interface/`
- [ ] A clean `interface/` tree exits `0` and prints a summary (files checked, 0 failures)
- [ ] A broken YAML file and a broken `.sql` file introduced simultaneously both appear in the same
      run's failure list, and the process exits non-zero
- [ ] Validation code lives entirely under `./scripts/` - no new files under `ingestion/`, `flink/`,
      or `tests/` outside the test files themselves
- [ ] `make validate` run against the repository's current, unmodified `interface/` tree exits `0`
- [ ] Gate check passes: `python -m pytest tests/scripts/test_validate.py`
- [ ] Full gate passes: `make test && make validate`
- [ ] Test count recorded

**Tests**: unit
**Gate**: full

**Commit**: `feat(scripts): add validate.py aggregator and wire make validate`

---

### T5: Fix `Makefile`'s `test` target exit code

**What**: Remove the leading `-` from `Makefile:26`'s pytest recipe line, so `make test`'s own exit
code reflects `pytest`'s real pass/fail state instead of always being `0`.
**Where**: `Makefile`
**Depends on**: None (independent fix, sequenced here since P4/T6 needs it)
**Reuses**: n/a (one-character deletion)
**Requirement**: IVL-23

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] `Makefile:26`'s recipe line no longer starts with `-`
- [ ] Smoke-checked: a deliberately broken test (temporarily, reverted after) makes `make test` exit
      non-zero; reverting the break makes it exit `0` again
- [ ] `make test` against the current, passing suite still exits `0` (no regression)

**Tests**: none
**Gate**: build

**Commit**: `fix(makefile): propagate pytest's real exit code from make test`

---

### T6: GitHub Actions CI workflow

**What**: `.github/workflows/ci.yml` - one job, triggered on `pull_request` targeting `main` and on
`push` to `main`; steps: checkout, `actions/setup-python@v5` (Python `3.12`), `pip install -r
requirements/dev.txt`, `make test`, `make validate`. `make test`'s step and `make validate`'s step
run as separate steps (not chained with `&&`) so one failing does not hide the other's result in the
run's step list.
**Where**: `.github/workflows/ci.yml`
**Depends on**: T4, T5
**Reuses**: `requirements/dev.txt` (already aggregates `ingestion.txt`+`flink.txt`+dev tools
including `pytest`/`sqlfluff`); the `Makefile`'s `test`/`validate` targets as-is
**Requirement**: IVL-17, IVL-18, IVL-19, IVL-20, IVL-21, IVL-22

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] Workflow triggers on a pull request targeting `main` and runs both `make test` and
      `make validate` as separate, visible steps
- [ ] Workflow triggers on a push to `main` and runs both
- [ ] Dependencies install from `requirements/dev.txt` on a Python `3.12` runner before either
      command runs
- [ ] Pushed to a real branch and opened as a pull request against `main` on this repository - both
      steps appear and pass on a clean branch (per spec's Independent Test)
- [ ] A deliberately broken branch (one failing test, pushed then reverted) shows the `make test`
      step - and only that one - failing in the same PR's run

**Tests**: none
**Gate**: build

**Commit**: `ci: add GitHub Actions workflow running make test and make validate`

---

## Phase Execution Map

```
Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5

Phase 1:  T1
Phase 2:  T2 ------→ T3
Phase 3:  T1 ------→ T4
Phase 3:  T3 ------→ T4
Phase 5:  T4 ------→ T6
Phase 5:  T5 ------→ T6
```

Execution is strictly sequential. 6 tasks total - single batch, no sub-agent dispatch (fits well
under the ~7-task budget).

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: YAML contract validator | 1 file, 1 function | ✅ Granular |
| T2: TVF mask + shape check | 1 file, 1 function | ✅ Granular |
| T3: `sqlfluff` integration | 1 file, 1 function | ✅ Granular |
| T4: Aggregator + `make validate` | 2 new files + 1 Makefile edit, 1 cohesive concern (wiring) | ✅ Granular |
| T5: Makefile exit-code fix | 1 file, 1 line | ✅ Granular |
| T6: CI workflow | 1 file | ✅ Granular |

## Diagram-Definition Cross-Check

| Task | Depends On (task body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | None | ✅ Match |
| T2 | None | None | ✅ Match |
| T3 | T2 | T2 → T3 | ✅ Match |
| T4 | T1, T3 | T1 → T4, T3 → T4 | ✅ Match |
| T5 | None | None | ✅ Match |
| T6 | T4, T5 | T4 → T6, T5 → T6 | ✅ Match |

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1: YAML validator | YAML contract validator | unit | unit | ✅ OK |
| T2: TVF mask + shape check | Windowing-TVF mask + shape check | unit | unit | ✅ OK |
| T3: `sqlfluff` integration | SQL contract validator | unit | unit | ✅ OK |
| T4: Aggregator + `make validate` | Aggregator entrypoint | unit | unit | ✅ OK |
| T5: Makefile fix | `Makefile`'s `test` target | none | none | ✅ OK |
| T6: CI workflow | CI workflow | none | none | ✅ OK |
