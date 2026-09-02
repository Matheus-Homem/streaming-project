# Interface Validation Specification

## Problem Statement

`interface/` is the platform's contract-as-API surface (`AD-006`) - the space a platform user edits,
not internal implementation. Today, correctness of what's under `interface/` is only ever checked as
a side effect of `make test` (e.g. `tests/flink/normalization/test_contracts_github.py` used to load
the real `interface/sources/github/normalization.yml`, until that coupling was found and removed
this session). There is no dedicated way to check whether a file under `interface/` is well-formed
without either running the full test suite or standing up the real pipeline. `interface/analytics/`'s
SQL files have no check at all today - a syntax error there is only caught by manually running the
Flink job.

## Goals

- [ ] A `make validate` target exists that checks every file under `interface/` independently of
      `make test`
- [ ] Every `interface/sources/<name>/{ingestion,normalization}.yml` that exists loads and validates
      against its existing Pydantic model, for every source directory - wired or not
- [ ] Every `interface/analytics/*.sql` file is checked for syntax errors, including the
      Flink-specific windowing table-valued function syntax that no off-the-shelf tool parses today
- [ ] `make validate` reports every failure found across every file, with file/line context, and
      exits non-zero on any failure
- [ ] A CI/CD pipeline runs `make test` and `make validate` automatically on every pull request
      targeting `main` and on every push to `main`

## Out of Scope

| Feature | Reason |
| --- | --- |
| Executing SQL against a real/embedded Flink environment (PyFlink `execute_sql`/`explain_sql`) | User decision this session - a real `INSERT INTO ... SELECT` submits a streaming job and would try to reach Kafka; unsafe/unreliable in a CI check with no infra running |
| Cross-file consistency between a source's `ingestion.yml` and `normalization.yml` (e.g. every `event_types` key in the contract being something `ingestion.yml` could plausibly produce) | User decision this session - only the `source:`-matches-directory-name check was selected; deeper cross-file semantic checks are a bigger, separate capability |
| Configuring GitHub's branch-protection rule that makes the new CI checks *required* before merge | A repository-settings change (GitHub UI/API), not a file this feature commits - out of the "local implementation and local commits" blast radius a spec/tasks approval authorizes. The workflow producing the checks is in scope (P4); making them mandatory is a manual follow-up for the user |
| Validating `interface/analytics/*.sql` files' *semantics* (does the query compute the right thing) | Only syntax/shape is checked - correctness of the aggregation logic itself is out of a syntax validator's job |
| A general-purpose Flink SQL parser or sqlfluff dialect contribution upstream | The windowing-TVF gap is worked around locally (mask + shape-check), not fixed at the library level - `sqlfluff/sqlfluff#6522` tracks the real fix upstream |

---

## Assumptions & Open Questions

| Assumption / decision | Chosen default | Rationale | Confirmed? |
| --- | --- | --- | --- |
| Scope of `interface/` covered | `interface/sources/<name>/ingestion.yml`, `interface/sources/<name>/normalization.yml`, `interface/analytics/*.sql` - all three artifact types | User decision - "tudo que irá ser criado, tanto os yamls quanto sql" | y |
| YAML "correct platform feature usage" check beyond Pydantic | Only one extra check: a `normalization.yml`'s `source:` value matches its parent directory name | User decision - declined the broader ingestion/normalization cross-consistency option | y |
| Unwired sources included | Yes - every directory under `interface/sources/` is validated regardless of whether a real client/normalizer adapter exists (e.g. `gitlab`, config-only per `AD-004`) | User decision | y |
| SQL validation mechanism | `sqlfluff parse --dialect flink` (already a dev dependency, `sqlfluff==4.3.0`) for everything except Flink's windowing table-valued functions (`TUMBLE`/`HOP`/`CUMULATE`/`SESSION`) - those spans are masked out before the `sqlfluff` pass and separately checked against Flink's documented `TVF(TABLE data, DESCRIPTOR(timecol), size[, offset])` grammar via a dedicated shape check | Researched this session (Knowledge Verification Chain step 3/4): confirmed live against `interface/analytics/repo_counts_5m.sql` that `sqlfluff`'s `flink` dialect does not parse windowing TVF syntax (`sqlfluff/sqlfluff#6522`, open, unresolved) - not a config mistake, a real upstream gap. A masked-run + separate shape-check prototype was built and verified against both the real file (passes) and two injected-fault fixtures (a real syntax typo outside the TVF, and a malformed TVF shape) - both correctly fail | y |
| PyFlink-based SQL validation | Rejected | User decision - real job submission, unsafe without live Kafka | y |
| Scripts location | `./scripts/` (new top-level directory) | User decision - explicit, not the existing per-domain `ingestion/`/`flink/` trees | y |
| Failure reporting | Every failure across every file is reported in one run (not stop-at-first); non-zero exit on any failure | Standard CI-check convention, matches `make test`'s own pytest reporting shape | n |
| CI platform | GitHub Actions (`.github/workflows/`) | The repo's remote is `github.com/Matheus-Homem/streaming-project` - no other CI platform is configured or referenced anywhere in the project | n |
| CI trigger events | `pull_request` targeting `main`, and `push` to `main` | Standard convention for a single-main-branch repo (matches this project's own branching pattern - feature branches PR into `main`) | n |
| CI job shape | One job, two steps (`make test` then `make validate`), installing `requirements/dev.txt` (already aggregates `ingestion.txt`+`flink.txt`+dev tools including `pytest`/`sqlfluff`) | Mechanical - matches the Makefile's own existing commands, no new install surface needed | n |

**Open questions:** none - all resolved above.

---

## User Stories

### P1: YAML contract files validate independently of `make test` ⭐ MVP

**User Story**: As a platform developer, I want every `interface/sources/<name>/{ingestion,normalization}.yml`
to be checkable on its own, so that a config mistake is caught by a dedicated command instead of by
`make test` accidentally exercising the real file.

**Why P1**: This is the exact gap found this session - the smallest independently useful slice, and
what unblocks removing any future accidental `make test` ↔ `interface/` coupling for good.

**Acceptance Criteria**:

1. WHEN `make validate` runs THEN the system SHALL attempt to load and validate every
   `interface/sources/<name>/ingestion.yml` that exists against `SourceYamlEntry`.
2. WHEN `make validate` runs THEN the system SHALL attempt to load and validate every
   `interface/sources/<name>/normalization.yml` that exists against `NormalizationContract`.
3. IF a source directory contains only one of `ingestion.yml`/`normalization.yml` THEN the system
   SHALL validate the file that exists and SHALL NOT treat the absent one as a failure (a source may
   legitimately have only one today, e.g. `gitlab`).
4. IF a `normalization.yml`'s top-level `source:` value does not equal its parent directory's name
   THEN the system SHALL report a failure naming the file and both values.
5. IF a YAML file fails its Pydantic model's validation (unknown key, missing required field,
   malformed `type:` grammar, etc.) THEN the system SHALL report the file path and the underlying
   `ValidationError` message, not swallow or summarize it away.
6. The system SHALL validate every directory under `interface/sources/`, independent of whether a
   real client/normalizer adapter is wired for that source.

**Independent Test**: Point the validator at `interface/sources/` as it exists today (`github`
fully populated, `gitlab` ingestion-only) and confirm it reports 0 failures. Introduce a
hand-crafted broken copy (an unknown key, a `source:` mismatch, a missing `type:`) in a scratch
directory and confirm each is reported with file path and message.

---

### P2: `interface/analytics/*.sql` files are syntax-checked, including windowing TVFs

**User Story**: As a platform developer, I want a syntax error in an analytics `.sql` file caught
before it reaches a running Flink job, including in Flink-specific windowing syntax that generic SQL
tools don't understand.

**Why P2**: Independent of P1 - a different artifact type and a genuinely harder problem (researched
this session; no existing tool solves it fully). Sequenced after P1 since P1 is the smaller, already
end-to-end-verified slice.

**Acceptance Criteria**:

1. WHEN `make validate` runs THEN the system SHALL run a syntax check against every `.sql` file
   under `interface/analytics/`.
2. WHEN a `.sql` file contains one or more windowing table-valued function calls (`TABLE(TUMBLE(...))`,
   `HOP`, `CUMULATE`, `SESSION`) THEN the system SHALL mask each call out of the text before running
   `sqlfluff`, and SHALL instead validate each masked call's shape against Flink's documented
   `TVF(TABLE data, DESCRIPTOR(timecol), size[, offset])` grammar via a dedicated check - `sqlfluff`
   does not parse this syntax today (`sqlfluff/sqlfluff#6522`).
3. WHEN the masked SQL text is run through `sqlfluff parse --dialect flink` THEN any resulting parse
   violation SHALL be reported with the file path, line, and position `sqlfluff` itself provides.
4. IF a windowing TVF call's shape does not match the documented grammar (missing `DESCRIPTOR(...)`,
   missing/malformed `INTERVAL '<n>' <unit>` size, wrong argument order) THEN the system SHALL report
   a failure naming the file and the offending call text.
5. IF an `interface/analytics/*.sql` file is syntactically valid (including its windowing TVF calls,
   if any) THEN the system SHALL report it as passing.

**Independent Test**: Run against `interface/analytics/repo_counts_5m.sql` as it exists today and
confirm it passes (0 failures) - this is the file the P2 research prototype was already verified
against. Run against two hand-crafted fixtures: one with a real syntax error outside any TVF call
(confirm it fails, citing `sqlfluff`'s line/position), one with a malformed TVF shape (confirm it
fails, citing the shape check) - both fixture behaviors were already prototyped and verified this
session.

---

### P3: `make validate` ships as one command covering all of `interface/`

**User Story**: As a platform developer, I want one command that runs every check above across the
whole `interface/` tree, so a future CI/CD pipeline (or a local pre-PR habit) has a single thing to
invoke.

**Why P3**: Depends on P1 and P2 - it is the wiring that makes them one command, not new checking
logic of its own.

**Acceptance Criteria**:

1. The system SHALL provide a `make validate` target in the project `Makefile`, following the
   existing `make test`/`make neat` target shape.
2. WHEN `make validate` runs THEN the system SHALL invoke the P1 and P2 checks across every file
   under `interface/` and aggregate their results into one report.
3. WHEN every checked file passes THEN `make validate` SHALL exit `0` and print a summary (count of
   files checked, 0 failures).
4. IF any file fails any check THEN `make validate` SHALL exit non-zero and print every failure found
   in the run - not stop at the first failure.
5. The validation code SHALL live under `./scripts/` (a new top-level directory), not inside
   `ingestion/`, `flink/`, or `tests/`.

**Independent Test**: Run `make validate` against the repository as it exists today and confirm exit
code `0`. Introduce one broken YAML file and one broken `.sql` file simultaneously and confirm
`make validate` exits non-zero and reports both failures in the same run.

---

### P4: CI/CD pipeline runs `make test` and `make validate` automatically

**User Story**: As a platform developer, I want `make test` and `make validate` to run automatically
on every pull request and every push to `main`, so a broken developer change or a broken `interface/`
config is caught before it reaches `main` without anyone remembering to run the commands by hand.

**Why P4**: Depends on P1-P3 existing (`make validate` has to exist before a pipeline can call it).
This is the automation layer the user named as the actual reason this feature exists.

**Acceptance Criteria**:

1. WHEN a pull request is opened or updated targeting `main` THEN the CI pipeline SHALL run
   `make test`.
2. WHEN a pull request is opened or updated targeting `main` THEN the CI pipeline SHALL run
   `make validate`.
3. WHEN a commit is pushed directly to `main` THEN the CI pipeline SHALL run both `make test` and
   `make validate`.
4. IF `make test` exits non-zero THEN the CI run SHALL be reported as failed, independent of
   `make validate`'s result.
5. IF `make validate` exits non-zero THEN the CI run SHALL be reported as failed, independent of
   `make test`'s result.
6. The CI pipeline SHALL install project dependencies from `requirements/dev.txt` before running
   either command, on a supported Python 3.12 runner (matching the project's own `.venv`).
7. The `Makefile`'s `test` target SHALL propagate `pytest`'s real exit code - the pre-existing
   `-` prefix on its recipe line (`Makefile:26`), which today makes `make test` always exit `0`
   regardless of test outcome, SHALL be removed. Without this, AC4 above cannot hold: CI would never
   detect a failing test.

**Independent Test**: Open a pull request against `main` on this repository and confirm the pipeline
run appears with both `make test` and `make validate` as visible steps, both passing on a clean
branch. Push a commit that breaks one of the two (e.g. an unused import `make neat` would have
caught, or a malformed `interface/` file) to a branch and confirm the corresponding step - and only
that one - fails.

---

## Edge Cases

- IF `interface/sources/` contains no subdirectories THEN `make validate` SHALL exit `0` and report
  that nothing was found, not crash (P1).
- IF `interface/analytics/` contains no `.sql` files THEN the SQL check SHALL be a no-op - not a
  failure (P2).
- IF a `.sql` file under `interface/analytics/` is empty THEN the system SHALL report it as passing
  trivially - there is nothing to fail (P2).
- IF a source directory under `interface/sources/` contains neither `ingestion.yml` nor
  `normalization.yml` (an empty directory) THEN the system SHALL skip it without failing (P1).
- IF `make test` is run directly (outside CI) after the `Makefile` fix (IVL-23) and a test fails
  THEN the invoking shell SHALL see a non-zero exit code - this is a behavior change from today for
  any local/manual use of `make test`, not just CI (P4).

---

## Requirement Traceability

| Requirement ID | Story | Phase | Status |
| --- | --- | --- | --- |
| IVL-01 | P1: Validate every `ingestion.yml` that exists | Tasks | In Tasks |
| IVL-02 | P1: Validate every `normalization.yml` that exists | Tasks | In Tasks |
| IVL-03 | P1: Missing file (of the pair) is not a failure | Tasks | In Tasks |
| IVL-04 | P1: `source:` matches directory name | Tasks | In Tasks |
| IVL-05 | P1: Pydantic validation errors are reported, not swallowed | Tasks | In Tasks |
| IVL-06 | P1: Unwired sources are validated too | Tasks | In Tasks |
| IVL-07 | P2: Syntax check runs on every `.sql` under `interface/analytics/` | Tasks | In Tasks |
| IVL-08 | P2: Windowing TVF calls are masked and shape-checked separately | Tasks | In Tasks |
| IVL-09 | P2: Non-TVF parse violations are reported with sqlfluff's own location | Tasks | In Tasks |
| IVL-10 | P2: Malformed TVF shape is reported | Tasks | In Tasks |
| IVL-11 | P2: A syntactically valid `.sql` file passes | Tasks | In Tasks |
| IVL-12 | P3: `make validate` target exists | Tasks | In Tasks |
| IVL-13 | P3: P1+P2 checks run across all of `interface/`, aggregated | Tasks | In Tasks |
| IVL-14 | P3: All-pass exits 0 with a summary | Tasks | In Tasks |
| IVL-15 | P3: Any failure exits non-zero, reports every failure | Tasks | In Tasks |
| IVL-16 | P3: Validation code lives under `./scripts/` | Tasks | In Tasks |
| IVL-17 | P4: CI runs `make test` on PRs targeting `main` | Tasks | In Tasks |
| IVL-18 | P4: CI runs `make validate` on PRs targeting `main` | Tasks | In Tasks |
| IVL-19 | P4: CI runs both on push to `main` | Tasks | In Tasks |
| IVL-20 | P4: `make test` failure fails the CI run independent of `make validate` | Tasks | In Tasks |
| IVL-21 | P4: `make validate` failure fails the CI run independent of `make test` | Tasks | In Tasks |
| IVL-22 | P4: CI installs `requirements/dev.txt` on a Python 3.12 runner | Tasks | In Tasks |
| IVL-23 | P4: `Makefile`'s `test` target propagates pytest's real exit code | Tasks | In Tasks |

**ID format:** `IVL-NN`

**Status values:** Pending → In Design → In Tasks → Implementing → Verified

**Coverage:** 23 total, 23 mapped to tasks, 0 unmapped

---

## Success Criteria

- [ ] `make validate` exits `0` against the repository's current, unmodified `interface/` tree
- [ ] A hand-crafted broken YAML (unknown key, `source:` mismatch) is caught and reported with file
      path and message
- [ ] A hand-crafted broken `.sql` file (a real syntax error outside any windowing TVF call) is
      caught and reported with `sqlfluff`'s file/line/position
- [ ] A hand-crafted malformed windowing TVF call is caught and reported by the shape check
- [ ] `make test`'s exit code reliably reflects `pytest`'s real pass/fail state
- [ ] A pull request against `main` on this repository shows both `make test` and `make validate`
      as pipeline steps, and a deliberately broken branch fails the corresponding step
- [ ] Every IVL requirement above reaches `Verified` in this table
