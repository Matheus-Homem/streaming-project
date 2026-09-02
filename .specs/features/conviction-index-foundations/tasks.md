# Conviction Index Foundations Tasks

## Execution Protocol (MANDATORY -- do not skip)

Implement these tasks with the `tlc-spec-driven` skill: **activate it by name and follow its
Execute flow and Critical Rules.** Do not search for skill files by filesystem path. The skill is
the source of truth for the full flow (per-task cycle, sub-agent delegation, adequacy review,
Verifier, discrimination sensor).

**If the skill cannot be activated, STOP and tell the user - do not proceed without it.**

**Per-task authorship** (`AD-008`, derived and confirmed in-session 2026-08-28 - no
`.mentor/features/conviction-index-foundations/map.md` exists yet): T1/T2 are `own` (the user
writes the production code and its tests; the agent explains/reviews, never authors). T3/T4 are
`deliver` (the agent writes, the user reviews).

**Scope**: This file covers **P1 only**. P2-P6 are `spec.md`'s remaining stories, each getting its
own Design → Tasks pass when picked up, in the order `spec.md` fixes.

---

**Design**: `.specs/features/conviction-index-foundations/design.md`
**Status**: Approved

---

## Test Coverage Matrix

> Generated from codebase sampling (`tests/flink/normalization/`) and `Makefile`. No dedicated
> testing-guidelines file found in the repo (`AGENTS.md` absent) - coverage expectations below
> follow the strong default plus the existing test suite's own depth as a floor.

| Code Layer | Required Test Type | Coverage Expectation | Location Pattern | Run Command |
| --- | --- | --- | --- | --- |
| Domain model (`FieldRule.type` grammar) | unit | Every scalar token accepted; `ARRAY<T>` accepted; invalid token rejected; `as:`/`expression:` rejected; `model_fields_set` reflects default-declared-vs-absent | `tests/flink/normalization/test_models.py` | `python -m pytest tests/flink/normalization/test_models.py` |
| Domain evaluator (`_compile_rule`) | unit | 1:1 with P1 AC3/AC4/AC6/AC7 | `tests/flink/normalization/domain/test_evaluator.py` | `python -m pytest tests/flink/normalization/domain/test_evaluator.py` |
| Config contract (`github.yml`) | integration (pre-existing) | Identical `NormalizedEvent` output pre/post migration (P1 AC8) - no new test written, existing suite re-run | `tests/flink/normalization/test_contracts_github.py` | `python -m pytest tests/flink/normalization/test_contracts_github.py` |
| Rule doc (`.claude/rules/flink-contract-dsl.md`) | none | Doc-only | - | build gate only |

## Gate Check Commands

| Gate Level | When to Use | Command |
| --- | --- | --- |
| Quick | After T1, T2 | `python -m pytest -p no:cacheprovider -s -vv --log-cli-level=INFO tests/flink/normalization` |
| Full | After T3, T4 (closes the feature's P1 slice) | `make test` |

---

## Execution Plan

Phases are ordered and run sequentially - each phase completes before the next begins.

### Phase 1: Contract type grammar

```
T1
```

### Phase 2: Compiler update

```
T2
```

### Phase 3: Migration & docs

```
T3 → T4
```

---

## Task Breakdown

### T1: Add `type:` grammar to `FieldRule`, remove `as_`

**What**: `FieldRule` gains a required `type: str` field validated against the grammar
(`SCALAR | ARRAY<...>`); `as_`/`alias="as"` and `expression` are removed from the model.
**Where**: `flink/normalization/models.py`
**Depends on**: None
**Reuses**: `FROM_PATTERN`'s `field_validator` + `ValueError`-on-mismatch shape
**Requirement**: CVF-01, CVF-02, CVF-05, CVF-07
**Authorship**: `own` - user writes, agent reviews (no production code authored by the agent)

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] `type:` is required (no default) on `FieldRule`
- [ ] Every scalar token (`STRING`, `BOOLEAN`, `PRESENCE`, `TIMESTAMP`, `BIGINT`, `INT`, `DOUBLE`)
      validates
- [ ] `ARRAY<T>` (scalar `T`) validates
- [ ] An invalid token raises a `ValidationError` naming that token, not a generic parse failure
- [ ] A contract declaring `as:` is rejected (`extra="forbid"` on the unknown key)
- [ ] A contract declaring `expression:` is rejected (`extra="forbid"` on the unknown key)
- [ ] `FieldRule(...).model_fields_set` correctly distinguishes an explicitly-declared
      `default: null` from an absent `default`
- [ ] Gate check passes: `python -m pytest tests/flink/normalization/test_models.py`
- [ ] Test count recorded (no silent deletions from the pre-existing `TestFieldRule`/
      `TestNormalizationContract` cases)

**Tests**: unit
**Gate**: quick

---

### T2: `_compile_rule` compiles from `type:` instead of `as_`

**What**: The `match rule:` block's `as_="boolean"`/`as_="timestamp"` cases become
`type="PRESENCE"`/`type="TIMESTAMP"`; the `default` composition check moves from
`rule.default is None` to `"default" not in rule.model_fields_set`.
**Where**: `flink/normalization/domain/evaluator.py`
**Depends on**: T1
**Reuses**: The existing `_compile_rule` method body - the two `case` arms and the `default` check
line change, and the leading `if rule.expression: return rule.expression` branch is deleted;
`case FieldRule(take=take)` and `case _:` are untouched
**Requirement**: CVF-03, CVF-04, CVF-06
**Authorship**: `own` - user writes, agent reviews

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] `type: PRESENCE` compiles to the same expression `as: boolean` produced
      (`{from_} != \`null\``)
- [ ] `type: TIMESTAMP` compiles to the same expression `as: timestamp` produced
      (`iso_to_millis({from_})`)
- [ ] `type: BOOLEAN` (native boolean, not `PRESENCE`) passes the resolved value through
      unchanged - no presence coercion
- [ ] A field with no `default:` declared and one with `default: null` explicitly declared both
      compile correctly, and the evaluator's `default` branch is driven by `model_fields_set`, not
      a value check
- [ ] The `if rule.expression: return rule.expression` branch is gone - `_compile_rule` no longer
      references `rule.expression`
- [ ] Gate check passes: `python -m pytest tests/flink/normalization/domain/test_evaluator.py`
- [ ] Test count recorded (pre-existing `TestCompileRule` cases exercising `expression:` are
      intentionally removed, not silently dropped - noted in the commit, not just deleted)

**Tests**: unit
**Gate**: quick

---

### T3: Migrate `github.yml` to the `type:` vocabulary

**What**: Every field rule in `interface/sources/github/normalization.yml` gains an explicit
`type:`; all `as: timestamp` become `type: TIMESTAMP`, the one `as: boolean` becomes
`type: PRESENCE`, every other field gets its matching scalar/`ARRAY<...>` type.
**Where**: `interface/sources/github/normalization.yml`
**Depends on**: T1, T2
**Reuses**: n/a (config file)
**Requirement**: CVF-08
**Authorship**: `deliver` - agent writes, user reviews

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] Every field rule in every block (`partition_key`, `envelope`, `common`, every `event_types`
      entry) declares `type:`
- [ ] No `as:` key remains anywhere in the file
- [ ] `tests/flink/normalization/test_contracts_github.py` passes unmodified against the migrated
      contract - identical `NormalizedEvent` output for every event type fixture
- [ ] Gate check passes: `python -m pytest tests/flink/normalization/test_contracts_github.py`

**Tests**: integration (pre-existing suite, no new test written)
**Gate**: full

**Commit**: `feat(normalization): migrate github.yml to explicit type: vocabulary`

---

### T4: Update `.claude/rules/flink-contract-dsl.md`'s `default` guidance

**What**: The rule "always use `is not None` to check whether a `default` was declared" is replaced
with the `model_fields_set`-based convention T2 actually implements.
**Where**: `.claude/rules/flink-contract-dsl.md`
**Depends on**: T2, T3 (sequenced after T3 within Phase 3 - no data dependency on T3's output, but
both land in the same closing phase)
**Reuses**: n/a (doc file)
**Requirement**: N/A - housekeeping, not spec-mapped (a rule-file correction, not a CVF acceptance
criterion)
**Authorship**: `deliver` - agent writes, user reviews

**Tools**:

- MCP: NONE
- Skill: NONE

**Done when**:

- [ ] The rule file's `default` bullet reflects `model_fields_set` as the current convention,
      explains why (distinguishes "no default" from "explicit `default: null`", which `is not None`
      cannot do)
- [ ] Gate check passes: `make test` (full suite, confirms nothing broke by the doc-only change)

**Tests**: none
**Gate**: full

**Commit**: `docs(rules): update default-check convention to model_fields_set`

---

## Phase Execution Map

```
Phase 1 → Phase 2 → Phase 3

Phase 1:  T1 ------→ T2
Phase 2:  T1 ------→ T3
Phase 2:  T2 ------→ T3
Phase 3:  T2 ------→ T4
Phase 3:  T3 ------→ T4
```

Execution is strictly sequential. 4 tasks total - single batch, no sub-agent dispatch (fits well
under the ~7-task budget).

---

## Task Granularity Check

| Task | Scope | Status |
| --- | --- | --- |
| T1: Add `type:` grammar to `FieldRule` | 1 file, 1 concept | ✅ Granular |
| T2: `_compile_rule` uses `type:` | 1 file, 1 method | ✅ Granular |
| T3: Migrate `github.yml` | 1 file | ✅ Granular |
| T4: Update rule doc | 1 file | ✅ Granular |

## Diagram-Definition Cross-Check

| Task | Depends On (task body) | Diagram Shows | Status |
| --- | --- | --- | --- |
| T1 | None | None | ✅ Match |
| T2 | T1 | T1 → T2 (via phase order) | ✅ Match |
| T3 | T1, T2 | T1,T2 precede Phase 3 | ✅ Match |
| T4 | T2, T3 | T2 → T4, T3 → T4 | ✅ Match |

## Test Co-location Validation

| Task | Code Layer Created/Modified | Matrix Requires | Task Says | Status |
| --- | --- | --- | --- | --- |
| T1: `type:` grammar | Domain model | unit | unit | ✅ OK |
| T2: `_compile_rule` | Domain evaluator | unit | unit | ✅ OK |
| T3: `github.yml` migration | Config contract | integration (pre-existing) | integration | ✅ OK |
| T4: rule doc update | Doc | none | none | ✅ OK |
