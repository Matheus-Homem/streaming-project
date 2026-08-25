# Feature: interface-layout

<!-- Written by /mentor-map. Rewritten on every remap.
     The Task x Knowledge crossing for this feature, and the verdict for each task.
     See references/task-matrix.md. -->

- Opened: 2026-08-25
- Closed:
- Spec source: `.specs/features/interface-layout/tasks.md`
- Snapshot: 2026-08-21T09:57:09Z (4 days old, within the 14-day budget - not stale). Snapshot's own
  `nodes` array is empty (no Comprehension data recorded for anything yet) - Comprehension resolves
  `unknown` for every node in this project regardless of feature.
- Task key used: explicit id (T1-T11, as written in `tasks.md`)

## Tasks

<!-- verdict: own | paired | delegated
     deciding node: the node that produced the verdict, per the aggregation rule
     active nodes: how many survived the `waived` filter, out of how many required
     flags: contested | unverified | class-first | manual-override | empty -->

| task | verdict | deciding node | active nodes | flags |
|---|---|---|---|---|
| T1 - split `ingestion/config/sources.yml` into per-source files | delegated | - | 0/0 | |
| T2 - rework the ingestion loader for per-source files | delegated | - | 0/0 | |
| T3 - remove `ingestion/config/sources.yml` | delegated | - | 0/0 | |
| T4 - move the normalization contract to the source directory | delegated | - | 0/0 | |
| T5 - repoint the normalization contract repository | delegated | - | 0/0 | |
| T6 - rename `flink/aggregation/` to `flink/analytics/` | delegated | - | 0/0 | |
| T7 - update `flink/analytics/app.py`'s query path and env var | delegated | - | 0/0 | |
| T8 - update `flink/analytics/Dockerfile` | delegated | - | 0/0 | |
| T9 - add `COPY interface/` to the other two Dockerfiles | delegated | - | 0/0 | |
| T10 - wire the rename into `docker-compose.yml` | delegated | - | 0/0 | |
| T11 - live verification | delegated | - | 0/0 | |

## Knowledge

<!-- Every node this feature requires, with its resolved triple.
     Each resolution names its origin — a declaration, a date, a derivation.
     A value without its origin is not written. -->

| node | domain (origin) | comprehension (date) | application (source) | tasks |
|---|---|---|---|---|

(none - see Notes)

## Gaps

<!-- Required nodes absent from the Gemini Notebook snapshot, or present with
     comprehension `no`. This is the study list — what to take to Gemini Notebook. -->

| node | why it is required | tasks affected |
|---|---|---|

(none)

## Trace

<!-- The full resolution behind every verdict. Required — a verdict without its
     trace is not written. -->

T1-T11  delegated
    ← required = {} (no depth-4 node derived for any task)
    active = [] (empty set)
    per the aggregate rule: "A task requiring nothing... is delegated. That is a normal and
    healthy outcome, not a failure of the derivation." (`references/task-matrix.md`)

## Notes

- 2026-08-25: **Zero new knowledge nodes derived for this feature.** Every one of the 11 tasks was
  tested against "what would someone have to command in order to write this themselves?"
  (`references/task-matrix.md` Step 1) and none resolved to a depth-4 competency worth tracking:
  - T1, T3, T4, T6: file/directory relocation, no logic
  - T2, T5: re-implementing a loader by copying an existing, already-working pattern in the same
    repo (`YamlContractRepository._load`, cited verbatim in each task's `Reuses` field) - applying
    an already-owned pattern, not developing a new one
  - T7: env var rename + `Path(__file__).parent.parent.parent` arithmetic, the same relative-path
    technique `flink/normalization/app.py` already uses one level shallower - not a new class of
    artifact
  - T8, T9, T10: one-line Dockerfile/compose renames
  - T11: live verification, re-running an already-established procedure (`flink-aggregation` T6)
    against unchanged behavior
  This matches `spec.md`'s own framing of the feature ("mechanical, no behavior change") and the
  plan's choice to skip Design entirely for the same reason.
- **Scope-gate disclosure** (`.claude/rules/interaction-protocol.md`, "Scope gate on task
  generation"): this task list comes out **entirely `delegated`**. Per that rule, a regenerated
  list that is 100% delegated is a delivery change, not a learning one, and must be said out loud
  before the map is treated as final - done here, in the chat turn that produced this file, not
  silently.
