# STATE

## Decisions

### AD-001
- **Decision**: Across this entire project, the agent never authors production or test implementation code. The user implements every task; the agent operates in mentor mode (Level 2-3 by default: explains concepts, points at the approach, gives isolated pseudocode/examples on request) and reviews the user's code before the task is marked done. The agent still runs the gate (tests) and creates the atomic commit once the user's code passes.
- **Reason**: `.specs/RFC.md` states the explicit purpose of this project is for the user to learn Kafka, Flink, architecture design, and observability tooling hands-on, using `.claude/skills/technical-learning-mentor` to force their own reasoning. It explicitly says the agent must not develop the code in their place.
- **Trade-off**: Slower delivery cadence than standard tlc-spec-driven execution. Agent-made commits carry a `Co-Authored-By: Claude` trailer per this harness's convention, which the user accepted knowingly (git history will show it).
- **Scope**: Governs the Execute phase for every feature under `.specs/features/` in this repository, superseding the skill's default "agent implements each task" behavior. **Amended by AD-002** for the unit-test-authoring exception.
- **Date**: 2026-08-05
- **Status**: active

### AD-002
- **Decision**: `/mentor-help` may author unit test code directly (Level 5), but only when the class/module under test is already functional - its production implementation is complete and working, and only test coverage is missing or broken. It does not extend to production code under any circumstance, and does not apply while the class itself is still mid-implementation as part of an in-progress task (mentor mode still governs that case even under `/mentor-help`).
- **Reason**: User request - writing exhaustive unit tests for code that already works and was already designed/reasoned through by the user has lower learning value than the design/implementation work itself; `/mentor-help` should be able to shortcut that specific, lower-value step.
- **Trade-off**: Narrows AD-001's blanket "never authors test code" rule. The boundary condition ("is the class already functional?") requires judgment call each time - if ambiguous, the agent should ask rather than assume the exception applies.
- **Scope**: Amends AD-001. Governs `/mentor-help` specifically (`.claude/commands/mentor-help.md`); does not change default mentor-mode behavior for `/mentor-debug`, `/mentor-code-review`, or unassisted requests.
- **Date**: 2026-08-07
- **Status**: active

## Handoff

- **Feature**: streaming-ingestion (renamed from `github-ingestion` - directory and doc references updated 2026-08-10; the git branch itself, `feat/github-ingestion-resilience`, was NOT renamed since it's pushed to `origin` and a rename there is a remote operation outside this session's authorization)
- **Phase / Task**: Phase 1 [S1] (T1, T2) - T1 done and committed; T2 (rate-limit detection in the client) not started per `tasks.md`. **Needs reconciliation before trusting this**: `git log` shows commits after the last `tasks.md` review (`BoundedUniqueTracker` in `ingestion/utils/tracker.py`, wired into `ingestion/use_case.py`/`app.py`, plus an `IngestionProducer`/`JsonSerializer` refactor) that look related to Phase 2's T3/T4 (dedup tracker + wiring) but under different file/class names than the task bodies describe (`ingestion/dedup.py` → became `ingestion/utils/tracker.py`+`BoundedUniqueTracker`). Not verified against T3/T4's `Done when` criteria - do that before marking them complete.
- **Completed**: T1, T5 (both confirmed via existing tests/gate, checkboxes updated in `tasks.md`)
- **In-progress** (file:line): `tests/ingestion/utils/test_tracker.py` - untracked, user is writing unit tests for `BoundedUniqueTracker` (production code already functional per git log - falls under `AD-002`'s `/mentor-help` exception if invoked)
- **Next step**: Reconcile T2/T3/T4 status against the `BoundedUniqueTracker`/`IngestionProducer` work already on the branch (map it to the P2 tasks it satisfies, or determine it's a different concern), then continue Execute from whichever task is actually next
- **Blockers**: none
- **Uncommitted files**: `.specs/STATE.md`, `.specs/features/streaming-ingestion/tasks.md` (this refactor), `tests/ingestion/utils/test_tracker.py` (untracked, in-progress)
- **Branch**: feat/github-ingestion-resilience (tracks `origin/feat/github-ingestion-resilience`)
