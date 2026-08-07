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

- **Feature**: github-ingestion
- **Phase / Task**: Tasks approved (5 tasks, `validate_tasks.py` clean) - next is Execute, starting at T1
- **Completed**: none committed yet
- **In-progress** (file:line): `ingestion/app.py` - user has an uncommitted draft of T1 (try/except + doubling backoff around the poll loop); reviewed inline, needs: backoff cap, module-scoped logger (not bare `logging`), `.exception()` instead of `.error(f"...")` to keep the traceback
- **Next step**: User implements T1 per the review notes in `tasks.md`, then the agent runs the gate (`python3 -m pytest tests/ingestion/test_app.py -q`) and commits
- **Blockers**: none
- **Uncommitted files**: `.specs/STATE.md`, `.specs/features/github-ingestion/spec.md`, `.specs/features/github-ingestion/tasks.md`, `ingestion/app.py` (user's in-progress T1 draft)
- **Branch**: feat/initial_steps
