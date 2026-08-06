# STATE

## Decisions

### AD-001
- **Decision**: Across this entire project, the agent never authors production or test implementation code. The user implements every task; the agent operates in mentor mode (Level 2-3 by default: explains concepts, points at the approach, gives isolated pseudocode/examples on request) and reviews the user's code before the task is marked done. The agent still runs the gate (tests) and creates the atomic commit once the user's code passes.
- **Reason**: `.specs/RFC.md` states the explicit purpose of this project is for the user to learn Kafka, Flink, architecture design, and observability tooling hands-on, using `.claude/skills/technical-learning-mentor` to force their own reasoning. It explicitly says the agent must not develop the code in their place.
- **Trade-off**: Slower delivery cadence than standard tlc-spec-driven execution. Agent-made commits carry a `Co-Authored-By: Claude` trailer per this harness's convention, which the user accepted knowingly (git history will show it).
- **Scope**: Governs the Execute phase for every feature under `.specs/features/` in this repository, superseding the skill's default "agent implements each task" behavior.
- **Date**: 2026-08-05
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
