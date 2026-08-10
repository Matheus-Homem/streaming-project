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
- **Phase / Task**: All 5 tasks (T1-T5) across Phase 1/2/3 verified complete against `tasks.md`'s `Done when` checklists and re-checked in that file - 2026-08-10 `/checkpoint` reconciliation. Gate (`python3 -m pytest tests/ingestion -q`) passes with 63 tests. T4's dedup filtering was additionally confirmed empirically against real GitHub API traffic in `tmp/log` (a poll with 100% repeat events was fully skipped; the next poll's all-new events all published).
- **Completed**: T1, T2, T3, T4, T5 - all checkboxes updated in `tasks.md`
- **In-progress**: none
- **Next step**: The feature's code-level work is done. `spec.md`'s Success Criteria still has one open item: *"The service can run unattended for hours without crashing on a transient GitHub/Kafka failure (P2, pending)"* - left open by user decision (2026-08-10), not converted into a new task. **Live validation in progress**: a terminal window the user is watching hit `RateLimitError` at 2026-08-10 12:49:35 (`reset_at=2026-08-10 16:42:13+00:00`) - this is a real, unplanned occurrence of exactly the T1+T2 scenario (rate limit → backoff capped at 300s → no crash). User will check after the reset time whether the service resumes publishing normally; if so, that closes the open Success Criteria item with real evidence instead of a synthetic test. No commits made this session by user's explicit choice ("Eu decido quando commitar as alterações") - T2/T3/T4 code is functional and gate-passing but still uncommitted.
- **Blockers**: none
- **Uncommitted files**: `.specs/STATE.md`, `.specs/features/streaming-ingestion/tasks.md` (checkbox reconciliation), plus the user's already-uncommitted implementation diffs (`ingestion/app.py` tracker size/comment, `Makefile`, `tests/ingestion/test_use_case.py` new dedup test) - not yet committed as of this handoff
- **Branch**: feat/github-ingestion-resilience (tracks `origin/feat/github-ingestion-resilience`)
