# STATE

## Decisions

### AD-001
- **Decision**: Across this entire project, the agent never authors production or test implementation code. The user implements every task; the agent operates in mentor mode (Level 2-3 by default: explains concepts, points at the approach, gives isolated pseudocode/examples on request) and reviews the user's code before the task is marked done. The agent still runs the gate (tests) and creates the atomic commit once the user's code passes.
- **Reason**: `.specs/RFC.md` states the explicit purpose of this project is for the user to learn Kafka, Flink, architecture design, and observability tooling hands-on, using `.claude/skills/technical-learning-mentor` to force their own reasoning. It explicitly says the agent must not develop the code in their place.
- **Trade-off**: Slower delivery cadence than standard tlc-spec-driven execution. Agent-made commits carry a `Co-Authored-By: Claude` trailer per this harness's convention, which the user accepted knowingly (git history will show it).
- **Scope**: Governs the Execute phase for every feature under `.specs/features/` in this repository, superseding the skill's default "agent implements each task" behavior. **Amended by AD-002** for the unit-test-authoring exception, **amended by AD-003** (agent no longer creates the commit either - the "creates the atomic commit" clause above is superseded).
- **Date**: 2026-08-05
- **Status**: active

### AD-002
- **Decision**: `/mentor-help` may author unit test code directly (Level 5), but only when the class/module under test is already functional - its production implementation is complete and working, and only test coverage is missing or broken. It does not extend to production code under any circumstance, and does not apply while the class itself is still mid-implementation as part of an in-progress task (mentor mode still governs that case even under `/mentor-help`).
- **Reason**: User request - writing exhaustive unit tests for code that already works and was already designed/reasoned through by the user has lower learning value than the design/implementation work itself; `/mentor-help` should be able to shortcut that specific, lower-value step.
- **Trade-off**: Narrows AD-001's blanket "never authors test code" rule. The boundary condition ("is the class already functional?") requires judgment call each time - if ambiguous, the agent should ask rather than assume the exception applies.
- **Scope**: Amends AD-001. Governs `/mentor-help` specifically (`.claude/commands/mentor-help.md`); does not change default mentor-mode behavior for `/mentor-debug`, `/mentor-code-review`, or unassisted requests.
- **Date**: 2026-08-07
- **Status**: active

### AD-003
- **Decision**: The agent never runs `git commit` (or any commit-creating operation - revert, cherry-pick, merge) on this repository, in any mode, including the `tlc-spec-driven` Execute phase and the Verifier's report-writing step. The user makes every commit. The agent may stage changes, run gates, and propose a commit message, but stops short of committing.
- **Reason**: User request, triggered after reviewing a session where the agent made 3 commits (including Verifier-authored `validation.md`/traceability updates) directly to local `main` without the user pressing the button. Those 3 commits were reverted via `git reset --soft` back to `603f99d` on 2026-08-10 - their content is preserved as staged/working-tree changes, nothing was lost, but the commits themselves no longer exist in history.
- **Trade-off**: Every gate-passing task or validation cycle now ends with the agent handing off a diff + suggested message instead of a finished commit - one extra manual step per unit of work, in exchange for the user retaining full control over what lands in history and when.
- **Scope**: Project-wide, all workflows, supersedes the "creates the commit" clause in `AD-001` and any similar clause in `.claude/skills/tlc-spec-driven` (that skill's default "the orchestrator creates the commit" behavior does not apply here). Also recorded in `CLAUDE.md`'s Critical interaction rule.
- **Date**: 2026-08-10
- **Status**: active

## Handoff

- **Feature**: streaming-ingestion
- **Phase / Task**: All 5 P2 tasks (T1-T5) were previously committed/merged to `main` via PR #2 (`603f99d`). This session's Verifier work (iteration 1 FAIL → 2 test gaps found in ING-10/ING-12 → fixes applied via `/mentor-help` → iteration 2 PASS, 66 tests, all 12 requirements Verified) was done as 3 commits that have since been **reverted per AD-003** (`git reset --soft 603f99d`) - their content is intact as uncommitted changes in the working tree, ready for the user to review and commit themselves.
- **Completed**: T1-T5 (committed, on `main`). The P2 verification/fix cycle (test coverage for ING-10/ING-12 + `type=int` fix + typo fix + `validation.md` + `spec.md` traceability update) is done and gate-passing but **uncommitted** - staged in the working tree, pending the user's own `git commit`.
- **In-progress**: none
- **Next step**: User commits the staged P2-verification changes (`ingestion/app.py`, `ingestion/use_case.py`, `tests/ingestion/adapters/test_client.py`, `tests/ingestion/test_app.py`, `.specs/STATE.md`, `.specs/features/streaming-ingestion/{spec,validation}.md`, `.specs/LESSONS.md`, `.specs/lessons.json`) at their own pace - the agent will not commit them. Separately, `spec.md`'s Success Criteria still has one open item: *"The service can run unattended for hours without crashing on a transient GitHub/Kafka failure"* - a live observation, gated on a `RateLimitError` the user is watching resolve (`reset_at=2026-08-10 16:42:13 UTC`). After that, the next feature-level activity is planning Flink normalization/aggregation over `events-raw`.
- **Blockers**: none
- **Uncommitted files**: see Next step list above - all staged, working tree otherwise matches `origin/main`
- **Branch**: main (feature branch `feat/github-ingestion-resilience` merged via PR #2, not deleted locally/remotely - deletion is a remote operation outside this session's default authorization)
