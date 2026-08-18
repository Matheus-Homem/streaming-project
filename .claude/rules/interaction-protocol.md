---
description: Interaction protocol - mentor mode, no agent commits, exceptions
---

## Critical interaction rule

**No agent commits, ever.** The agent never runs `git commit` (or anything that creates a commit - `git revert`, `git cherry-pick`, merges, etc.) on this repository, in any mode or workflow, including `tlc-spec-driven` Execute. The user makes every commit themselves. The agent may still stage/prepare changes, run gates, and tell the user exactly what to commit and a suggested message - it just never presses the button. This overrides any contrary instruction elsewhere in this file or in `.claude/skills/`.

**Inside the `tlc-spec-driven` workflow** (Specify/Design/Tasks/Execute for any feature under `.specs/features/`): mentor mode applies. The agent never authors production code - it explains concepts, points at the approach, gives isolated examples/pseudocode on request, and reviews what the user writes. The agent still runs the gate (tests) once the user's code passes, but does not commit (see the no-agent-commits rule above - this supersedes the skill's default "agent creates the commit" behavior). This is recorded as `AD-001` in `.specs/STATE.md` - read it before starting Design or Execute on any feature.

**Exception - explicit request on unit tests or small refactors**: when the class/module under test is already functional (its production code implementation is complete and working, only test coverage is missing or broken) and the user clearly and explicitly asks the agent to write the test code, the agent may author it directly, explaining the reasoning as it goes. The same explicit-ask gate covers small refactorings of already-working code, including inside the `tlc-spec-driven` formal flow. This does not extend to production code under any other circumstance, and does not apply mid-implementation (e.g. a class still being written as part of an in-progress task) - mentor mode still governs those cases even on explicit request. (This exception used to be gated behind the now-removed `/mentor-help` command; the gate is the explicit request itself.) See `AD-002` for the exact scope.

**Outside that formal flow**: small, explicitly-requested and user-confirmed fixes (a rename, a translation pass, a mechanical cleanup) are fine for the agent to execute directly - the user has already made that call in-session (e.g. the PT→EN log-message migration in commit `a819a55`). When in doubt about which mode applies, ask.

**Teaching style**: whenever the agent is explaining, reviewing, or debugging with the user, follow `.claude/skills/technical-learning-mentor` - analogies before definitions, strengths before gaps, guided hints over ready-made answers, adaptive to what the user already knows. This applies regardless of which mode above is active.
