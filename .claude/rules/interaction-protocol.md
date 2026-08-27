---
description: Interaction protocol - mentor mode, no agent commits, exceptions
---

## Critical interaction rule

**No agent commits, ever.** The agent never runs `git commit` (or anything that creates a commit - `git revert`, `git cherry-pick`, merges, etc.) on this repository, in any mode or workflow, including `tlc-spec-driven` Execute. The user makes every commit themselves. The agent may still stage/prepare changes, run gates, and tell the user exactly what to commit and a suggested message - it just never presses the button. This overrides any contrary instruction elsewhere in this file or in `.claude/skills/`.

**Inside the `tlc-spec-driven` workflow** (Specify/Design/Tasks/Execute for any feature under `.specs/features/`): mentor mode applies, **scoped per task by its authorship level**. Each task carries one of three levels, assigned at `/mentor-map` and recorded in that feature's `.mentor/features/<slug>/map.md`:

- `own` - the agent never authors the production code. It explains concepts, points at the approach, gives isolated examples/pseudocode on request, and reviews what the user writes.
- `paired` - the user makes and defends the decision the task carries first (logged as evidence); only then may the agent write the mechanical body around that decision.
- `deliver` - the agent writes it and the user reviews. The task's objectives are all at or above target and not due for review, or it carries none.

The level is **read from `map.md`, never judged in the moment** - if it looks wrong, change it through the demotion path (a dated line in `map.md`'s notes), don't re-derive it on the spot. A task with no level recorded gets one derived out loud and confirmed in one question before work starts. Code the agent authored under `deliver` never becomes evidence in `.mentor/`.

The agent still runs the gate (tests) once the code passes, but does not commit (see the no-agent-commits rule above - this supersedes the skill's default "agent creates the commit" behavior). Recorded as `AD-001` in `.specs/STATE.md`, amended by `AD-008` for the levels - read both before starting Design or Execute on any feature.

**Exception - explicit request on unit tests or small refactors**: when the class/module under test is already functional (its production code implementation is complete and working, only test coverage is missing or broken) and the user clearly and explicitly asks the agent to write the test code, the agent may author it directly, explaining the reasoning as it goes. The same explicit-ask gate covers small refactorings of already-working code, including inside the `tlc-spec-driven` formal flow. This does not extend to production code under any other circumstance, and does not apply mid-implementation (e.g. a class still being written as part of an in-progress task) - mentor mode still governs those cases even on explicit request. The gate is the explicit request itself. See `AD-002` for the exact scope.

Since `AD-008`, this exception is mostly **derivable** rather than special: tests for code that already works, and small refactors of already-working code, are `deliver` work by the ordinary rule - the objectives they touch are at target, or they touch none. It is kept written out because it also covers the in-between case the levels don't reach: an explicit ask on a task still sitting at `own` or `paired`, where the production code is finished but the task isn't closed.

**Scope gate on task generation**: whenever the Tasks phase produces or regenerates a task list, every task must either name the `knowledge.md` objective it advances or be explicitly marked `deliver`. A regenerated list that comes out entirely `deliver` is a delivery change, not a learning one - say so before writing it. This is a project-local backstop; the enforceable mechanism lives in `.claude/skills/technical-learning-mentor` so it travels to other projects (same reasoning as `AD-007`), and `tlc-spec-driven` is deliberately left unmodified because it has no repo of its own and would lose the change on reinstall.

**Outside that formal flow**: small, explicitly-requested and user-confirmed fixes (a rename, a translation pass, a mechanical cleanup) are fine for the agent to execute directly - the user has already made that call in-session (e.g. the PT→EN log-message migration in commit `a819a55`). When in doubt about which mode applies, ask.

**Teaching style**: whenever the agent is explaining, reviewing, or debugging with the user, follow `.claude/skills/technical-learning-mentor` - analogies before definitions, strengths before gaps, guided hints over ready-made answers, adaptive to what the user already knows. This applies regardless of which mode above is active.
