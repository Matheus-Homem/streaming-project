# RFC: streaming-project

## Purpose

A hands-on project to expand the author's Data/Software Engineering and DevOps knowledge by building a streaming data pipeline end to end, rather than having it handed over working.

## Related documents

`.specs/RFC-conviction-index.md` explores a concrete capstone idea (a composite crypto signal joining spot price, mempool congestion, and futures leverage) and works out what this RFC's target platform would have to become to support it. It does not replace this RFC - it states *why* the project exists; that one states *what the platform should be able to prove it can do*, exercising this RFC's own architecture decisions (`AD-004`, `AD-006`, `AD-009`) against a harder case than any source built so far.

## Learning Goals

| Technology / Concept | Requirement |
| --- | --- |
| Kafka | Required |
| Flink | Required |
| Ability to plan architecture and choose components | Required |
| Grafana (observability) | Required |
| OpenSearch (distributed storage/search) | Required |
| Kubernetes | Required |
| Drone (CI/CD) | Opportunistic - if a natural chance shows up |
| Terraform (IaC) | Opportunistic - if a natural chance shows up |

**Kubernetes moved from opportunistic to required (2026-08-28)**: the project's mental model already treats "one process, one container, one source" as the norm, not "one process handling every source" - `infra/docker/docker-compose.yml` runs a single `ingestion` service today only because there is a single wired source (GitHub), not because the architecture assumes one. `AD-005` (`.specs/STATE.md`) already anticipated this when it named "5 ingestion processes" as part of the condition that would make Kubernetes worth adopting. `.specs/RFC-conviction-index.md` gives that condition a concrete path to happen (5 sources, 2 transport models), so Kubernetes stops being contingent on a chance appearing and becomes a goal the project plans toward. `AD-005`'s trigger-condition framing should be revisited to match - see the note left in `.specs/STATE.md`.

## Interaction Constraint

The `.claude/skills/technical-learning-mentor` skill exists specifically so the author isn't handed ready-made answers from the AI - it forces the author to think through and argue for their own decisions. The agent may help specify the project, plan architecture, and explain concepts, but **must not write the code in the author's place**. See `AD-001` in `.specs/STATE.md` for how this is enforced during Execute.

## Prior Planning

An earlier plan existed in `docs/github-streaming-jira-roadmap.md`, but it was drafted before all the target technologies above were decided on, so it doesn't cover every learning goal.

## Approach

The author has never built a streaming pipeline before. The chosen strategy is to start from a basic MVP and incorporate the remaining technologies incrementally in later versions, rather than designing the full target architecture up front.
