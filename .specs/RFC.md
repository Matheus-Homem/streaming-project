# RFC: streaming-project

## Purpose

A hands-on project to expand the author's Data/Software Engineering and DevOps knowledge by building a streaming data pipeline end to end, rather than having it handed over working.

## Learning Goals

| Technology / Concept | Requirement |
| --- | --- |
| Kafka | Required |
| Flink | Required |
| Ability to plan architecture and choose components | Required |
| Grafana (observability) | Required |
| OpenSearch (distributed storage/search) | Required |
| Kubernetes | Opportunistic - if a natural chance shows up |
| Drone (CI/CD) | Opportunistic - if a natural chance shows up |
| Terraform (IaC) | Opportunistic - if a natural chance shows up |

## Interaction Constraint

The `.claude/skills/technical-learning-mentor` skill exists specifically so the author isn't handed ready-made answers from the AI - it forces the author to think through and argue for their own decisions. The agent may help specify the project, plan architecture, and explain concepts, but **must not write the code in the author's place**. See `AD-001` in `.specs/STATE.md` for how this is enforced during Execute.

## Prior Planning

An earlier plan existed in `docs/github-streaming-jira-roadmap.md`, but it was drafted before all the target technologies above were decided on, so it doesn't cover every learning goal.

## Approach

The author has never built a streaming pipeline before. The chosen strategy is to start from a basic MVP and incorporate the remaining technologies incrementally in later versions, rather than designing the full target architecture up front.
