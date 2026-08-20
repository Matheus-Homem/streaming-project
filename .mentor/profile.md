# Mentor profile

<!-- Written in English. Created on first /mentor-map. Edited only by /mentor-map,
     the commands that log time, or by the user directly. Delete a tag row to be
     re-asked about it. -->

## Config

- `spec_artifacts`: .specs/features/flink-normalization/*.md
- `default_eval_budget`: 15
- `active_feature`: flink-normalization
- `study_hours_total`: 24

<!-- study_hours_total is a running counter, never reset. Every command that can
     surface a review (mentor-eval, mentor-close) asks "how long have you studied
     since last time?" and adds it here before computing anything. This is what
     drives the exposure clock in references/retention.md — it is read, not
     estimated. -->

## Tags

| tag | experience | target | declared_on |
|---|---|---|---|
| kafka | a-little | decide | 2026-08-16 |
| distributed-systems | never | decide | 2026-08-16 |
| architecture-patterns | regular | decide | 2026-08-16 |
| pyflink | never | decide | 2026-08-16 |
| docker-compose | a-little | explain | 2026-08-16 |

<!-- experience seeds the initial state of that tag's objectives:
       never   -> unassessed
       a-little / regular -> declared  (self-report is weak evidence)
       skip    -> objectives archived for now
     target is the ceiling the user is aiming for in this tag; individual
     objectives may sit below it, never above it.
     Only tags absent from this table are asked about at /mentor-map. -->

## Notes

<!-- Anything durable about how this user wants to be mentored:
     formats they dislike, areas they explicitly do not want assessed,
     agreements about delegating specific work outright. -->

- `docker-compose`'s target (`explain`) capped `K-05` (Compose completion-ordering pattern)
  down from the `decide` bucket sort to `explain` — the ceiling rule (objectives never sit
  above their tag's declared target) overrides the bucket-sort default. Worth remembering:
  the user opted into lighter assessment on Compose specifically, not on the whole feature.
- **2026-08-18**: user asked that all timestamps recorded by this skill (`evidence.jsonl` `ts`,
  and any other logged time) use UTC-3 (`America/Sao_Paulo`) going forward instead of UTC.
  Applies from this point on; evidence written before this note (E-01..E-11) stays as recorded
  in UTC, not backfilled.
