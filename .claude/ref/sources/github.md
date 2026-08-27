# GitHub Events API - fields used by the pipeline

This is a **curated** summary, not the full official GitHub Events API schema. It covers only the fields `ingestion/` and `flink/normalization/` actually consume today, extracted from real events pulled from the public API and from the contract `interface/sources/github/normalization.yml`. Those events are inlined in `tests/fixtures/events.py` (`GITHUB_EVENT` and `SAMPLE_SHAPED_GITHUB_EVENTS`) - the raw capture file they came from is a local artifact and never enters the repo (`.gitignore`'s `*_sample.*`). If a new field is needed, check its real shape there (or in GitHub's official docs) instead of assuming - the API has many more fields than the ones listed here.

Event types backed by a real captured event: `IssueCommentEvent`, `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`, `PullRequestEvent`. `WatchEvent` is declared in `github.yml` but has no captured instance - the shape below is the one GitHub documents officially, not verified locally.

## Envelope common to every event

Present on any event type (`type` varies):

```
id                        string  - the event's numeric id, as a string
type                       string  - "IssueCommentEvent", "PullRequestEvent", etc.
actor.id                   int
actor.login                string
repo.id                    int
repo.name                  string  - "org/repo"
created_at                 string  - ISO 8601, e.g. "2026-07-17T12:21:32Z"
org.login                  string  - ABSENT when the event does not belong to an org (use a default in the contract)
payload.action              string  - varies by event type ("created", "opened", "closed", etc.)
```

## `IssueCommentEvent`

```
payload.issue.labels                array of objects, each with at least:
  .name                              string  - used via `take: name` (pluck) in the contract
payload.issue.pull_request          object OR absent - its presence means the "issue" is really
                                     a PR; used as `payload.issue.pull_request != null`
                                     (expression escape hatch, see AD-006 in STATE.md)
payload.comment.id                  int
payload.comment.body                string
payload.comment.created_at          string  - ISO 8601
payload.comment.user.login          string
```

## `WatchEvent` (no captured instance - confirm if/when one shows up)

```
payload.action                      string  - typically "started" (starring a repo)
```

## `PullRequestEvent`, `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`

All three are captured in `tests/fixtures/events.py` (`SAMPLE_SHAPED_GITHUB_EVENTS`) and consumed by `interface/sources/github/normalization.yml`. When changing one of these mappings, inspect the real shape in that fixture - do not assume symmetry with `IssueCommentEvent`.
