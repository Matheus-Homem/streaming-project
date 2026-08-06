from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Union

from pydantic import BaseModel


class ExtractionType(Enum):
    ALL_EVENTS = "all_events"
    NETWORK_EVENTS = "network_events"
    ORGANIZATION_EVENTS = "organization_events"


class GitLabEventType(str, Enum):
    JOB = "Job Hook"
    PUSH = "Push Hook"
    ISSUE = "Issue Hook"
    EMOJI = "Emoji Hook"
    RELEASE = "Release Hook"
    SUBGROUP = "Subgroup Hook"
    TAG_PUSH = "Tag Push Hook"
    PIPELINE = "Pipeline Hook"
    ISSUE_COMMENT = "Note Hook"
    WIKI_PAGE = "Wiki Page Hook"
    MILESTONE = "Milestone Hook"
    DEPLOYMENT = "Deployment Hook"
    FEATURE_FLAG = "Feature Flag Hook"
    ACCESS_TOKEN = "Access Token Hook"
    GROUP_MEMBER = "Group Member Hook"
    MERGE_REQUEST = "Merge Request Hook"
    VULNERABILITY = "Vulnerability Hook"


class GitHubEventType(str, Enum):
    FORK = "ForkEvent"
    PUSH = "PushEvent"
    WATCH = "WatchEvent"
    CREATE = "CreateEvent"
    DELETE = "DeleteEvent"
    GOLLUM = "GollumEvent"
    ISSUES = "IssuesEvent"
    MEMBER = "MemberEvent"
    PUBLIC = "PublicEvent"
    RELEASE = "ReleaseEvent"
    DISCUSSION = "DiscussionEvent"
    SPONSORSHIP = "SponsorshipEvent"
    PULL_REQUEST = "PullRequestEvent"
    ISSUE_COMMENT = "IssueCommentEvent"
    COMMIT_COMMENT = "CommitCommentEvent"
    PULL_REQUEST_REVIEW = "PullRequestReviewEvent"
    PULL_REQUEST_REVIEW_COMMENT = "PullRequestReviewCommentEvent"


class SourceType(str, Enum):
    GITHUB = "github"
    GITLAB = "gitlab"


class EventModel(BaseModel):
    """
    Modelo de evento.
    """


class GitHubEvent(EventModel):
    id: str
    type: GitHubEventType
    actor: dict[str, Any]
    repo: dict[str, Any]
    payload: dict[str, Any]
    public: bool
    created_at: str
    org: dict[str, Any] = None


class GitLabEvent(EventModel):
    id: str


class RawEvent(BaseModel):
    source: SourceType
    source_event_id: str
    source_event_type: Union[GitHubEventType, GitLabEventType]
    observed_at: str
    schema_version: int
    payload: dict[str, Any]


@dataclass(frozen=True)
class SourceConfig:
    events_url: str
    network_events_url: Callable[[str, str], str]
    organization_events_url: Callable[[str], str]
    event_model: EventModel


SOURCE_REGISTRY: dict[SourceType, SourceConfig] = {
    SourceType.GITHUB: SourceConfig(
        events_url="https://api.github.com/events",
        network_events_url=lambda owner, repo: f"https://api.github.com/networks/{owner}/{repo}/events",
        organization_events_url=lambda org: f"https://api.github.com/orgs/{org}/events",
        event_model=GitHubEvent,
    ),
    SourceType.GITLAB: SourceConfig(
        events_url="https://gitlab.com",
        network_events_url=lambda owner, repo: f"https://gitlab.com{owner}/{repo}/events",
        organization_events_url=lambda org: f"https://gitlab.com/{org}/events",
        event_model=GitLabEvent,
    ),
}


def get_source_config(source: SourceType) -> SourceConfig:
    try:
        return SOURCE_REGISTRY[source]
    except KeyError:
        raise NotImplementedError(f"Fonte não suportada: {source}")
