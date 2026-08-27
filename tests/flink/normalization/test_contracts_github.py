import copy
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase

import flink.normalization
from flink.normalization.domain.contract_repository import YamlContractRepository
from flink.normalization.domain.evaluator import NormalizationRulesEventEvaluator
from flink.normalization.domain.normalizer import EventNormalizer
from shared.models import RawEvent
from tests.fixtures.events import DOC_SHAPED_GITHUB_EVENTS, SAMPLE_SHAPED_GITHUB_EVENTS

CONTRACTS_DIR = (
    Path(flink.normalization.__file__).parent.parent.parent / "interface" / "sources"
)

OBSERVED_AT = "2026-07-17T12:21:32+00:00"
OBSERVED_AT_MILLIS = 1784290892000
OBSERVED_AT_DATETIME = datetime(2026, 7, 17, 12, 21, 32, tzinfo=timezone.utc)

ENVELOPE_FIELDS = {
    "source",
    "event_id",
    "event_type",
    "entity_id",
    "entity_name",
    "event_time",
    "ingested_at",
    "schema_version",
    "partition_key",
}

COMMON_FIELDS = {"repo_id", "repo_name", "org_id", "org_login", "public"}

PER_TYPE_FIELDS = {
    "IssueCommentEvent": {
        "action",
        "issue_id",
        "issue_number",
        "issue_title",
        "issue_state",
        "issue_created_at",
        "issue_updated_at",
        "issue_comments_count",
        "issue_is_pull_request",
        "issue_user_login",
        "issue_labels",
        "comment_id",
        "comment_body",
        "comment_user_login",
        "comment_created_at",
        "comment_updated_at",
    },
    "PullRequestEvent": {
        "action",
        "pr_number",
        "pr_id",
        "pr_base_ref",
        "pr_base_sha",
        "pr_base_repo_id",
        "pr_base_repo_name",
        "pr_head_ref",
        "pr_head_sha",
        "pr_head_repo_id",
        "pr_head_repo_name",
        "label_name",
        "labels",
    },
    "PullRequestReviewEvent": {
        "action",
        "pr_number",
        "pr_id",
        "pr_base_ref",
        "pr_head_ref",
        "review_id",
        "review_state",
        "review_body",
        "review_submitted_at",
        "review_user_login",
    },
    "PullRequestReviewCommentEvent": {
        "action",
        "pr_number",
        "pr_id",
        "pr_base_ref",
        "pr_head_ref",
        "comment_id",
        "comment_body",
        "comment_path",
        "comment_position",
        "comment_diff_hunk",
        "comment_commit_id",
        "comment_created_at",
        "comment_updated_at",
        "comment_user_login",
    },
    "WatchEvent": {"action"},
    "CreateEvent": {
        "ref",
        "ref_type",
        "master_branch",
        "description",
        "pusher_type",
    },
    "DeleteEvent": {"ref", "ref_type", "pusher_type"},
    "PublicEvent": set(),
    "GollumEvent": {"pages"},
    "IssuesEvent": {
        "action",
        "issue_id",
        "issue_number",
        "issue_title",
        "issue_state",
        "issue_created_at",
        "issue_updated_at",
        "issue_comments_count",
        "issue_user_login",
        "issue_labels",
        "assignee_login",
        "assignees",
        "label_name",
        "labels",
    },
    "MemberEvent": {"action", "member_id", "member_login"},
}


class GithubContractTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.events = copy.deepcopy(SAMPLE_SHAPED_GITHUB_EVENTS)

    def setUp(self):
        self.normalizer = EventNormalizer(
            event_evaluator=NormalizationRulesEventEvaluator(),
            contract_repository=YamlContractRepository(contracts_dir=CONTRACTS_DIR),
        )

    def normalize(self, event: dict) -> dict:
        return self.normalizer.normalize(
            RawEvent(
                source="github",
                source_event_id=str(event["id"]),
                source_event_endpoint="events",
                source_event_type=event["type"],
                observed_at=OBSERVED_AT,
                schema_version=1,
                payload=event,
            )
        ).model_dump()


class TestGithubContractEnvelopeAndCommon(GithubContractTestCase):

    def setUp(self):
        super().setUp()
        self.event = self.events["IssueCommentEvent"]
        self.result = self.normalize(self.event)

    def test_can_resolve_partition_key_from_repo_name(self):
        self.assertEqual(self.result["partition_key"], self.event["repo"]["name"])

    def test_can_carry_envelope_identity_from_the_raw_event(self):
        self.assertEqual(self.result["source"], "github")
        self.assertEqual(self.result["event_id"], str(self.event["id"]))
        self.assertEqual(self.result["event_type"], "IssueCommentEvent")
        self.assertEqual(self.result["schema_version"], 1)

    def test_can_resolve_entity_from_the_payload_root(self):
        # entity_id/entity_name (renamed 2026-08-19 from actor_id/actor_login) - same source
        # values, generalized envelope field names. entity_id is cast to str by NormalizedEvent.
        self.assertEqual(self.result["entity_id"], str(self.event["actor"]["id"]))
        self.assertEqual(self.result["entity_name"], self.event["actor"]["login"])

    def test_can_convert_both_timestamps_to_epoch_millis(self):
        # ingested_at is typed `datetime` on NormalizedEvent, so the epoch-millis int
        # `to_millis()` produces gets coerced into a real datetime by Pydantic - flagged in
        # `.specs/STATE.md` (2026-08-19) as a discrepancy from spec.md FLK-09's plain-int wording.
        self.assertEqual(self.result["ingested_at"], OBSERVED_AT_DATETIME)
        self.assertIsInstance(self.result["event_time"], int)

    def test_can_resolve_common_repo_and_org_block(self):
        self.assertEqual(self.result["repo_id"], self.event["repo"]["id"])
        self.assertEqual(self.result["repo_name"], self.event["repo"]["name"])
        self.assertEqual(self.result["org_id"], self.event["org"]["id"])
        self.assertEqual(self.result["org_login"], self.event["org"]["login"])

    def test_can_resolve_public_as_the_real_boolean_not_a_presence_check(self):
        self.assertEqual(self.result["public"], self.event["public"])
        self.assertIsInstance(self.result["public"], bool)

    def test_can_return_null_org_fields_for_an_event_without_an_org(self):
        orgless_event = copy.deepcopy(self.event)
        orgless_event.pop("org")

        result = self.normalize(orgless_event)

        self.assertIsNone(result["org_id"])
        self.assertIsNone(result["org_login"])

    def test_can_omit_raw_event_fields_the_spec_does_not_declare(self):
        for raw_only_field in (
            "source_event_id",
            "source_event_type",
            "source_event_endpoint",
            "observed_at",
            "payload",
        ):
            self.assertNotIn(raw_only_field, self.result)


class TestGithubContractDeclaredEventTypes(GithubContractTestCase):

    # Guards the subTest loop below: without this, a fixture that stopped
    # carrying these types would still report a green, empty run.
    def test_the_fixture_carries_every_sample_verified_type(self):
        self.assertEqual(
            {
                "IssueCommentEvent",
                "PullRequestEvent",
                "PullRequestReviewEvent",
                "PullRequestReviewCommentEvent",
            },
            set(self.events),
        )

    def test_can_produce_exactly_the_spec_field_set_for_each_sample_verified_type(self):
        for event_type, event in self.events.items():
            with self.subTest(event_type=event_type):
                result = self.normalize(event)

                self.assertEqual(
                    set(result),
                    ENVELOPE_FIELDS | COMMON_FIELDS | PER_TYPE_FIELDS[event_type],
                )

    def test_can_fall_back_to_envelope_and_common_for_an_undeclared_type(self):
        undeclared = copy.deepcopy(DOC_SHAPED_GITHUB_EVENTS["WatchEvent"])
        undeclared["type"] = "PushEvent"

        result = self.normalize(undeclared)

        self.assertEqual(set(result), ENVELOPE_FIELDS | COMMON_FIELDS)
        self.assertEqual(result["event_type"], "PushEvent")
        self.assertIsNotNone(result["partition_key"])


class TestGithubContractFieldSemantics(GithubContractTestCase):

    def test_can_pluck_issue_labels_as_names_not_objects(self):
        result = self.normalize(self.events["IssueCommentEvent"])

        self.assertTrue(result["issue_labels"])
        for label in result["issue_labels"]:
            self.assertIsInstance(label, str)

    def test_can_pluck_pull_request_labels_as_names_not_objects(self):
        result = self.normalize(self.events["PullRequestEvent"])

        self.assertTrue(result["labels"])
        for label in result["labels"]:
            self.assertIsInstance(label, str)

    def test_can_report_issue_is_pull_request_when_the_key_is_present(self):
        event = copy.deepcopy(self.events["IssueCommentEvent"])
        event["payload"]["issue"]["pull_request"] = {"url": "https://api.github.com/x"}

        self.assertIs(self.normalize(event)["issue_is_pull_request"], True)

    def test_can_report_issue_is_pull_request_when_the_key_is_absent(self):
        event = copy.deepcopy(self.events["IssueCommentEvent"])
        event["payload"]["issue"].pop("pull_request", None)

        self.assertIs(self.normalize(event)["issue_is_pull_request"], False)

    def test_can_resolve_label_name_when_payload_label_is_present(self):
        event = copy.deepcopy(self.events["PullRequestEvent"])
        event["payload"]["label"] = {"name": "needs-triage"}

        self.assertEqual(self.normalize(event)["label_name"], "needs-triage")

    def test_can_return_null_label_name_when_payload_label_is_absent(self):
        event = copy.deepcopy(self.events["PullRequestEvent"])
        event["payload"].pop("label", None)

        self.assertIsNone(self.normalize(event)["label_name"])


class TestGithubContractDocResolvedEventTypes(GithubContractTestCase):

    def test_can_produce_exactly_the_spec_field_set_for_each_doc_resolved_type(self):
        for event_type, event in DOC_SHAPED_GITHUB_EVENTS.items():
            with self.subTest(event_type=event_type):
                result = self.normalize(event)

                self.assertEqual(
                    set(result),
                    ENVELOPE_FIELDS | COMMON_FIELDS | PER_TYPE_FIELDS[event_type],
                )

    def test_can_exclude_full_ref_from_create_event(self):
        event = DOC_SHAPED_GITHUB_EVENTS["CreateEvent"]

        result = self.normalize(event)

        self.assertIn("full_ref", event["payload"])
        self.assertNotIn("full_ref", result)

    def test_can_produce_envelope_and_common_for_public_event_with_no_per_type_fields(
        self,
    ):
        result = self.normalize(DOC_SHAPED_GITHUB_EVENTS["PublicEvent"])

        self.assertEqual(set(result), ENVELOPE_FIELDS | COMMON_FIELDS)
        self.assertEqual(result["event_type"], "PublicEvent")
        self.assertEqual(result["repo_name"], "octo/repo")

    def test_can_curate_gollum_pages_dropping_html_url(self):
        result = self.normalize(DOC_SHAPED_GITHUB_EVENTS["GollumEvent"])

        self.assertEqual(
            result["pages"],
            [
                {
                    "page_name": "Home",
                    "title": "Home",
                    "summary": None,
                    "action": "edited",
                    "sha": "abc123",
                }
            ],
        )

    def test_can_resolve_issues_event_assignees_as_logins(self):
        result = self.normalize(DOC_SHAPED_GITHUB_EVENTS["IssuesEvent"])

        self.assertEqual(result["assignee_login"], "maintainer")
        self.assertEqual(result["assignees"], ["maintainer", "helper"])
        self.assertEqual(result["issue_labels"], ["bug", "p1"])

    def test_can_return_null_assignee_login_when_absent(self):
        event = copy.deepcopy(DOC_SHAPED_GITHUB_EVENTS["IssuesEvent"])
        event["payload"]["issue"].pop("assignee")

        self.assertIsNone(self.normalize(event)["assignee_login"])
