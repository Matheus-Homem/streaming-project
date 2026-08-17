import unittest

from pydantic import ValidationError

from ingestion.models import (
    EventModel,
    SourceConfig,
    SourceYamlEntry,
    get_source_config,
)


class TestSourceYamlEntry(unittest.TestCase):

    def _valid_payload(self, **overrides):
        payload = {
            "endpoints": {
                "default": "https://api.github.com/events",
                "network": "https://api.github.com/networks/{owner}/{repo}/events",
                "organization": "https://api.github.com/orgs/{org}/events",
            },
            "headers": {
                "rate_limit_remaining": "X-RateLimit-Remaining",
                "rate_limit_reset": "X-RateLimit-Reset",
            },
            "id_field": "id",
            "type_field": "type",
        }
        payload.update(overrides)
        return payload

    def test_valid_payload_creates_instance(self):
        entry = SourceYamlEntry(**self._valid_payload())
        self.assertEqual(entry.endpoints["default"], "https://api.github.com/events")
        self.assertEqual(entry.headers["rate_limit_remaining"], "X-RateLimit-Remaining")
        self.assertEqual(entry.id_field, "id")
        self.assertEqual(entry.type_field, "type")

    def test_missing_required_field_raises_validation_error(self):
        payload = self._valid_payload()
        del payload["id_field"]

        with self.assertRaises(ValidationError):
            SourceYamlEntry(**payload)


class TestEventModel(unittest.TestCase):

    def test_valid_payload_creates_instance(self):
        event = EventModel(id="1", type="PushEvent")
        self.assertEqual(event.id, "1")
        self.assertEqual(event.type, "PushEvent")

    def test_missing_required_field_raises_validation_error(self):
        with self.assertRaises(ValidationError):
            EventModel(id="1")

    def test_extra_fields_are_preserved(self):
        event = EventModel(id="1", type="PushEvent", actor={"login": "octocat"})
        self.assertEqual(event.model_dump()["actor"], {"login": "octocat"})


class TestGetSourceConfig(unittest.TestCase):

    def test_returns_config_for_github(self):
        config = get_source_config("github")

        self.assertEqual(config.source, "github")
        self.assertEqual(config.endpoints["default"], "https://api.github.com/events")
        self.assertEqual(config.id_field, "id")
        self.assertEqual(config.type_field, "type")

    def test_returns_config_for_gitlab(self):
        config = get_source_config("gitlab")

        self.assertEqual(config.endpoints["default"], "https://gitlab.com")
        self.assertEqual(config.id_field, "id")
        self.assertEqual(config.type_field, "object_kind")

    def test_raises_not_implemented_for_unknown_source(self):
        with self.assertRaises(NotImplementedError):
            get_source_config("bitbucket")

    def test_config_is_immutable(self):
        config = get_source_config("github")

        with self.assertRaises(Exception):
            config.url = "https://example.com"


class TestSourceConfigUrlResolution(unittest.TestCase):

    def _url(self, endpoint=None, endpoint_params=None):
        return get_source_config("github", endpoint, endpoint_params).url

    def test_omitted_endpoint_falls_back_to_default_variant(self):
        config = get_source_config("github")

        self.assertEqual(config.variant, "default")
        self.assertEqual(config.url, "https://api.github.com/events")

    def test_explicit_default_endpoint_returns_events_url(self):
        self.assertEqual(self._url("default"), "https://api.github.com/events")

    def test_network_endpoint_with_owner_and_repo(self):
        self.assertEqual(
            self._url("network", {"owner": "kubernetes", "repo": "kubernetes"}),
            "https://api.github.com/networks/kubernetes/kubernetes/events",
        )

    def test_organization_endpoint_with_org(self):
        self.assertEqual(
            self._url("organization", {"org": "anthropics"}),
            "https://api.github.com/orgs/anthropics/events",
        )

    def test_unknown_endpoint_variant_raises_value_error(self):
        with self.assertRaises(ValueError) as context:
            self._url("does-not-exist")

        self.assertIn("Unsupported endpoint", str(context.exception))

    def test_missing_endpoint_param_raises_value_error(self):
        with self.assertRaises(ValueError) as context:
            self._url("network", {"owner": "kubernetes"})

        self.assertIn("repo", str(context.exception))

    def test_no_endpoint_params_for_parameterized_variant_raises_value_error(self):
        with self.assertRaises(ValueError):
            self._url("organization")

    def test_extra_endpoint_params_are_ignored(self):
        self.assertEqual(
            self._url("organization", {"org": "anthropics", "owner": "unused"}),
            "https://api.github.com/orgs/anthropics/events",
        )


class TestSourceConfigAccessors(unittest.TestCase):

    def _config(self, id_field="id", type_field="type"):
        return SourceConfig(
            source="github",
            endpoints={"default": "https://api.github.com/events"},
            headers={
                "rate_limit_remaining": "X-RateLimit-Remaining",
                "rate_limit_reset": "X-RateLimit-Reset",
            },
            id_field=id_field,
            type_field=type_field,
            variant="default",
            url="https://api.github.com/events",
        )

    def test_get_event_id_and_type_from_flat_path(self):
        config = self._config()
        event = {"id": "1", "type": "PushEvent"}

        self.assertEqual(config.get_event_id(event), "1")
        self.assertEqual(config.get_event_type(event), "PushEvent")

    def test_get_event_value_from_nested_path(self):
        config = self._config(id_field="meta.event.id")
        event = {"meta": {"event": {"id": "42"}}}

        self.assertEqual(config.get_event_id(event), "42")

    def test_unresolvable_path_raises_value_error(self):
        config = self._config(id_field="meta.missing")

        with self.assertRaises(ValueError) as context:
            config.get_event_id({"meta": {"event": {"id": "42"}}})

        self.assertIn("meta.missing", str(context.exception))

    def test_path_through_non_dict_raises_value_error(self):
        config = self._config(id_field="id.nested")

        with self.assertRaises(ValueError):
            config.get_event_id({"id": "1"})

    def test_rate_limit_header_properties(self):
        config = self._config()

        self.assertEqual(config.rate_limit_remaining, "X-RateLimit-Remaining")
        self.assertEqual(config.rate_limit_reset, "X-RateLimit-Reset")


if __name__ == "__main__":
    unittest.main()
