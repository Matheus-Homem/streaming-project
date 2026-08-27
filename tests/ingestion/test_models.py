import unittest
from unittest.mock import patch

from pydantic import ValidationError

from ingestion.models import AuthConfig, EventModel, SourceConfig, SourceYamlEntry


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

    def test_auth_defaults_to_none_when_omitted(self):
        entry = SourceYamlEntry(**self._valid_payload())
        self.assertIsNone(entry.auth)

    def test_auth_block_is_parsed_when_present(self):
        entry = SourceYamlEntry(
            **self._valid_payload(
                auth={
                    "env_var": "GITHUB_TOKEN",
                    "header": "Authorization",
                    "value_template": "Bearer {token}",
                }
            )
        )
        self.assertEqual(entry.auth.env_var, "GITHUB_TOKEN")
        self.assertEqual(entry.auth.header, "Authorization")
        self.assertEqual(entry.auth.value_template, "Bearer {token}")


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


class TestSourceConfigAccessors(unittest.TestCase):

    def _config(self, id_field="id", type_field="type", auth=None):
        return SourceConfig(
            source="github",
            endpoints={"default": "https://api.github.com/events"},
            headers={
                "rate_limit_remaining": "X-RateLimit-Remaining",
                "rate_limit_reset": "X-RateLimit-Reset",
            },
            auth=auth,
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


class TestResolveAuthHeader(unittest.TestCase):

    def _auth(self, **overrides):
        payload = {
            "env_var": "GITHUB_TOKEN",
            "header": "Authorization",
            "value_template": "Bearer {token}",
        }
        payload.update(overrides)
        return AuthConfig(**payload)

    def _config(self, auth):
        return SourceConfig(
            source="github",
            endpoints={"default": "https://api.github.com/events"},
            headers={
                "rate_limit_remaining": "X-RateLimit-Remaining",
                "rate_limit_reset": "X-RateLimit-Reset",
            },
            auth=auth,
            id_field="id",
            type_field="type",
            variant="default",
            url="https://api.github.com/events",
        )

    def test_no_auth_configured_returns_empty_dict(self):
        config = self._config(auth=None)

        self.assertEqual(config.resolve_auth_header(), {})

    @patch.dict("os.environ", {}, clear=True)
    def test_auth_configured_but_env_var_unset_returns_empty_dict(self):
        config = self._config(auth=self._auth())

        self.assertEqual(config.resolve_auth_header(), {})

    @patch.dict("os.environ", {"GITHUB_TOKEN": ""}, clear=True)
    def test_auth_configured_but_env_var_blank_returns_empty_dict(self):
        config = self._config(auth=self._auth())

        self.assertEqual(config.resolve_auth_header(), {})

    @patch.dict("os.environ", {"GITHUB_TOKEN": "abc123"}, clear=True)
    def test_auth_configured_and_env_var_set_returns_header(self):
        config = self._config(auth=self._auth())

        self.assertEqual(
            config.resolve_auth_header(), {"Authorization": "Bearer abc123"}
        )

    @patch.dict("os.environ", {"GITLAB_TOKEN": "xyz789"}, clear=True)
    def test_header_name_and_template_come_from_config_not_hardcoded(self):
        config = self._config(
            auth=self._auth(
                env_var="GITLAB_TOKEN",
                header="PRIVATE-TOKEN",
                value_template="{token}",
            )
        )

        self.assertEqual(config.resolve_auth_header(), {"PRIVATE-TOKEN": "xyz789"})


if __name__ == "__main__":
    unittest.main()
