from pathlib import Path
from unittest import TestCase

import ingestion
from ingestion.domain.source_config_repository import YamlSourceConfigRepository

SOURCES_DIR = Path(ingestion.__file__).parent.parent / "interface" / "sources"


class TestYamlSourceConfigRepositoryGet(TestCase):

    def setUp(self):
        self.repository = YamlSourceConfigRepository(sources_dir=SOURCES_DIR)

    def test_returns_config_for_github(self):
        config = self.repository.get("github")

        self.assertEqual(config.source, "github")
        self.assertEqual(config.endpoints["default"], "https://api.github.com/events")
        self.assertEqual(config.id_field, "id")
        self.assertEqual(config.type_field, "type")
        self.assertEqual(config.auth.env_var, "GITHUB_TOKEN")
        self.assertEqual(config.auth.header, "Authorization")
        self.assertEqual(config.auth.value_template, "Bearer {token}")

    def test_returns_config_for_gitlab(self):
        config = self.repository.get("gitlab")

        self.assertEqual(config.endpoints["default"], "https://gitlab.com")
        self.assertEqual(config.id_field, "id")
        self.assertEqual(config.type_field, "object_kind")
        self.assertIsNone(config.auth)

    def test_raises_not_implemented_for_unknown_source(self):
        with self.assertRaises(NotImplementedError):
            self.repository.get("bitbucket")

    def test_config_is_immutable(self):
        config = self.repository.get("github")

        with self.assertRaises(Exception):
            config.url = "https://example.com"

    def test_can_cache_result_for_repeated_calls_with_the_same_source(self):
        first_call = self.repository._load_entry("github")
        second_call = self.repository._load_entry("github")

        self.assertIs(first_call, second_call)


class TestYamlSourceConfigRepositoryUrlResolution(TestCase):

    def setUp(self):
        self.repository = YamlSourceConfigRepository(sources_dir=SOURCES_DIR)

    def _url(self, endpoint=None, endpoint_params=None):
        return self.repository.get("github", endpoint, endpoint_params).url

    def test_omitted_endpoint_falls_back_to_default_variant(self):
        config = self.repository.get("github")

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
