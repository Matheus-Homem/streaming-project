import runpy
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

from flink.analytics.app import main, split_statements


class TestSplitStatements(TestCase):

    def test_empty_string_returns_empty_list(self):
        self.assertEqual(split_statements(""), [])

    def test_single_statement_with_no_trailing_semicolon(self):
        self.assertEqual(split_statements("SELECT 1"), ["SELECT 1"])

    def test_single_statement_with_trailing_semicolon_has_no_empty_extra(self):
        self.assertEqual(split_statements("SELECT 1;"), ["SELECT 1"])

    def test_multiple_statements_are_split_in_order(self):
        self.assertEqual(
            split_statements("CREATE TABLE t (a INT); INSERT INTO t SELECT 1;"),
            ["CREATE TABLE t (a INT)", "INSERT INTO t SELECT 1"],
        )

    def test_whitespace_only_segments_are_filtered(self):
        self.assertEqual(
            split_statements("SELECT 1;   ;\n;SELECT 2;"), ["SELECT 1", "SELECT 2"]
        )

    def test_each_statement_is_stripped(self):
        self.assertEqual(
            split_statements("\n  SELECT 1  ;\n  SELECT 2  "),
            ["SELECT 1", "SELECT 2"],
        )

    def test_only_semicolons_and_whitespace_returns_empty_list(self):
        self.assertEqual(split_statements(" ; ;\n; "), [])


class TestMain(TestCase):

    def test_enables_checkpointing_before_creating_the_table_environment(self):
        env = Mock()

        with patch("flink.analytics.app.StreamTableEnvironment") as mock_t_env_cls:
            main(sql_statements=["SELECT 1"], env=env)

        env.enable_checkpointing.assert_called_once_with(60000)
        mock_t_env_cls.create.assert_called_once_with(env)

    def test_executes_every_statement_in_order(self):
        env = Mock()
        statements = ["CREATE TABLE t (a INT)", "INSERT INTO t SELECT 1"]

        with patch("flink.analytics.app.StreamTableEnvironment") as mock_t_env_cls:
            main(sql_statements=statements, env=env)

        mock_t_env = mock_t_env_cls.create.return_value
        self.assertEqual(
            mock_t_env.execute_sql.call_args_list,
            [((statements[0],),), ((statements[1],),)],
        )

    def test_waits_on_the_last_statement_result(self):
        env = Mock()

        with patch("flink.analytics.app.StreamTableEnvironment") as mock_t_env_cls:
            main(sql_statements=["SELECT 1", "SELECT 2"], env=env)

        mock_t_env = mock_t_env_cls.create.return_value
        last_result = mock_t_env.execute_sql.return_value
        last_result.wait.assert_called_once_with()

    def test_empty_statement_list_does_not_wait_or_execute(self):
        env = Mock()

        with patch("flink.analytics.app.StreamTableEnvironment") as mock_t_env_cls:
            main(sql_statements=[], env=env)

        mock_t_env = mock_t_env_cls.create.return_value
        mock_t_env.execute_sql.assert_not_called()


class TestAppAsMain(TestCase):

    def setUp(self):
        self.patchers = [
            patch("dotenv.load_dotenv"),
            patch("shared.logger.setup_logging"),
            patch("pyflink.datastream.StreamExecutionEnvironment"),
            patch("pyflink.table.StreamTableEnvironment"),
            patch("pathlib.Path.read_text"),
        ]
        (
            self.mock_load_dotenv,
            self.mock_setup_logging,
            self.mock_stream_execution_environment,
            self.mock_stream_table_environment,
            self.mock_read_text,
        ) = (patcher.start() for patcher in self.patchers)
        for patcher in self.patchers:
            self.addCleanup(patcher.stop)

    def run_app_as_main(self):
        # TestSplitStatements/TestMain above import `flink.analytics.app` normally,
        # which leaves it in sys.modules - drop it first so runpy re-executes the
        # module fresh instead of warning about the stale cached entry.
        sys.modules.pop("flink.analytics.app", None)
        runpy.run_module("flink.analytics.app", run_name="__main__")

    def test_raises_key_error_when_query_file_env_var_is_missing(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(KeyError):
                self.run_app_as_main()

        self.mock_read_text.assert_not_called()

    def test_reads_and_splits_the_sql_file_named_by_the_env_var(self):
        self.mock_read_text.return_value = (
            "CREATE TABLE t (a INT); INSERT INTO t SELECT 1;"
        )

        with patch.dict("os.environ", {"ANALYTICS_QUERY_FILE": "repo_counts_5m.sql"}):
            self.run_app_as_main()

        mock_env = (
            self.mock_stream_execution_environment.get_execution_environment.return_value
        )
        mock_t_env = self.mock_stream_table_environment.create.return_value
        self.assertEqual(
            mock_t_env.execute_sql.call_args_list,
            [
                (("CREATE TABLE t (a INT)",),),
                (("INSERT INTO t SELECT 1",),),
            ],
        )
        mock_env.enable_checkpointing.assert_called_once_with(60000)

    def test_empty_query_file_runs_cleanly_with_no_statements(self):
        self.mock_read_text.return_value = ""

        with patch.dict("os.environ", {"ANALYTICS_QUERY_FILE": "empty.sql"}):
            self.run_app_as_main()

        mock_t_env = self.mock_stream_table_environment.create.return_value
        mock_t_env.execute_sql.assert_not_called()


if __name__ == "__main__":
    import unittest

    unittest.main()
