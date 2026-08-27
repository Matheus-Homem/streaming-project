import os
from logging import getLogger
from pathlib import Path

from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from shared.logger import setup_logging

load_dotenv()
setup_logging()


def split_statements(sql_text: str) -> list[str]:
    return [s.strip() for s in sql_text.split(";") if s.strip()]


def main(
    sql_statements: list[str],
    env: StreamExecutionEnvironment,
):
    logger = getLogger("AggregationApplication")
    logger.info(f"Starting application with {len(sql_statements)} SQL statements")

    env.enable_checkpointing(60000)
    t_env = StreamTableEnvironment.create(env)

    result = None
    for stmt in sql_statements:
        logger.info(f"Executing SQL statement: {stmt[:50]}...")
        result = t_env.execute_sql(stmt)

    if result:
        result.wait()


if __name__ == "__main__":
    query_file = os.environ["ANALYTICS_QUERY_FILE"]
    sql_dir = Path(__file__).parent.parent.parent / "interface" / "analytics"
    raw_sql = (sql_dir / query_file).read_text()

    main(
        sql_statements=split_statements(raw_sql),
        env=StreamExecutionEnvironment.get_execution_environment(),
    )
