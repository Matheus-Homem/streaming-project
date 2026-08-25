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


def main():
    logger = getLogger("AggregationApplication")
    query_file = os.environ["ANALYTICS_QUERY_FILE"]

    query_path = Path(__file__).parent.parent.parent / "interface" / "analytics" / query_file
    logger.info(f"Starting application for query={query_file}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)

    for stmt in split_statements(query_path.read_text()):
        logger.info(f"Executing SQL statement {stmt}")
        result = t_env.execute_sql(stmt)

    result.wait()


if __name__ == "__main__":
    main()
