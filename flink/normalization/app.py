import os
from logging import getLogger
from pathlib import Path

from dotenv import load_dotenv
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from flink.common import KafkaFactory, KafkaSinkParams, KafkaSourceParams
from flink.normalization.adapters.contract_repository import YamlContractRepository
from flink.normalization.adapters.function import NormalizationFlatMapFunction
from flink.normalization.domain.evaluator import NormalizationRulesEventEvaluator
from flink.normalization.domain.normalizer import EventNormalizer
from shared.logger import setup_logging

load_dotenv()
setup_logging()


if __name__ == "__main__":
    logger = getLogger("NormalizationApplication")
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    logger.info(
        f"Starting normalization application, bootstrap_servers={bootstrap_servers}"
    )

    env = StreamExecutionEnvironment.get_execution_environment()

    kafka_source = KafkaFactory.create_source(
        params=KafkaSourceParams(
            topic="events-raw",
            group_id="normalization-consumer-group",
            bootstrap_servers=bootstrap_servers,
        )
    )
    kafka_sink = KafkaFactory.create_sink(
        params=KafkaSinkParams(
            topic="events-normalized",
            bootstrap_servers=bootstrap_servers,
        )
    )

    contract_repository = YamlContractRepository(
        contracts_dir=Path(__file__).parent / "sources"
    )
    normalizer = EventNormalizer(
        event_evaluator=NormalizationRulesEventEvaluator(),
        contract_repository=contract_repository,
    )
    flatmap_function = NormalizationFlatMapFunction(normalizer=normalizer)

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Normalization Stream",
    )
    stream.flat_map(flatmap_function, output_type=Types.STRING()).sink_to(kafka_sink)
    env.execute("normalization-job")
