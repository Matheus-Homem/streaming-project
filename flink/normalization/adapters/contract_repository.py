from logging import getLogger
from pathlib import Path

import yaml

from flink.normalization.models import NormalizationContract
from flink.normalization.ports import ContractRepositoryPort


class YamlContractRepository(ContractRepositoryPort):

    def __init__(self, contracts_dir: Path):
        self.logger = getLogger(self.__class__.__name__)
        self._contracts_dir = contracts_dir
        self._cache: dict[str, NormalizationContract] = {}

    def get(self, source: str) -> NormalizationContract:
        if source not in self._cache:
            self._cache[source] = self._load(source)
        return self._cache[source]

    def _load(self, source: str) -> NormalizationContract:
        path = self._contracts_dir / f"{source}.yml"
        self.logger.info(f"Loading contract for source={source} from {path}")
        try:
            with path.open() as file:
                config = yaml.safe_load(file)
            return NormalizationContract.model_validate(config)
        except FileNotFoundError:
            raise NotImplementedError(
                f"Source '{source}' is not implemented in Normalization pipeline"
            )
