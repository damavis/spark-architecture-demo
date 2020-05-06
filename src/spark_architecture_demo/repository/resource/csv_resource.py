import abc
from typing import List

from pyspark.sql import DataFrame


class CsvResource(abc.ABC):

    @abc.abstractmethod
    def read(self, path: List[str], sep='\t') -> DataFrame:
        pass

    @abc.abstractmethod
    def read_with_schema(self, path: List[str], schema, sep='\t') -> DataFrame:
        pass
