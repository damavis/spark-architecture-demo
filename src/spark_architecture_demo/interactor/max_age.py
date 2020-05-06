import abc

from pyspark.sql import DataFrame


class MaxAge(abc.ABC):

    @abc.abstractmethod
    def apply(self, data: DataFrame) -> DataFrame:
        pass
