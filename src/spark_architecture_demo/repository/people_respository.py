import abc

from pyspark.sql import DataFrame


class PeopleRepository(abc.ABC):

    @abc.abstractmethod
    def get_people(self) -> DataFrame:
        pass
