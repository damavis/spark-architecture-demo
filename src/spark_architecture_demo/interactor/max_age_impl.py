from pyspark.sql import DataFrame

from spark_architecture_demo.interactor.max_age import MaxAge


class MaxAgeImpl(MaxAge):

    def apply(self, data: DataFrame) -> DataFrame:
        return data.agg({"age": "max"})
