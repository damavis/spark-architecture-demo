from typing import List

from pyspark.sql import DataFrame, SparkSession
from spark_architecture_demo.repository.resource.csv_resource import CsvResource


class CsvResourceImpl(CsvResource):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: List[str], header: str = "true", sep='\t') -> DataFrame:
        return self.spark.read.option("delimiter", sep).option("header", header).csv(path)

    def read_with_schema(self, path: List[str], schema, sep='\t') -> DataFrame:
        return self.spark.read.option("delimiter", sep).option("header", "true").schema(schema).csv(path)
