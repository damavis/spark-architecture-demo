import logging
import unittest
from typing import List, Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class PySparkTestBase(unittest.TestCase):
    spark = None

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_pyspark_session(cls):
        spark = SparkSession.builder.master('local[*]') \
            .appName('spark_architecture_demo') \
            .config('spark.ui.showConsoleProgress', 'true') \
            .config('spark.scheduler.mode', 'FIFO') \
            .config("spark.sql.broadcastTimeout", "36000") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

        return spark

    @classmethod
    def setUpClass(cls):
        if not cls.spark:
            cls.suppress_py4j_logging()
            cls.spark = cls.create_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()
            cls.spark = None

    def createDataFrame(self, data: List[List[Any]],
                        schema: StructType = None,
                        columns: List[str] = None) -> DataFrame:

        rows = [Row(*item) for item in data]
        dataframe = self.spark.createDataFrame(rows, schema=schema)
        if columns:
            return dataframe.toDF(*columns)
        return dataframe

    def createDataFrameFromDict(self, data: List[Dict],
                                schema: StructType = None,
                                columns: List[str] = None) -> DataFrame:
        rows = [Row(**item) for item in data]
        dataframe = self.spark.createDataFrame(rows, schema=schema)
        if columns:
            return dataframe.toDF(*columns)
        return dataframe

    def assertDataFrameEquals(self, expected: DataFrame,
                              actual: DataFrame):
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.collect(), actual.collect())

    def set_df_columns_nullable(self, df: DataFrame, column_list: list, nullable: bool) -> DataFrame:
        for struct_field in df.schema:
            if struct_field.name in column_list:
                struct_field.nullable = nullable

        return self.spark.createDataFrame(df.rdd, df.schema)
