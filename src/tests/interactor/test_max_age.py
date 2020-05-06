from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split

from spark_architecture_demo.interactor.max_age_impl import MaxAgeImpl
from tests.pyspark_test_base import PySparkTestBase


class TestMaxAge(PySparkTestBase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def get_data(self) -> DataFrame:
        test_data = [
            {"age": 20}, {"age": 13}, {"age": 35}, {"age": 53}, {"age": 22}
        ]
        return self.createDataFrameFromDict(test_data) \
            .select("age")

    def get_expected(self) -> DataFrame:
        test_data = [
            {"max(age)": 53}
        ]
        return self.createDataFrameFromDict(test_data) \
            .select("max(age)")

    def test_connection(self):
        max_age = MaxAgeImpl()

        data = self.get_data()
        expected = self.get_expected()

        result = max_age.apply(data)

        self.assertDataFrameEquals(expected, result)
