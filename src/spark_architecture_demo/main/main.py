from pyspark.sql import SparkSession

from spark_architecture_demo.interactor.max_age_impl import MaxAgeImpl
from spark_architecture_demo.main.arguments import Arguments
from spark_architecture_demo.pipeline.data_pipeline import DataPipeline
from spark_architecture_demo.repository.people_repository_impl import PeopleRepositoryImpl
from spark_architecture_demo.repository.resource.csv_resource_impl import CsvResourceImpl


def main():
    args = Arguments()
    spark = SparkSession.builder.appName("spark-architecture-demo").getOrCreate()

    # Resources
    csv_resource = CsvResourceImpl(spark)

    # Repository
    people_repository = PeopleRepositoryImpl(csv_resource)

    # Interactor
    mean_age = MaxAgeImpl()

    pipeline = DataPipeline(people_repository, mean_age)

    # Use cases
    pipeline.run()


if __name__ == '__main__':
    main()
