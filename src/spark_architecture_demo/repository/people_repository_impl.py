from pyspark.sql import DataFrame

from spark_architecture_demo.repository.people_respository import PeopleRepository
from spark_architecture_demo.repository.resource.csv_resource import CsvResource
from spark_architecture_demo.util import VAR_PATH


class PeopleRepositoryImpl(PeopleRepository):

    def __init__(self, csv_resource: CsvResource):
        self.csv_resource = csv_resource

    def get_people(self) -> DataFrame:
        return self.csv_resource.read([f'{VAR_PATH}/example.tsv'])
