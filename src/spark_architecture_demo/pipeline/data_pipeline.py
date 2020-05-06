from spark_architecture_demo.interactor.max_age import MaxAge
from spark_architecture_demo.repository.people_respository import PeopleRepository


class DataPipeline:
    def __init__(self, people_repository: PeopleRepository, max_age: MaxAge):
        self.people_repository = people_repository
        self.max_age = max_age

    def run(self):
        data = self.people_repository.get_people()
        self.max_age.apply(data).show()
