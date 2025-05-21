from redis import Redis

from src.exceptions import DependencyBrokenException
from src.models import TableRecord, FunctionalDependency, FieldDescriptor


def initialize_dependencies(functional_dependencies: list[FunctionalDependency]):
    parsed_dependencies: dict[FieldDescriptor, list[FunctionalDependency]] = dict()
    for dependency in functional_dependencies:
        if dependency.dependent not in parsed_dependencies:
            parsed_dependencies[dependency.dependent] = list()

        parsed_dependencies[dependency.dependent].append(dependency)
    return parsed_dependencies


class Core:
    def __init__(self, connection: Redis, functional_dependencies: list[FunctionalDependency], clean_redis=False):
        self.connection = connection

        self.parsed_dependencies: dict[FieldDescriptor, list[FunctionalDependency]] = initialize_dependencies(
            functional_dependencies)

        if clean_redis:
            self.clean_redis()

    def clean_redis(self):
        self.connection.flushdb()

    def check_record_dependencies(self, record: TableRecord):
        dependency_indexes_update_list: list[tuple[str, str]] = []

        for field_descriptor in record.table_definition.get_all_fields():
            field_value = record.get_value(field_descriptor)

            for dependency in self.parsed_dependencies.get(field_descriptor, []):
                dependency_key = dependency.get_key(record)
                dependency_index_random_member = self.connection.srandmember(dependency_key)

                if dependency_index_random_member is not None:
                    expected_value = self.connection.get(dependency_index_random_member)
                    if expected_value != field_value:
                        raise DependencyBrokenException()

                value_key = record.get_field_key(field_descriptor)
                dependency_indexes_update_list.append((dependency_key, value_key))

        return dependency_indexes_update_list

    def insert_value(self, record: TableRecord):
        # check all dependencies for all fields. raises exception if dependency is broken
        dependency_indexes_update_list = self.check_record_dependencies(record)

        # if no dependency is broken, update dependency indexes and insert values
        for dependency_key, value_key in dependency_indexes_update_list:
            self.connection.sadd(dependency_key, value_key)

        for field_descriptor in record.table_definition.get_normal_fields():
            value_key = record.get_field_key(field_descriptor)
            field_value = record.get_value_object(field_descriptor)

            if field_value is not None:
                self.connection.set(value_key, field_value.value)
