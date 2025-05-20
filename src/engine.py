from redis import Redis

from src.exceptions import DependencyBrokenException
from src.models import Tuple6NF, FunctionalDependency, FieldDescriptor



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

    def insert_value(self, value: Tuple6NF):
        primary_key_identifier = value.get_primary_key_identifier()
        value_key = value.field_descriptor.get_field_key(primary_key_identifier)

        dependencies = self.parsed_dependencies.get(value.field_descriptor, [])
        dependency_keys: dict[FunctionalDependency, str] = dict()

        for dependency in dependencies:
            dependency_key = dependency.get_key(self.connection, primary_key_identifier)
            dependency_keys[dependency] = dependency_key

            dependency_index_random_member = self.connection.srandmember(dependency_key)
            if dependency_index_random_member is not None:
                expected_value = self.connection.get(dependency_index_random_member)
                if expected_value != value.field_value.value:
                    raise DependencyBrokenException()

        for dependency in dependencies:
            dependency_key = dependency_keys[dependency]
            self.connection.sadd(dependency_key, value_key)

        self.connection.set(value_key, value.field_value.value)
