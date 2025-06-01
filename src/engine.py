import redis
from redis import Redis
from redis.client import Pipeline

from exceptions import DependencyBrokenException
from models import TableRecord, FunctionalDependency, FieldDescriptor


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

    def check_record_dependencies(self, pipeline: Redis | Pipeline, record: TableRecord):
        dependency_indexes_update_list: list[tuple[str, str]] = []

        for field_descriptor in record.table_definition.get_all_fields():
            field_value = record.get_value(field_descriptor)
            value_key = record.get_field_key(field_descriptor)

            # ensure value will not be changed until transaction executed
            pipeline.watch(value_key)

            for dependency in self.parsed_dependencies.get(field_descriptor, []):
                dependency_key = dependency.get_key(record)

                # ensure dependency index will not be changed until transaction executed
                pipeline.watch(dependency_key)

                dependency_index_random_member = self.connection.srandmember(dependency_key)

                if dependency_index_random_member is not None:
                    expected_value = self.connection.get(dependency_index_random_member)
                    if expected_value != field_value:
                        raise DependencyBrokenException()

                dependency_indexes_update_list.append((dependency_key, value_key))

        return dependency_indexes_update_list

    def simple_insert_value(self, record: TableRecord):
        # check all dependencies for all fields. raises exception if dependency is broken
        dependency_indexes_update_list = self.check_record_dependencies(self.connection, record)

        # if no dependency is broken, update dependency indexes and insert values
        for dependency_key, value_key in dependency_indexes_update_list:
            self.connection.sadd(dependency_key, value_key)

        table_key = record.table_definition.get_table_key()
        key_identifier = record.get_primary_key_identifier()
        self.connection.sadd(table_key, key_identifier)

        for field_descriptor in record.table_definition.get_all_fields():
            value_key = record.get_field_key(field_descriptor)
            field_value = record.get_value_object(field_descriptor)

            if field_value is not None:
                self.connection.set(value_key, field_value.value)

    def insert_value_transaction(self, record: TableRecord):
        with self.connection.pipeline() as pipeline:
            while True:
                try:
                    # check all dependencies for all fields. raises exception if dependency is broken
                    dependency_indexes_update_list = self.check_record_dependencies(pipeline, record)

                    # start transaction
                    pipeline.multi()

                    # if no dependency is broken, update dependency indexes and insert values
                    for dependency_key, value_key in dependency_indexes_update_list:
                        pipeline.sadd(dependency_key, value_key)

                    table_key = record.table_definition.get_table_key()
                    key_identifier = record.get_primary_key_identifier()
                    self.connection.sadd(table_key, key_identifier)

                    for field_descriptor in record.table_definition.get_all_fields():
                        value_key = record.get_field_key(field_descriptor)
                        field_value = record.get_value_object(field_descriptor)

                        if field_value is not None:
                            pipeline.set(value_key, field_value.value)

                    pipeline.execute()
                    break

                except redis.WatchError:
                    print("WatchError: retrying transaction")

    def insert_using_lua_script(self, record: TableRecord):
        keys = []
        args = []

        all_fields = record.table_definition.get_all_fields()
        args.append(len(all_fields))

        for field_descriptor in all_fields:
            field_value = record.get_value(field_descriptor)
            value_key = record.get_field_key(field_descriptor)

            args.append(field_value)
            keys.append(value_key)

            dependencies = self.parsed_dependencies.get(field_descriptor, [])

            args.append(len(dependencies))

            for dependency in dependencies:
                dependency_key = dependency.get_key(record)
                keys.append(dependency_key)

        # TODO: write a lua script
        lua_check_and_set = """
        local current = redis.call("GET", KEYS[1])
        if current == ARGV[1] then
            redis.call("SET", KEYS[1], ARGV[2])
            return "OK"
        else
            return nil
        end
        """

        check_set_script = self.connection.register_script(lua_check_and_set)
        res = check_set_script(keys=keys, args=args)
        if res:
            print("Redis script executed successfully")
        else:
            raise DependencyBrokenException()

    def insert_value(self, record: TableRecord):
        return self.insert_value_transaction(record)
        # return self.simple_insert_value(record)
