from dataclasses import dataclass
from typing import Iterable

from redis import Redis

from models import TableDefinition
from src.models import FieldValue, FieldDescriptor


class TableIterator:
    def __init__(self, connection: Redis, table: TableDefinition):
        self.connection = connection
        self.table = table

    @staticmethod
    def extract_key_identifier(key):
        return ":".join(key.split(":")[3:])

    # Iterating with SCAN
    # https://redis.io/docs/latest/commands/scan/
    def scan_generator(self):
        pattern = self.table.get_field_key_prefix() + ":*"

        cursor = 0
        while True:
            cursor, keys = self.connection.scan(cursor=cursor, match=pattern)
            for key in keys:
                yield self.extract_key_identifier(key)
            if cursor == 0:
                break

    # Iterating with KEYS
    # https://redis.io/docs/latest/commands/keys/
    def keys_generator(self):
        pattern = self.table.get_field_key_prefix() + ":*"

        for key in self.connection.keys(pattern=pattern):
            yield self.extract_key_identifier(key)

    # keeping set of all keys that belong to table
    def set_generator(self):
        for key in self.connection.smembers(self.table.get_table_key()):
            yield key

    def __iter__(self):
        # return self.scan_generator()
        # return self.keys_generator()
        return self.set_generator()


@dataclass
class SelectRecord:
    values: dict[TableDefinition, dict[FieldDescriptor, FieldValue]]


@dataclass
class JoinStatement:
    base_fields: list[tuple[TableDefinition, FieldDescriptor]]
    target_table: TableDefinition
    target_fields: list[FieldDescriptor]


@dataclass
class Selector:
    connection: Redis
    select_fields: dict[TableDefinition, list[FieldDescriptor]]
    from_table: TableDefinition
    join_statements: list[JoinStatement]

    def single_table_select(self, source_table: TableDefinition) -> Iterable[SelectRecord]:
        for key_identifier in TableIterator(self.connection, source_table):
            values = {source_table: dict()}

            for field in self.select_fields[source_table]:
                key_prefix = source_table.get_field_key_prefix(field)
                field_key = f"{key_prefix}:{key_identifier}"

                value: str = self.connection.get(field_key)
                if value is None:
                    values[source_table][field] = None
                else:
                    values[source_table][field] = FieldValue(value)

            yield SelectRecord(values)

    def nested_loops_select(self):
        def nested_loops_join(accumulator: Iterable[SelectRecord], target_records: Iterable[SelectRecord],
                              join_statement: JoinStatement):
            joined_records = []

            for accumulator_record in accumulator:
                for target_record in target_records:
                    check = True

                    for (base_table, base_field), target_field in zip(join_statement.base_fields,
                                                                      join_statement.target_fields):
                        if accumulator_record.values[base_table][base_field] != \
                                target_record.values[join_statement.target_table][target_field]:
                            check = False
                            break

                    if check:
                        joined_records.append(SelectRecord(values={**accumulator_record.values, **target_record.values}))

            return joined_records

        result = list(self.single_table_select(self.from_table))
        for join_statement in self.join_statements:
            target_table = list(self.single_table_select(join_statement.target_table))
            result = nested_loops_join(result, target_table, join_statement)

        return result

    def select(self):
        return self.nested_loops_select()
