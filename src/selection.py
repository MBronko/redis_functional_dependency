from typing import Iterable

from redis import Redis

from tools import get_key_generator
from config import JoiningAlgorithm
from selection_tools import TableIterator
from basic_models import FieldValue, FieldDescriptor, TableDescriptor, ResultRow, JoinStatement, Selector
from models import MetadataStore


def get_select_function(joining_algorithm: JoiningAlgorithm):
    return {
        JoiningAlgorithm.NESTED_LOOPS: nested_loops_select
    }[joining_algorithm]


def check_if_primary_key_joinable(metadata_store: MetadataStore, join_statement: JoinStatement):
    target_table = metadata_store.get_table_by_name(join_statement.target_table)
    target_primary_key_fields = target_table.get_primary_key_fields()

    if len(join_statement.target_fields) != len(target_primary_key_fields):
        return False

    for field in join_statement.target_fields:
        if field not in target_primary_key_fields:
            return False

    return True


def primary_key_join(conn: Redis, accumulator: Iterable[ResultRow], metadata_store: MetadataStore,
                     join_statement: JoinStatement, select_fields: list[FieldDescriptor]):
    print("Doing primary key join")
    joined_records = []

    target_table = metadata_store.get_table_by_name(join_statement.target_table)

    for accumulator_record in accumulator:
        primary_key_values: dict[FieldDescriptor, FieldValue] = dict()
        for (base_table, base_field), target_field in zip(join_statement.base_fields, join_statement.target_fields):
            primary_key_values[target_field] = accumulator_record.values[base_table.get_alias()][base_field]

        key_identifier = get_key_generator(metadata_store.config.key_policy)(primary_key_values)

        values: dict[FieldDescriptor, FieldValue] = {}
        for field in select_fields:
            key_prefix = target_table.get_field_key_prefix(field)
            key = f"{key_prefix}:{key_identifier}"

            values[field] = FieldValue(conn.get(key))

        joined_records.append(ResultRow(values={**accumulator_record.values, join_statement.target_table.get_alias(): values}))

    return joined_records

def single_table_select(conn: Redis, metadata_store: MetadataStore, fields: list[FieldDescriptor],
                        source_table_descriptor: TableDescriptor) -> Iterable[ResultRow]:
    source_table = metadata_store.get_table_by_name(source_table_descriptor)

    for key_identifier in TableIterator(conn, metadata_store, source_table_descriptor):
        values = {source_table_descriptor.get_alias(): dict()}

        for field in fields:
            key_prefix = source_table.get_field_key_prefix(field)
            field_key = f"{key_prefix}:{key_identifier}"

            value: str = conn.get(field_key)
            if value is None:
                values[source_table_descriptor.get_alias()][field] = None
            else:
                values[source_table_descriptor.get_alias()][field] = FieldValue(value)

        yield ResultRow(values)


def nested_loops_join(accumulator: Iterable[ResultRow], target_records: Iterable[ResultRow],
                      join_statement: JoinStatement):
    joined_records = []

    for accumulator_record in accumulator:
        for target_record in target_records:
            check = True

            for (base_table, base_field), target_field in zip(join_statement.base_fields,
                                                              join_statement.target_fields):
                if accumulator_record.values[base_table.get_alias()][base_field] != \
                        target_record.values[join_statement.target_table.get_alias()][target_field]:
                    check = False
                    break

            if check:
                joined_records.append(ResultRow(values={**accumulator_record.values, **target_record.values}))

    return joined_records


def nested_loops_select(conn: Redis, metadata_store: MetadataStore, selector: Selector) -> Iterable[ResultRow]:
    # selector.select_fields
    result = list(
        single_table_select(conn, metadata_store, selector.select_fields[selector.from_table], selector.from_table))
    for join_statement in selector.join_statements:
        if check_if_primary_key_joinable(metadata_store, join_statement):
            result = primary_key_join(conn, result, metadata_store, join_statement, selector.select_fields[join_statement.target_table])
        else:
            target_table = list(
                single_table_select(conn, metadata_store, selector.select_fields[join_statement.target_table],
                                    join_statement.target_table))
            result = nested_loops_join(result, target_table, join_statement)

    return result
