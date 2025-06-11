from dotenv import load_dotenv
import os
import pytest

from hash_db import Core, MetadataStore, TableDescriptor, TableDefinition, FieldDescriptor, FieldDefinition, FieldValue, \
    FunctionalDependency, TableRecord


@pytest.fixture()
def init_core():
    load_dotenv()
    redis_host = os.environ["REDIS_HOST"]
    redis_port = os.environ["REDIS_PORT"]

    table = TableDefinition(
        table_descriptor=TableDescriptor("test_table"),
        fields=[
            FieldDefinition(FieldDescriptor("primary_field_1"), primary_key=True),
            FieldDefinition(FieldDescriptor("field_1")),
            FieldDefinition(FieldDescriptor("field_2"))
        ],
        dependencies=[
            FunctionalDependency(
                determinants=[
                    FieldDescriptor("primary_field_1")
                ],
                dependent=FieldDescriptor("field_1")
            ),
        ]
    )

    core = Core(
        redis_host=redis_host,
        redis_port=redis_port,
        metadata_store=MetadataStore(
            tables=[
                table
            ]
        ),
        clean_redis=True
    )

    basic_record = TableRecord(
        table_descriptor=TableDescriptor("test_table"),
        values={
            FieldDescriptor("primary_field_1"): FieldValue("p1"),
            FieldDescriptor("field_1"): FieldValue("f1"),
            FieldDescriptor("field_2"): FieldValue("f2"),
        }
    )

    return core, basic_record


def test_values_are_deleted(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    key_identifier = '{"primary_field_1":"p1"}'

    assert core.conn.get(f'__value__:test_table:primary_field_1:{key_identifier}') == "p1"
    assert core.conn.get(f'__value__:test_table:field_1:{key_identifier}') == "f1"
    assert core.conn.get(f'__value__:test_table:field_2:{key_identifier}') == "f2"

    core.delete(basic_record)

    assert core.conn.get(f'__value__:test_table:primary_field_1:{key_identifier}') is None
    assert core.conn.get(f'__value__:test_table:field_1:{key_identifier}') is None
    assert core.conn.get(f'__value__:test_table:field_2:{key_identifier}') is None


def test_functional_dependency_indexes_are_cleared(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    key_identifier = '{"primary_field_1":"p1"}'

    assert core.conn.sismember('__dependency_index__:primary_field_1=>field_1:{"primary_field_1":"p1"}',
                               f'__value__:test_table:field_1:{key_identifier}')

    core.delete(basic_record)

    assert not core.conn.sismember('__dependency_index__:primary_field_1=>field_1:{"primary_field_1":"p1"}',
                                   f'__value__:test_table:field_1:{key_identifier}')


def test_row_cleared_from_table_index(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    key_identifier = '{"primary_field_1":"p1"}'

    assert core.conn.sismember('__table_keys__:test_table', key_identifier)

    core.delete(basic_record)

    assert not core.conn.sismember('__table_keys__:test_table', key_identifier)
