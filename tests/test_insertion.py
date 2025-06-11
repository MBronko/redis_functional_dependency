from dotenv import load_dotenv
import os
import pytest

from hash_db import Core, MetadataStore, TableDescriptor, TableDefinition, FieldDescriptor, FieldDefinition, FieldValue, \
    FunctionalDependency, TableRecord
from hash_db.exceptions import DependencyBrokenException


@pytest.fixture()
def init_core():
    load_dotenv()
    redis_host = os.environ["REDIS_HOST"]
    redis_port = os.environ["REDIS_PORT"]

    table = TableDefinition(
        table_descriptor=TableDescriptor("test_table"),
        fields=[
            FieldDefinition(FieldDescriptor("primary_field_1"), primary_key=True),
            FieldDefinition(FieldDescriptor("primary_field_2"), primary_key=True),
            FieldDefinition(FieldDescriptor("field_1")),
            FieldDefinition(FieldDescriptor("field_2")),
            FieldDefinition(FieldDescriptor("field_3"))
        ],
        dependencies=[
            FunctionalDependency(
                determinants=[
                    FieldDescriptor("primary_field_1")
                ],
                dependent=FieldDescriptor("field_1")
            ),
            FunctionalDependency(
                determinants=[
                    FieldDescriptor("field_1"),
                    FieldDescriptor("field_2"),
                ],
                dependent=FieldDescriptor("field_3")
            )
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
            FieldDescriptor("primary_field_2"): FieldValue("p2"),
            FieldDescriptor("field_1"): FieldValue("f1"),
            FieldDescriptor("field_2"): FieldValue("f2"),
            FieldDescriptor("field_3"): FieldValue("f3"),
        }
    )

    return core, basic_record


def test_values_are_inserted(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    key_identifier = '{"primary_field_1":"p1","primary_field_2":"p2"}'

    assert core.conn.get(f'__value__:test_table:primary_field_1:{key_identifier}') == "p1"
    assert core.conn.get(f'__value__:test_table:primary_field_2:{key_identifier}') == "p2"
    assert core.conn.get(f'__value__:test_table:field_1:{key_identifier}') == "f1"
    assert core.conn.get(f'__value__:test_table:field_2:{key_identifier}') == "f2"
    assert core.conn.get(f'__value__:test_table:field_3:{key_identifier}') == "f3"


def test_functional_dependency_indexes_are_set(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    key_identifier = '{"primary_field_1":"p1","primary_field_2":"p2"}'

    assert core.conn.sismember('__dependency_index__:primary_field_1=>field_1:{"primary_field_1":"p1"}',
                               f'__value__:test_table:field_1:{key_identifier}')
    assert core.conn.sismember('__dependency_index__:field_1&field_2=>field_3:{"field_1":"f1","field_2":"f2"}',
                               f'__value__:test_table:field_3:{key_identifier}')


def test_row_added_to_table_index(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    key_identifier = '{"primary_field_1":"p1","primary_field_2":"p2"}'

    assert core.conn.sismember('__table_keys__:test_table', key_identifier)


def test_functional_dependency_are_respected_1(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    with pytest.raises(DependencyBrokenException):
        core.insert(TableRecord(
            table_descriptor=TableDescriptor("test_table"),
            values={
                FieldDescriptor("primary_field_1"): FieldValue("p1"),
                FieldDescriptor("primary_field_2"): FieldValue("p2 prim"),
                FieldDescriptor("field_1"): FieldValue("f1 prim"),
                FieldDescriptor("field_2"): FieldValue("f2"),
                FieldDescriptor("field_3"): FieldValue("f3"),
            }
        ))


def test_functional_dependency_are_respected_2(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    with pytest.raises(DependencyBrokenException):
        core.insert(TableRecord(
            table_descriptor=TableDescriptor("test_table"),
            values={
                FieldDescriptor("primary_field_1"): FieldValue("p1 prim"),
                FieldDescriptor("primary_field_2"): FieldValue("p2"),
                FieldDescriptor("field_1"): FieldValue("f1"),
                FieldDescriptor("field_2"): FieldValue("f2"),
                FieldDescriptor("field_3"): FieldValue("f3 prim"),
            }
        ))


def test_functional_dependency_with_equal_value_passes(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table"),
        values={
            FieldDescriptor("primary_field_1"): FieldValue("p1 prim"),
            FieldDescriptor("primary_field_2"): FieldValue("p2"),
            FieldDescriptor("field_1"): FieldValue("f1"),
            FieldDescriptor("field_2"): FieldValue("f2"),
            FieldDescriptor("field_3"): FieldValue("f3"),
        }
    ))


def test_multiple_inserts_without_breaking_functional_dependency(init_core):
    core, basic_record = init_core

    core.insert(basic_record)

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table"),
        values={
            FieldDescriptor("primary_field_1"): FieldValue("p1 prim"),
            FieldDescriptor("primary_field_2"): FieldValue("p2"),
            FieldDescriptor("field_1"): FieldValue("f1 prim"),
            FieldDescriptor("field_2"): FieldValue("f2"),
            FieldDescriptor("field_3"): FieldValue("f3 prim"),
        }
    ))

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table"),
        values={
            FieldDescriptor("primary_field_1"): FieldValue("p1 prim2"),
            FieldDescriptor("primary_field_2"): FieldValue("p2"),
            FieldDescriptor("field_1"): FieldValue("f1"),
            FieldDescriptor("field_2"): FieldValue("f2 prim"),
            FieldDescriptor("field_3"): FieldValue("f3 prim2"),
        }
    ))
