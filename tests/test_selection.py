from dotenv import load_dotenv
import os
import pytest

from hash_db import Core, MetadataStore, TableDescriptor, TableDefinition, FieldDescriptor, FieldDefinition, FieldValue, \
    TableRecord, Selector, SelectorConditionEquals, SelectorConditionNot, JoinStatement


@pytest.fixture()
def init_core():
    load_dotenv()
    redis_host = os.environ["REDIS_HOST"]
    redis_port = os.environ["REDIS_PORT"]

    table1 = TableDefinition(
        table_descriptor=TableDescriptor("test_table_1"),
        fields=[
            FieldDefinition(FieldDescriptor("table1_primary_field_1"), primary_key=True),
            FieldDefinition(FieldDescriptor("table1_field_1")),
        ]
    )

    table2 = TableDefinition(
        table_descriptor=TableDescriptor("test_table_2"),
        fields=[
            FieldDefinition(FieldDescriptor("table2_primary_field_1"), primary_key=True),
            FieldDefinition(FieldDescriptor("table2_field_1")),
        ]
    )

    core = Core(
        redis_host=redis_host,
        redis_port=redis_port,
        metadata_store=MetadataStore(
            tables=[
                table1,
                table2
            ]
        ),
        clean_redis=True
    )

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table_1"),
        values={
            FieldDescriptor("table1_primary_field_1"): FieldValue("p1"),
            FieldDescriptor("table1_field_1"): FieldValue("f1"),
        }
    ))

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table_1"),
        values={
            FieldDescriptor("table1_primary_field_1"): FieldValue("p2"),
            FieldDescriptor("table1_field_1"): FieldValue("f2"),
        }
    ))

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table_1"),
        values={
            FieldDescriptor("table1_primary_field_1"): FieldValue("p3"),
            FieldDescriptor("table1_field_1"): FieldValue("f3"),
        }
    ))

    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table_1"),
        values={
            FieldDescriptor("table1_primary_field_1"): FieldValue("p4"),
            FieldDescriptor("table1_field_1"): FieldValue("f1"),
        }
    ))


    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table_2"),
        values={
            FieldDescriptor("table2_primary_field_1"): FieldValue("f1"),
            FieldDescriptor("table2_field_1"): FieldValue("f1 prim"),
        }
    ))


    core.insert(TableRecord(
        table_descriptor=TableDescriptor("test_table_2"),
        values={
            FieldDescriptor("table2_primary_field_1"): FieldValue("f2"),
            FieldDescriptor("table2_field_1"): FieldValue("f2 prim"),
        }
    ))

    return core


def test_equals_condition_is_respected(init_core):
    core = init_core

    selector = Selector(
        select_fields={
            TableDescriptor("test_table_1"): [
                FieldDescriptor("table1_primary_field_1"),
                FieldDescriptor("table1_field_1")
            ]
        },
        from_table=TableDescriptor("test_table_1"),
        join_statements=[],
        conditions=[
                SelectorConditionEquals(TableDescriptor("test_table_1"), FieldDescriptor("table1_field_1"), "f1")
        ]
    )

    results = list(core.select(selector))

    assert len(results) == 2
    for result in results:
        assert result.values["test_table_1"][FieldDescriptor("table1_primary_field_1")].value in ["p1", "p4"]
        assert result.values["test_table_1"][FieldDescriptor("table1_field_1")].value == "f1"


def test_condition_negation_is_respected(init_core):
    core = init_core

    selector = Selector(
        select_fields={
            TableDescriptor("test_table_1"): [
                FieldDescriptor("table1_primary_field_1"),
                FieldDescriptor("table1_field_1")
            ]
        },
        from_table=TableDescriptor("test_table_1"),
        join_statements=[],
        conditions=[
                SelectorConditionNot(SelectorConditionEquals(TableDescriptor("test_table_1"), FieldDescriptor("table1_field_1"), "f1"))
        ]
    )

    results = list(core.select(selector))

    assert len(results) == 2
    for result in results:
        assert result.values["test_table_1"][FieldDescriptor("table1_primary_field_1")].value in ["p2", "p3"]
        assert result.values["test_table_1"][FieldDescriptor("table1_field_1")].value != "f1"


def test_table_alias(init_core):
    core = init_core

    selector = Selector(
        select_fields={
            TableDescriptor("test_table_1", "alias_name_1"): [
                FieldDescriptor("table1_primary_field_1"),
                FieldDescriptor("table1_field_1")
            ]
        },
        from_table=TableDescriptor("test_table_1", "alias_name_1"),
        join_statements=[],
        conditions=[]
    )

    results = list(core.select(selector))

    assert len(results) == 4
    for result in results:
        assert "alias_name_1" in result.values

def test_cross_product_with_itself(init_core):
    core = init_core

    selector = Selector(
        select_fields={
            TableDescriptor("test_table_1", "alias_name_1"): [
                FieldDescriptor("table1_primary_field_1"),
                FieldDescriptor("table1_field_1")
            ],
            TableDescriptor("test_table_1", "alias_name_2"): [
                FieldDescriptor("table1_primary_field_1"),
                FieldDescriptor("table1_field_1")
            ]
        },
        from_table=TableDescriptor("test_table_1", "alias_name_1"),
        join_statements=[
            JoinStatement(
                base_fields=[],
                target_table=TableDescriptor("test_table_1", "alias_name_2"),
                target_fields=[]
            )
        ],
        conditions=[]
    )

    results = list(core.select(selector))

    assert len(results) == 16
    for result in results:
        assert "alias_name_1" in result.values
        assert "alias_name_2" in result.values


def test_simple_join(init_core):
    core = init_core

    selector = Selector(
        select_fields={
            TableDescriptor("test_table_1"): [
                FieldDescriptor("table1_primary_field_1")
            ],
            TableDescriptor("test_table_2"): [
                FieldDescriptor("table2_field_1")
            ]
        },
        from_table=TableDescriptor("test_table_1"),
        join_statements=[
            JoinStatement(
                base_fields=[(TableDescriptor("test_table_1"), FieldDescriptor("table1_field_1"))],
                target_table=TableDescriptor("test_table_2"),
                target_fields=[FieldDescriptor("table2_primary_field_1")]
            )
        ],
        conditions=[]
    )

    results = list(core.select(selector))
    assert len(results) == 3

    check = dict()

    for result in results:
        t1 = result.values["test_table_1"][FieldDescriptor("table1_primary_field_1")].value
        t2 = result.values["test_table_2"][FieldDescriptor("table2_field_1")].value
        check[(t1, t2)] = True

    for expected in [("p1", "f1 prim"), ("p2", "f2 prim"), ("p4", "f1 prim")]:
        assert check.get(expected, False)
