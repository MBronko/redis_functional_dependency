import sys
import os
import uuid
from time import perf_counter
import random

from dotenv import load_dotenv

from hash_db import Core, CoreConfiguration, TableDefinition, TableDescriptor, FieldDefinition, FieldDescriptor, \
    MetadataStore, TableRecord, FieldValue, Selector, JoinStatement

load_dotenv()
redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]

table1 = TableDefinition(
    table_descriptor=TableDescriptor("select_benchmark_table_1"),
    fields=[
        FieldDefinition(FieldDescriptor("table1_primary_field_1"), primary_key=True),
        FieldDefinition(FieldDescriptor("table1_field_1")),
        FieldDefinition(FieldDescriptor("table1_field_2")),
    ],
    dependencies=[
    ]
)

table2 = TableDefinition(
    table_descriptor=TableDescriptor("select_benchmark_table_2"),
    fields=[
        FieldDefinition(FieldDescriptor("table2_primary_field_1"), primary_key=True),
        FieldDefinition(FieldDescriptor("table2_field_1")),
        FieldDefinition(FieldDescriptor("table2_field_2")),
    ],
    dependencies=[
    ]
)


def populate_database(core, table1_size, table2_size):
    for _ in range(table1_size):
        table2_random_index = str(random.randint(0, table2_size - 1))

        table1_primary_field_1 = str(uuid.uuid4())
        table1_field_1 = table2_random_index
        table1_field_2 = table2_random_index

        core.insert(TableRecord(
            table_descriptor=TableDescriptor("select_benchmark_table_1"),
            values={
                FieldDescriptor("table1_primary_field_1"): FieldValue(table1_primary_field_1),
                FieldDescriptor("table1_field_1"): FieldValue(table1_field_1),
                FieldDescriptor("table1_field_2"): FieldValue(table1_field_2),
            }
        ))

    for i in range(table2_size):
        table2_primary_field_1 = str(i)
        table2_field_1 = str(i)
        table2_field_2 = str(uuid.uuid4())

        core.insert(TableRecord(
            table_descriptor=TableDescriptor("select_benchmark_table_2"),
            values={
                FieldDescriptor("table2_primary_field_1"): FieldValue(table2_primary_field_1),
                FieldDescriptor("table2_field_1"): FieldValue(table2_field_1),
                FieldDescriptor("table2_field_2"): FieldValue(table2_field_2),
            }
        ))


def benchmark_select(table1_size, table2_size, select_count):
    core = Core(
        redis_host=redis_host,
        redis_port=redis_port,
        metadata_store=MetadataStore(
            tables=[
                table1,
                table2
            ],
            config=CoreConfiguration()
        ),
        clean_redis=True
    )

    populate_database(core, table1_size, table2_size)

    selector = Selector(
        select_fields={
            TableDescriptor("select_benchmark_table_1"): [
                FieldDescriptor("table1_primary_field_1"),
            ],
            TableDescriptor("select_benchmark_table_2"): [
                FieldDescriptor("table2_field_2"),
            ]
        },
        from_table=TableDescriptor("select_benchmark_table_1"),
        join_statements=[
            JoinStatement(
                base_fields=[(TableDescriptor("select_benchmark_table_1"), FieldDescriptor("table1_field_1"))],
                target_table=TableDescriptor("select_benchmark_table_2"),
                target_fields=[FieldDescriptor("table2_primary_field_1")]
            )
        ],
        conditions=[]
    )

    start = perf_counter()
    result = []
    for i in range(select_count):
        result = list(core.select(selector))
    time_spent = perf_counter() - start

    print(
        f"Benchmark ran in {time_spent}s doing {select_count} selects. table1 size = {table1_size}, table2 size = {table2_size}, result size = {len(result)}")


def main():
    table1_size = 1000
    table2_size = 100
    select_count = 100

    if len(sys.argv) > 1:
        table1_size = int(sys.argv[1])

    if len(sys.argv) > 2:
        table2_size = int(sys.argv[2])

    if len(sys.argv) > 3:
        select_count = int(sys.argv[3])

    benchmark_select(table1_size, table2_size, select_count)

    # for i in range(1, 6):
    #     benchmark_select(table1_size, i * 1000, select_count)

    # for i in range(1, 6):
    #     benchmark_select(i * 1000, table2_size, select_count)


if __name__ == "__main__":
    main()
