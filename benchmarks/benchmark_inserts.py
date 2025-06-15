import sys
import os
import uuid
from time import perf_counter
import random

from dotenv import load_dotenv
import multiprocessing

from hash_db import Core, CoreConfiguration, TableDefinition, TableDescriptor, FieldDefinition, FieldDescriptor, \
    FunctionalDependency, MetadataStore, TableRecord, FieldValue, InsertType

load_dotenv()
redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]


def benchmark_worker(result_queue, worker_id, insert_type, rows_count, dependency_size):
    table = TableDefinition(
        table_descriptor=TableDescriptor("insert_benchmark_table"),
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
            ],
            config=CoreConfiguration(
                insert_type=insert_type
            )
        ),
        clean_redis=True
    )

    start = perf_counter()
    for _ in range(rows_count):
        # generate some data, which does not break functional dependencies
        dep_2_random = str(random.randint(1, dependency_size))

        field_1 = "field_1_" + dep_2_random
        field_2 = "field_2_" + dep_2_random
        field_3 = "field_3_" + dep_2_random

        primary_field_1 = str(uuid.uuid4())
        primary_field_2 = str(uuid.uuid4())

        core.insert(TableRecord(
            table_descriptor=TableDescriptor("insert_benchmark_table"),
            values={
                FieldDescriptor("primary_field_1"): FieldValue(primary_field_1),
                FieldDescriptor("primary_field_2"): FieldValue(primary_field_2),
                FieldDescriptor("field_1"): FieldValue(field_1),
                FieldDescriptor("field_2"): FieldValue(field_2),
                FieldDescriptor("field_3"): FieldValue(field_3),
            }
        ))
    time_spent = perf_counter() - start
    result_queue.put((worker_id, time_spent, core.metadata_store.insert_retries))


def benchmark_insert(insert_type: InsertType, rows_count, workers_count, dependency_size):
    # Just to clean redis before benchmarking
    Core(
        redis_host=redis_host,
        redis_port=redis_port,
        metadata_store=MetadataStore(
            tables=[
            ]
        ),
        clean_redis=True
    )

    rows_per_worker = rows_count // workers_count

    result_queue = multiprocessing.Queue()

    workers = []
    for i in range(workers_count):
        process = multiprocessing.Process(target=benchmark_worker, args=(result_queue, i, insert_type, rows_per_worker, dependency_size))
        workers.append(process)

    start = perf_counter()
    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()
    time_spent = perf_counter() - start

    worker_results = dict()
    total_retries = 0
    for _ in range(workers_count):
        res = result_queue.get()
        worker_results[res[0]] = res[1:]
        total_retries += res[2]

    print(f"Benchmark ran in {time_spent}s with {workers_count} workers and {total_retries} (avg: {total_retries/workers_count}) insert retries")
    # print("Worker details")
    # for i in range(workers_count):
    #     worker_time_spent, worker_retries = worker_results[i]
    #     print(f"Worker {i} ran in {worker_time_spent}s with {worker_retries} insert retries")


def main():
    insert_type = InsertType.REDIS_SCRIPT
    rows_count = 1000
    workers_count = 1
    dependency_size = 10

    if len(sys.argv) > 1:
        insert_type = InsertType[sys.argv[1].upper()]

    if len(sys.argv) > 2:
        rows_count = int(sys.argv[2])

    if len(sys.argv) > 3:
        workers_count = int(sys.argv[3])

    if len(sys.argv) > 4:
        dependency_size = int(sys.argv[4])

    benchmark_insert(insert_type, rows_count, workers_count, dependency_size)
    # for i in range(5):
    #     benchmark_insert(insert_type, rows_count, i * 2 + 1, dependency_size)


if __name__ == "__main__":
    main()
