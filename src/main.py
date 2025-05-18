from dotenv import load_dotenv
import os
import redis
from redis import Redis
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TextField
from redis.commands.search.query import Query

load_dotenv()

redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]

table_name = "table1"

# field1, field3 implies field2 and field4
functional_dependency = (
    ("field1", "field3"),
    ("field2", "field4")
)

index_name = f"functional_dependency:{":".join(functional_dependency[0])}"
index_prefix = f"{table_name}:"

values_to_add = [
    {
        "field1": "1A",
        "field2": "2A",
        "field3": "3A",
        "field4": "4A",
        "field5": "5A",
    },
    {
        "field1": "1B",
        "field2": "2B",
        "field3": "3B",
        "field4": "4B",
        "field5": "5B",
    },
    {
        "field1": "1C",
        "field2": "2C",
        "field3": "3C",
        "field4": "4C",
        "field5": "5C",
    },
    {
        "field1": "1A", # same dependency values as #1 value, different field5
        "field2": "2A",
        "field3": "3A",
        "field4": "4A",
        "field5": "5D",
    },
    {
        "field1": "1A", # dependency violation
        "field2": "2A",
        "field3": "3A",
        "field4": "4D",
        "field5": "5E",
    },
    {
        "field1": "1A",
        "field2": "2A",
        "field3": "3F",
        "field4": "4F",
        "field5": "5F",
    }
]


def clean_redis(connection: Redis):
    connection.flushdb()

def create_index(connection: Redis):
    print(f"creating index {index_name}")

    index_definition = IndexDefinition(index_type=IndexType.HASH, prefix=[index_prefix])
    index_schema = ((TextField(field) for field in functional_dependency[0]))

    connection.ft(index_name).create_index(index_schema, definition=index_definition)


def check_dependency(connection: Redis, row: dict[str, str]):
    query_string = " ".join(f"@{field}:{row[field]}" for field in functional_dependency[0])
    print(f"query for row {row}: {query_string}")

    query = Query(query_string)

    query.paging(0, 1)
    query.return_fields(*functional_dependency[1])

    res = connection.ft(index_name).search(query)

    if res.total > 0:
        dependency_row = res.docs[0]

        for field in functional_dependency[1]:
            if row[field] != dependency_row[field]:
                return False, f"Functional dependency violation {row} vs {dependency_row}"

    return True, ""


def insert_value(connection: Redis, row: dict[str, str], id: int):
    success, msg = check_dependency(connection, row)
    if not success:
        raise ValueError(msg)

    key = f"{index_prefix}{id}"
    connection.hset(key, mapping=row)


def main():
    connection = redis.Redis(host=redis_host, port=redis_port,
                             decode_responses=True)
    connection.ping()  # throws redis.exceptions.ConnectionError if ping fails

    clean_redis(connection)
    create_index(connection)

    for idx, value in enumerate(values_to_add):
        try:
            insert_value(connection, value, idx)
        except ValueError as e:
            print(e)


if __name__ == "__main__":
    main()
