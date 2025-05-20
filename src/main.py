from dotenv import load_dotenv
import os
import redis

from engine import Core

from models import FieldValue, Tuple6NF
from src.models import FieldDescriptor, FunctionalDependency

load_dotenv()

redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]

def main():
    connection = redis.Redis(host=redis_host, port=redis_port,
                             decode_responses=True)
    connection.ping()  # throws redis.exceptions.ConnectionError if ping fails

    functional_dependencies = [
        FunctionalDependency(
            determinants=(
                FieldDescriptor("value_field1"),
                FieldDescriptor("value_field2")
            ),
            dependent=FieldDescriptor("value_field3")
        )
    ]

    core = Core(connection, functional_dependencies, clean_redis=True)

    other_primary_key_value1 = Tuple6NF(
        primary_key={
            FieldDescriptor("key_field1"): FieldValue("key1"),
            FieldDescriptor("key_field3"): FieldValue("key3"),
            FieldDescriptor("key_field2"): FieldValue("key2"),
        },
        field_descriptor=FieldDescriptor("value_field1"),
        field_value=FieldValue("val1")
    )
    core.insert_value(other_primary_key_value1)

    other_primary_key_value2 = Tuple6NF(
        primary_key={
            FieldDescriptor("key_field1"): FieldValue("key1"),
            FieldDescriptor("key_field3"): FieldValue("key3"),
            FieldDescriptor("key_field2"): FieldValue("key2"),
        },
        field_descriptor=FieldDescriptor("value_field2"),
        field_value=FieldValue("val2")
    )
    core.insert_value(other_primary_key_value2)

    other_primary_key_value3 = Tuple6NF(
        primary_key={
            FieldDescriptor("key_field1"): FieldValue("key1"),
            FieldDescriptor("key_field3"): FieldValue("key3"),
            FieldDescriptor("key_field2"): FieldValue("key2"),
        },
        field_descriptor=FieldDescriptor("value_field3"),
        field_value=FieldValue("val3")
    )
    core.insert_value(other_primary_key_value3)






    other_primary_key_value1 = Tuple6NF(
        primary_key={
            FieldDescriptor("key_field1"): FieldValue("other_key1"),
            FieldDescriptor("key_field3"): FieldValue("other_key3"),
            FieldDescriptor("key_field2"): FieldValue("other_key2"),
        },
        field_descriptor=FieldDescriptor("value_field1"),
        field_value=FieldValue("val1")
    )
    core.insert_value(other_primary_key_value1)

    other_primary_key_value2 = Tuple6NF(
        primary_key={
            FieldDescriptor("key_field1"): FieldValue("other_key1"),
            FieldDescriptor("key_field3"): FieldValue("other_key3"),
            FieldDescriptor("key_field2"): FieldValue("other_key2"),
        },
        field_descriptor=FieldDescriptor("value_field2"),
        field_value=FieldValue("val2")
    )
    core.insert_value(other_primary_key_value2)

    other_primary_key_value3 = Tuple6NF(
        primary_key={
            FieldDescriptor("key_field1"): FieldValue("other_key1"),
            FieldDescriptor("key_field3"): FieldValue("other_key3"),
            FieldDescriptor("key_field2"): FieldValue("other_key2"),
        },
        field_descriptor=FieldDescriptor("value_field3"),
        field_value=FieldValue("val3")
    )
    core.insert_value(other_primary_key_value3)


if __name__ == "__main__":
    main()
