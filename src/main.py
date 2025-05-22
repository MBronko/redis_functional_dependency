import random
import datetime
from dotenv import load_dotenv
import os
import redis

from engine import Core

from models import FieldValue, TableRecord
from models import FieldDescriptor, FunctionalDependency, TableDefinition

load_dotenv()

redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]


def main():
    connection = redis.Redis(host=redis_host, port=redis_port,
                             decode_responses=True)
    connection.ping()  # throws redis.exceptions.ConnectionError if ping fails

    field_name = FieldDescriptor("name", primary_key=True)
    field_lastname = FieldDescriptor("lastname", primary_key=True)
    field_gender = FieldDescriptor("gender")
    field_city = FieldDescriptor("city")
    field_country = FieldDescriptor("country")

    table_person = TableDefinition(
        name="person",
        fields=[
            field_name,
            field_lastname,
            field_gender,
            field_city,
            field_country
        ]
    )

    functional_dependencies = [
        FunctionalDependency(
            determinants=[
                field_name
            ],
            dependent=field_gender
        ),
        FunctionalDependency(
            determinants=[
                field_city
            ],
            dependent=field_country
        )
    ]

    core = Core(connection, functional_dependencies, clean_redis=True)

    core.insert_value(TableRecord(
        table_definition=table_person,
        values={
            field_name: FieldValue("Jan"),
            field_lastname: FieldValue("Kowalski"),
            field_gender: FieldValue("male"),
            field_city: FieldValue("Wroclaw"),
            field_country: FieldValue("Poland")
        }
    ))

    core.insert_value(TableRecord(
        table_definition=table_person,
        values={
            field_name: FieldValue("Anna"),
            field_lastname: FieldValue("Nowak"),
            field_gender: FieldValue("female"),
            field_city: FieldValue("Warszawa"),
            field_country: FieldValue("Poland")
        }
    ))


if __name__ == "__main__":
    main()
