from dotenv import load_dotenv
import os
import redis

from engine import Core

from models import FieldValue, TableRecord
from models import FieldDescriptor, FunctionalDependency, TableDefinition
from selection import Selector
from src.selection import JoinStatement

load_dotenv()

redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]


def main():
    connection = redis.Redis(host=redis_host, port=redis_port,
                             decode_responses=True)
    connection.ping()  # throws redis.exceptions.ConnectionError if ping fails

    person_field_name = FieldDescriptor("name", primary_key=True)
    person_field_lastname = FieldDescriptor("lastname", primary_key=True)
    person_field_gender = FieldDescriptor("gender")
    person_field_city = FieldDescriptor("city")
    person_field_country = FieldDescriptor("country")

    table_person = TableDefinition(
        name="person",
        fields=[
            person_field_name,
            person_field_lastname,
            person_field_gender,
            person_field_city,
            person_field_country
        ]
    )

    functional_dependencies = [
        FunctionalDependency(
            determinants=[
                person_field_name
            ],
            dependent=person_field_gender
        ),
        FunctionalDependency(
            determinants=[
                person_field_city
            ],
            dependent=person_field_country
        )
    ]

    core = Core(connection, functional_dependencies, clean_redis=True)

    core.insert_value(TableRecord(
        table_definition=table_person,
        values={
            person_field_name: FieldValue("Jan"),
            person_field_lastname: FieldValue("Kowalski"),
            person_field_gender: FieldValue("male"),
            person_field_city: FieldValue("Wroclaw"),
            person_field_country: FieldValue("Poland")
        }
    ))

    core.insert_value(TableRecord(
        table_definition=table_person,
        values={
            person_field_name: FieldValue("Anna"),
            person_field_lastname: FieldValue("Nowak"),
            person_field_gender: FieldValue("female"),
            person_field_city: FieldValue("Warszawa"),
            person_field_country: FieldValue("Poland")
        }
    ))

    core.insert_value(TableRecord(
        table_definition=table_person,
        values={
            person_field_name: FieldValue("John"),
            person_field_lastname: FieldValue("Smith"),
            person_field_gender: FieldValue("male"),
            person_field_city: FieldValue("London"),
            person_field_country: FieldValue("England")
        }
    ))

    country_field_name = FieldDescriptor("name", primary_key=True)
    country_field_language = FieldDescriptor("language")

    table_country = TableDefinition(
        name="country",
        fields=[
            country_field_name,
            country_field_language
        ]
    )

    core.insert_value(TableRecord(
        table_definition=table_country,
        values={
            country_field_name: FieldValue("Poland"),
            country_field_language: FieldValue("Polish")
        }
    ))

    core.insert_value(TableRecord(
        table_definition=table_country,
        values={
            country_field_name: FieldValue("England"),
            country_field_language: FieldValue("English")
        }
    ))

    selector = Selector(
        connection=connection,
        select_fields={
            table_person: [
                person_field_name,
                person_field_lastname,
                person_field_country
            ],
            table_country: [
                country_field_name,
                country_field_language
            ]
        },
        from_table=table_person,
        join_statements=[
            JoinStatement(
                base_fields=[
                    (table_person, person_field_country)
                ],
                target_table=table_country,
                target_fields=[
                    country_field_name
                ]
            )
        ]
    )

    for record in selector.nested_loops_select():
        print(record.values.values())


if __name__ == "__main__":
    main()
