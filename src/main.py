from dotenv import load_dotenv
import os
import redis

from config import CoreConfiguration, InsertType, DeleteType, KeyPolicyType, ListRecordsType, JoiningAlgorithm
from engine import Core

from basic_models import TableDescriptor, FieldDefinition, FieldValue, FieldDescriptor, Selector, JoinStatement, \
    SelectorConditionEquals, SelectorConditionNot
from models import FunctionalDependency, TableDefinition, TableRecord, MetadataStore

# from selection import Selector, JoinStatement

load_dotenv()

redis_host = os.environ["REDIS_HOST"]
redis_port = os.environ["REDIS_PORT"]


def main():
    conn = redis.Redis(host=redis_host, port=redis_port,
                       decode_responses=True)
    conn.ping()  # throws redis.exceptions.ConnectionError if ping fails

    person_field_name = FieldDescriptor("name")
    person_field_lastname = FieldDescriptor("lastname")
    person_field_gender = FieldDescriptor("gender")
    person_field_city = FieldDescriptor("city")
    person_field_country = FieldDescriptor("country")

    table_person = TableDescriptor("person")

    table_person_definition = TableDefinition(
        table_descriptor=table_person,
        fields=[
            FieldDefinition(person_field_name, primary_key=True),
            FieldDefinition(person_field_lastname, primary_key=True),
            FieldDefinition(person_field_gender),
            FieldDefinition(person_field_city),
            FieldDefinition(person_field_country)
        ],
        dependencies=[
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
    )

    country_field_name = FieldDescriptor("name")
    country_field_language = FieldDescriptor("language")
    country_field_president_name = FieldDescriptor("president_name")
    country_field_president_lastname = FieldDescriptor("president_lastname")

    table_country = TableDescriptor("country")
    table_country_definition = TableDefinition(
        table_descriptor=table_country,
        fields=[
            FieldDefinition(country_field_name, primary_key=True),
            FieldDefinition(country_field_language),
            FieldDefinition(country_field_president_name),
            FieldDefinition(country_field_president_lastname)
        ]
    )

    core = Core(
        conn=conn,
        metadata_store=MetadataStore(
            tables=[
                table_person_definition,
                table_country_definition
            ],
            config=CoreConfiguration(
                insert_type=InsertType.REDIS_SCRIPT,
                delete_type=DeleteType.REDIS_SCRIPT,
                key_policy=KeyPolicyType.JSON,
                list_records_type=ListRecordsType.SET,
                joining_algorithm=JoiningAlgorithm.NESTED_LOOPS
            )
        ),
        clean_redis=True
    )

    core.insert(TableRecord(
        table_descriptor=table_person,
        values={
            person_field_name: FieldValue("Jan"),
            person_field_lastname: FieldValue("Kowalski"),
            person_field_gender: FieldValue("male"),
            person_field_city: FieldValue("Wroclaw"),
            person_field_country: FieldValue("Poland")
        }
    ))

    core.insert(TableRecord(
        table_descriptor=table_person,
        values={
            person_field_name: FieldValue("Anna"),
            person_field_lastname: FieldValue("Nowak"),
            person_field_gender: FieldValue("female"),
            person_field_city: FieldValue("Warszawa"),
            person_field_country: FieldValue("Poland")
        }
    ))

    john_smith_from_london = TableRecord(
        table_descriptor=table_person,
        values={
            person_field_name: FieldValue("John"),
            person_field_lastname: FieldValue("Smith"),
            person_field_gender: FieldValue("male"),
            person_field_city: FieldValue("London"),
            person_field_country: FieldValue("England")
        }
    )
    core.insert(john_smith_from_london)

    core.insert(TableRecord(
        table_descriptor=table_person,
        values={
            person_field_name: FieldValue("Charles"),
            person_field_lastname: FieldValue("Adams"),
            person_field_gender: FieldValue("male"),
            person_field_city: FieldValue("Birmingham"),
            person_field_country: FieldValue("England")
        }
    ))

    core.insert(TableRecord(
        table_descriptor=table_country,
        values={
            country_field_name: FieldValue("Poland"),
            country_field_language: FieldValue("Polish"),
            country_field_president_name: FieldValue("Jan"),
            country_field_president_lastname: FieldValue("Kowalski")
        }
    ))

    england = TableRecord(
        table_descriptor=table_country,
        values={
            country_field_name: FieldValue("England"),
            country_field_language: FieldValue("English"),
            country_field_president_name: FieldValue("Charles"),
            country_field_president_lastname: FieldValue("Adams")
        }
    )

    core.insert(england)

    table_president = TableDescriptor("person", alias="president")

    selector = Selector(
        select_fields={
            table_person: [
                person_field_name,
                person_field_lastname
            ],
            table_country: [
                country_field_name,
                country_field_language
            ],
            table_president: [
                person_field_name,
                person_field_lastname,
                person_field_city
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
            ),
            JoinStatement(
                base_fields=[
                    (table_country, country_field_president_name),
                    (table_country, country_field_president_lastname)
                ],
                target_table=table_president,
                target_fields=[
                    person_field_name,
                    person_field_lastname
                ]
            )
        ],
        conditions=[
            SelectorConditionNot(
                SelectorConditionEquals(table_person, person_field_gender, "female")
            )
            # SelectorConditionEquals(table_person, person_field_gender, "female")
        ]
    )

    for record in core.select(selector):
        for table, values in record.values.items():
            for col, val in values.items():
                print(f"{table}.{col.name} = {val.value}", end=", ")
        print()

    print("Deleting England")
    core.delete(england)

    # we need to delete John Smith from london, england
    core.delete(john_smith_from_london)

    # creating Karol Krawczyk from london, poland
    core.insert(TableRecord(
        table_descriptor=table_person,
        values={
            person_field_name: FieldValue("Karol"),
            person_field_lastname: FieldValue("Krawczyk"),
            person_field_gender: FieldValue("male"),
            person_field_city: FieldValue("London"),
            person_field_country: FieldValue("Poland")
        }
    ))

    for record in core.select(selector):
        for table, values in record.values.items():
            for col, val in values.items():
                print(f"{table}.{col.name} = {val.value}", end=", ")
        print()


if __name__ == "__main__":
    main()
