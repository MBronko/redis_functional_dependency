from dotenv import load_dotenv
import os

from hash_db import *

load_dotenv()

table_person = TableDescriptor("person")

person_name = FieldDescriptor("name")
person_lastname = FieldDescriptor("lastname")
person_city = FieldDescriptor("city")
person_country = FieldDescriptor("country")

table_person_definition = TableDefinition(
    table_descriptor=table_person,
    fields=[
        FieldDefinition(person_name, primary_key=True),
        FieldDefinition(person_lastname, primary_key=True),
        FieldDefinition(person_city),
        FieldDefinition(person_country)
    ],
    dependencies=[
        FunctionalDependency(
            determinants=[
                person_city
            ],
            dependent=person_country
        )
    ]
)

table_country = TableDescriptor("country")
table_president = TableDescriptor("person", alias="president")

country_name = FieldDescriptor("name")
country_language = FieldDescriptor("language")
country_president_name = FieldDescriptor("president_name")
country_president_lastname = FieldDescriptor("president_lastname")

table_country_definition = TableDefinition(
    table_descriptor=table_country,
    fields=[
        FieldDefinition(country_name, primary_key=True),
        FieldDefinition(country_language),
        FieldDefinition(country_president_name),
        FieldDefinition(country_president_lastname)
    ]
)

core = Core(
    redis_host=os.environ["REDIS_HOST"],
    redis_port=os.environ["REDIS_PORT"],
    metadata_store=MetadataStore(
        tables=[
            table_person_definition,
            table_country_definition
        ],
        config=CoreConfiguration(
            insert_type=InsertType.REDIS_SCRIPT,
            delete_type=DeleteType.REDIS_SCRIPT,
            key_policy=KeyPolicyType.JSON
        )
    ),
    clean_redis=True
)

jan_kowalski = TableRecord(
    table_descriptor=table_person,
    values={
        person_name: FieldValue("Jan"),
        person_lastname: FieldValue("Kowalski"),
        person_city: FieldValue("Wroclaw"),
        person_country: FieldValue("Poland")
    }
)

anna_nowak = TableRecord(
    table_descriptor=table_person,
    values={
        person_name: FieldValue("Anna"),
        person_lastname: FieldValue("Nowak"),
        person_city: FieldValue("Warszawa"),
        person_country: FieldValue("Poland")
    }
)

john_smith = TableRecord(
    table_descriptor=table_person,
    values={
        person_name: FieldValue("John"),
        person_lastname: FieldValue("Smith"),
        person_city: FieldValue("London"),
        person_country: FieldValue("England")
    }
)

charles_adams = TableRecord(
    table_descriptor=table_person,
    values={
        person_name: FieldValue("Charles"),
        person_lastname: FieldValue("Adams"),
        person_city: FieldValue("Birmingham"),
        person_country: FieldValue("England")
    }
)

poland = TableRecord(
    table_descriptor=table_country,
    values={
        country_name: FieldValue("Poland"),
        country_language: FieldValue("Polish"),
        country_president_name: FieldValue("Jan"),
        country_president_lastname: FieldValue("Kowalski")
    }
)

england = TableRecord(
    table_descriptor=table_country,
    values={
        country_name: FieldValue("England"),
        country_language: FieldValue("English"),
        country_president_name: FieldValue("Charles"),
        country_president_lastname: FieldValue("Adams")
    }
)