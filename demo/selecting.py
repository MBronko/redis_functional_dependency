from demo.setup import *
from inserting import insert_all


def select_demo():
    insert_all()

    selector = Selector(
        select_fields={
            table_person: [
                person_name,
                person_lastname
            ],
            table_country: [
                country_name,
                country_language
            ],
            table_president: [
                person_name,
                person_lastname,
                person_city
            ]
        },
        from_table=table_person,
        join_statements=[
            JoinStatement(
                base_fields=[
                    (table_person, person_country)
                ],
                target_table=table_country,
                target_fields=[
                    country_name
                ]
            ),
            JoinStatement(
                base_fields=[
                    (table_country, country_president_name),
                    (table_country, country_president_lastname)
                ],
                target_table=table_president,
                target_fields=[
                    person_name,
                    person_lastname
                ]
            )
        ],
        conditions=[
            SelectorConditionNot(
                SelectorConditionEquals(table_person, person_city, "Wroclaw")
            )
            # SelectorConditionEquals(table_person, person_city, "Wroclaw")
        ]
    )

    for record in core.select(selector):
        for table, values in record.values.items():
            for col, val in values.items():
                print(f"{table}.{col.name} = {val.value}", end=", ")
        print()


if __name__ == "__main__":
    select_demo()
