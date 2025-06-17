from demo.setup import *


def insert_broken_dependency():
    core.insert(TableRecord(
        table_descriptor=table_person,
        values={
            person_name: FieldValue("Adam"),
            person_lastname: FieldValue("Charles"),
            person_city: FieldValue("Birmingham"),
            person_country: FieldValue("Poland")
        }
    ))


def insert_all():
    core.insert(jan_kowalski)
    # core.insert(anna_nowak)
    # core.insert(john_smith)
    # core.insert(charles_adams)

    # core.insert(poland)
    # core.insert(england)

    # insert_broken_dependency()



if __name__ == "__main__":
    insert_all()
