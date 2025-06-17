from demo.setup import *
from demo.inserting import insert_all, insert_broken_dependency


def delete_demo():
    core.insert(jan_kowalski)
    core.delete(jan_kowalski)

    # insert_all()
    # core.delete(charles_adams)
    # insert_broken_dependency()


if __name__ == "__main__":
    delete_demo()
