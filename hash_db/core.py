from redis import Redis

from hash_db.models import Selector, MetadataStore, TableRecord

from hash_db.extensions.insertion import get_insert_function
from hash_db.extensions.selection import get_select_function
from hash_db.tools.selection_tools import select_projection
from hash_db.extensions.deletion import get_delete_function


class Core:
    def __init__(self, conn: Redis, metadata_store: MetadataStore,
                 clean_redis=False):
        self.conn: Redis = conn
        self.metadata_store = metadata_store

        if clean_redis:
            self.clean_redis()

    def clean_redis(self) -> None:
        self.conn.flushdb()

    def insert(self, record: TableRecord):
        return get_insert_function(self.metadata_store.config.insert_type)(self.conn, self.metadata_store, record)

    def delete(self, record: TableRecord):
        return get_delete_function(self.metadata_store.config.delete_type)(self.conn, self.metadata_store, record)

    def select(self, selector: Selector):
        results = get_select_function(self.metadata_store.config.joining_algorithm)(self.conn, self.metadata_store,
                                                                                    selector)

        for result_row in results:
            yield select_projection(selector, result_row)
