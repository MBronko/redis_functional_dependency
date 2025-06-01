from redis import Redis

from basic_models import Selector
from models import MetadataStore, TableRecord

from insertion import get_insert_function
from selection import get_select_function


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

    def select(self, selector: Selector):
        return get_select_function(self.metadata_store.config.joining_algorithm)(self.conn, self.metadata_store,
                                                                                 selector)
