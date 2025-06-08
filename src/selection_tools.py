from redis import Redis

from models import TableDefinition
from models import MetadataStore
from basic_models import TableDescriptor, Selector, ResultRow
from config import ListRecordsType


class TableIterator:
    conn: Redis
    table: TableDefinition
    metadata_store: MetadataStore

    def __init__(self, conn: Redis, metadata_store: MetadataStore, table: TableDescriptor):
        self.conn = conn
        self.table = metadata_store.get_table_by_name(table)
        self.metadata_store = metadata_store

    @staticmethod
    def extract_key_identifier(key):
        return ":".join(key.split(":")[3:])

    # Iterating with SCAN
    # https://redis.io/docs/latest/commands/scan/
    def scan_generator(self):
        pattern = self.table.get_field_key_prefix() + ":*"

        cursor = 0
        while True:
            cursor, keys = self.conn.scan(cursor=cursor, match=pattern)
            for key in keys:
                yield self.extract_key_identifier(key)
            if cursor == 0:
                break

    # Iterating with KEYS
    # https://redis.io/docs/latest/commands/keys/
    def keys_generator(self):
        pattern = self.table.get_field_key_prefix() + ":*"

        for key in self.conn.keys(pattern=pattern):
            yield self.extract_key_identifier(key)

    # keeping set of all keys that belong to table
    def set_generator(self):
        for key in self.conn.smembers(self.table.get_table_key()):
            yield key

    def __iter__(self):
        return {
            ListRecordsType.SCAN: self.scan_generator,
            ListRecordsType.KEYS: self.keys_generator,
            ListRecordsType.SET: self.set_generator
        }[self.metadata_store.config.list_records_type]()


def select_projection(selector: Selector, result_row: ResultRow) -> ResultRow:
    projected_values = dict()

    for table, fields in selector.select_fields.items():
        projected_values[table.get_alias()] = dict()

        for field in fields:
            projected_values[table.get_alias()][field] = result_row.values[table.get_alias()][field]

    return ResultRow(projected_values)