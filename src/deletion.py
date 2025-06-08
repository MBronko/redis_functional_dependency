from redis import Redis
from models import MetadataStore, TableRecord
from config import ListRecordsType


def get_delete_function():
    return delete_value


def delete_value(conn: Redis, metadata_store: MetadataStore, record: TableRecord) -> None:
    with conn.pipeline() as pipeline:
        table = metadata_store.get_table_by_name(record.table_descriptor)

        pipeline.multi()

        for field_descriptor in table.get_all_fields():
            value_key = record.get_field_key(metadata_store, field_descriptor)

            for dependency in table.functional_dependencies.get(field_descriptor, []):
                dependency_key = dependency.get_key(metadata_store, record)
                conn.srem(dependency_key, value_key)

            conn.delete(value_key)

        table_key = table.get_table_key()
        key_identifier = record.get_primary_key_identifier(metadata_store)
        conn.srem(table_key, key_identifier)

        pipeline.execute()
