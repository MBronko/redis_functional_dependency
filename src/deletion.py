from redis import Redis
from models import MetadataStore, TableRecord
from config import DeleteType


def get_delete_function(delete_type: DeleteType):
    return {
        DeleteType.SIMPLE: simple_delete,
        DeleteType.REDIS_SCRIPT: delete_using_redis_script
    }[delete_type]


def simple_delete(conn: Redis, metadata_store: MetadataStore, record: TableRecord) -> None:
    with conn.pipeline() as pipeline:
        table = metadata_store.get_table_by_name(record.table_descriptor)

        pipeline.multi()

        for field_descriptor in table.get_all_fields():
            field_key = record.get_field_key(metadata_store, field_descriptor)

            for dependency in table.functional_dependencies.get(field_descriptor, []):
                dependency_key = dependency.get_key(metadata_store, record)
                conn.srem(dependency_key, field_key)

            conn.delete(field_key)

        table_key = table.get_table_key()
        key_identifier = record.get_primary_key_identifier(metadata_store)
        conn.srem(table_key, key_identifier)

        pipeline.execute()


def delete_using_redis_script(conn: Redis, metadata_store: MetadataStore, record: TableRecord):
    keys = []
    args = []

    table = metadata_store.get_table_by_name(record.table_descriptor)
    table_key = table.get_table_key()
    key_identifier = record.get_primary_key_identifier(metadata_store)
    keys.append(table_key)
    args.append(key_identifier)

    table = metadata_store.get_table_by_name(record.table_descriptor)

    for field_descriptor in table.get_all_fields():
        field_key = record.get_field_key(metadata_store, field_descriptor)

        dependencies = table.functional_dependencies.get(field_descriptor, [])

        keys.append(field_key)
        args.append(len(dependencies))

        for dependency in dependencies:
            dependency_key = dependency.get_key(metadata_store, record)
            keys.append(dependency_key)

    lua_delete_record_script = """
    local argv_idx = 2
    local keys_idx = 2

    while keys_idx <= #KEYS do
        local field_key = KEYS[keys_idx]
        local dependency_count = ARGV[argv_idx]

        for dependency_iter = 1, dependency_count do
            local dependency_key = KEYS[keys_idx + dependency_iter]

            redis.call("SREM", dependency_key, field_key)
        end
        
        redis.call("DEL", field_key)
        
        keys_idx = keys_idx + dependency_count + 1
        argv_idx = argv_idx + 1
    end

    local table_key = KEYS[1]
    local key_identifier = ARGV[1]
    redis.call("SREM", table_key, key_identifier)

    return "OK"
    """

    conn.register_script(lua_delete_record_script)(keys=keys, args=args)
