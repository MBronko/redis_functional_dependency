import redis
from redis import Redis
from redis.client import Pipeline

from exceptions import DependencyBrokenException

from config import InsertType
from models import MetadataStore, TableRecord


def get_insert_function(insert_type: InsertType):
    return {
        InsertType.SIMPLE: simple_insert_value,
        InsertType.TRANSACTIONAL: insert_value_transaction,
        InsertType.REDIS_SCRIPT: insert_using_lua_script
    }[insert_type]


def check_dependencies(conn: Redis | Pipeline, metadata_store: MetadataStore, record: TableRecord) -> list[
    tuple[str, str]]:
    dependency_indexes_update_list: list[tuple[str, str]] = []

    table = metadata_store.get_table_by_name(record.table_descriptor)

    for field_descriptor in table.get_all_fields():
        field_value = record.get_value(field_descriptor)
        value_key = record.get_field_key(metadata_store, field_descriptor)

        # ensure value will not be changed until transaction executed
        conn.watch(value_key)

        for dependency in table.functional_dependencies.get(field_descriptor, []):
            dependency_key = dependency.get_key(metadata_store, record)

            # ensure dependency index will not be changed until transaction executed
            conn.watch(dependency_key)

            dependency_index_random_member = conn.srandmember(dependency_key)

            if dependency_index_random_member is not None:
                expected_value = conn.get(dependency_index_random_member)
                if expected_value != field_value:
                    raise DependencyBrokenException()

            dependency_indexes_update_list.append((dependency_key, value_key))

    return dependency_indexes_update_list


def insert_record_data(conn: Redis | Pipeline, metadata_store: MetadataStore, record: TableRecord,
                       dependency_indexes_update_list: list[tuple[str, str]]) -> None:
    table = metadata_store.get_table_by_name(record.table_descriptor)

    for dependency_key, value_key in dependency_indexes_update_list:
        conn.sadd(dependency_key, value_key)

    # we should maintain records index set to use when listing records
    table_key = table.get_table_key()
    key_identifier = record.get_primary_key_identifier(metadata_store)
    conn.sadd(table_key, key_identifier)

    for field_descriptor in table.get_all_fields():
        value_key = record.get_field_key(metadata_store, field_descriptor)
        field_value = record.get_value_object(field_descriptor)

        if field_value is not None:
            conn.set(value_key, field_value.value)


def simple_insert_value(conn: Redis, metadata_store: MetadataStore, record: TableRecord) -> None:
    # check all dependencies for all fields. raises exception if dependency is broken
    dependency_indexes_update_list = check_dependencies(conn, metadata_store, record)

    # if no dependency is broken, update dependency indexes and insert values
    insert_record_data(conn, metadata_store, record, dependency_indexes_update_list)


def insert_value_transaction(conn: Redis, metadata_store: MetadataStore, record: TableRecord) -> None:
    with conn.pipeline() as pipeline:
        while True:
            try:
                # check all dependencies for all fields. raises exception if dependency is broken
                dependency_indexes_update_list = check_dependencies(pipeline, metadata_store, record)

                # start transaction
                pipeline.multi()

                # if no dependency is broken, update dependency indexes and insert values
                insert_record_data(pipeline, metadata_store, record, dependency_indexes_update_list)

                pipeline.execute()
                break

            except redis.WatchError:
                print("WatchError: retrying transaction")


def insert_using_lua_script(conn: Redis, metadata_store: MetadataStore, record: TableRecord) -> None:
    keys = []
    args = []

    table = metadata_store.get_table_by_name(record.table_descriptor)
    table_key = table.get_table_key()
    key_identifier = record.get_primary_key_identifier(metadata_store)
    keys.append(table_key)
    args.append(key_identifier)

    table = metadata_store.get_table_by_name(record.table_descriptor)

    all_fields = table.get_all_fields()

    for field_descriptor in all_fields:
        field_value = record.get_value(field_descriptor)
        field_key = record.get_field_key(metadata_store, field_descriptor)

        args.append(field_value)
        keys.append(field_key)

        dependencies = table.functional_dependencies.get(field_descriptor, [])

        args.append(len(dependencies))

        for dependency in dependencies:
            dependency_key = dependency.get_key(metadata_store, record)
            keys.append(dependency_key)

    lua_check_and_set = """
    local argv_idx = 2
    local keys_idx = 2
    
    local dependency_indexes_update_list = {}
    local field_keys_values = {}
    
    while keys_idx <= #KEYS do
        local field_key = KEYS[keys_idx]
        local field_value = ARGV[argv_idx]
        table.insert(field_keys_values, {field_key, field_value})
        
        local dependency_count = ARGV[argv_idx + 1]
        argv_idx = argv_idx + 2
        
        for dependency_iter = 1, dependency_count do
            local dependency_key = KEYS[keys_idx + dependency_iter]
        
            local random_dependency_member = redis.call("SRANDMEMBER", dependency_key)
            if random_dependency_member then
                if field_value ~= redis.call("GET", random_dependency_member) then
                    return nil
                end
            end
            
            table.insert(dependency_indexes_update_list, {dependency_key, field_key})
        end
        keys_idx = keys_idx + dependency_count + 1
    end
    
    
    for i = 1, #dependency_indexes_update_list do
        redis.call("SADD", dependency_indexes_update_list[i][1], dependency_indexes_update_list[i][2])
    end
    
    local table_key = KEYS[1]
    local key_identifier = ARGV[1]
    redis.call("SADD", table_key, key_identifier)
    
    for i = 1, #field_keys_values do
        redis.call("SET", field_keys_values[i][1], field_keys_values[i][2])
    end
    
    return "OK"
    """

    check_set_script = conn.register_script(lua_check_and_set)

    res = check_set_script(keys=keys, args=args)

    if res == "OK":
        print("Redis script executed successfully")
    else:
        raise DependencyBrokenException()
