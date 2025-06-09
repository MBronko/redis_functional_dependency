from enum import Enum
from dataclasses import dataclass


class InsertType(Enum):
    SIMPLE = "simple"
    TRANSACTIONAL = "transactional"
    REDIS_SCRIPT = "redis_script"


class DeleteType(Enum):
    SIMPLE = "simple"
    REDIS_SCRIPT = "redis_script"


class KeyPolicyType(Enum):
    JSON = "json"
    HASH = "hash"


class ListRecordsType(Enum):
    SCAN = "scan"
    KEYS = "keys"
    SET = "set"


class JoiningAlgorithm(Enum):
    NESTED_LOOPS = "nested_loops"


@dataclass
class CoreConfiguration:
    insert_type: InsertType = InsertType.REDIS_SCRIPT
    delete_type: DeleteType = DeleteType.REDIS_SCRIPT
    key_policy: KeyPolicyType = KeyPolicyType.JSON
    list_records_type: ListRecordsType = ListRecordsType.SET
    joining_algorithm: JoiningAlgorithm = JoiningAlgorithm.NESTED_LOOPS
