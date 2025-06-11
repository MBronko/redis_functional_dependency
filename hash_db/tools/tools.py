from hashlib import sha256
from json import dumps

from hash_db.models import FieldDescriptor, FieldValue
from hash_db.config import KeyPolicyType


def json_key_policy(values: dict[FieldDescriptor, FieldValue | None]):
    values_dict = dict()

    for field_descriptor, field_value in values.items():
        if field_value is None:
            values_dict[field_descriptor.name] = None
        else:
            values_dict[field_descriptor.name] = field_value.value

    return dumps(values_dict, separators=(',', ':'), sort_keys=True)


def sha256_key_policy(values: dict[FieldDescriptor, FieldValue | None]):
    return sha256(json_key_policy(values).encode("utf-8")).hexdigest()


def key_policy(values: dict[FieldDescriptor, FieldValue | None]):
    return json_key_policy(values)


def get_key_generator(key_policy: KeyPolicyType):
    return {
        KeyPolicyType.JSON: json_key_policy,
        KeyPolicyType.HASH: sha256_key_policy,
    }[key_policy]
