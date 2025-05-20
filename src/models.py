from dataclasses import dataclass, field
from hashlib import sha256
from json import dumps

from redis.asyncio import Redis


@dataclass(frozen=True)
class FieldDescriptor:
    name: str

    def get_field_key(self, key_identifier: str):
        return f"__value__:{self.name}:{key_identifier}"


@dataclass(frozen=True)
class FieldValue:
    value: str


@dataclass
class Tuple6NF:
    primary_key: dict[FieldDescriptor, FieldValue]

    field_descriptor: FieldDescriptor
    field_value: FieldValue

    def get_primary_key_identifier(self):
        return key_policy(self.primary_key)


@dataclass(frozen=True)
class FunctionalDependency:
    determinants: tuple
    dependent: FieldDescriptor

    def get_dependency_identifier(self, connection: Redis, primary_key_identifier: str):
        dependency_values: dict[FieldDescriptor, FieldValue] = {}

        for determinant in self.determinants:
            value = FieldValue(connection.get(determinant.get_field_key(primary_key_identifier)))
            dependency_values[determinant] = value

        return key_policy(dependency_values)

    def get_key(self, connection: Redis, primary_key_identifier: str):
        determinant_names = "&".join(sorted(determinant.name for determinant in self.determinants))
        dependent_name = self.dependent.name

        return f"__index__:{determinant_names}=>{dependent_name}:{self.get_dependency_identifier(connection, primary_key_identifier)}"


def json_key_policy(tuple_fields: dict[FieldDescriptor, FieldValue]):
    fields_dict = {field_descriptor.name: field_value.value for field_descriptor, field_value in tuple_fields.items()}

    return dumps(fields_dict, separators=(',', ':'), sort_keys=True)


def sha256_key_policy(fields: dict[FieldDescriptor, FieldValue]):
    return sha256(json_key_policy(fields).encode("utf-8")).hexdigest()

key_policy = json_key_policy