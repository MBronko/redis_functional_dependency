from dataclasses import dataclass
from hashlib import sha256
from json import dumps
from typing import Optional


@dataclass(frozen=True)
class FieldDescriptor:
    name: str
    primary_key: bool = False


@dataclass(frozen=True)
class FieldValue:
    value: str


@dataclass
class TableDefinition:
    name: str
    fields: list[FieldDescriptor]

    def __hash__(self):
        return hash(self.name)

    def get_all_fields(self) -> list[FieldDescriptor]:
        return self.fields

    def get_primary_key_fields(self) -> list[FieldDescriptor]:
        return list(filter(lambda x: x.primary_key, self.fields))

    def get_normal_fields(self) -> list[FieldDescriptor]:
        return list(filter(lambda x: not x.primary_key, self.fields))

    def get_table_key(self):
        return f"__table_keys__:{self.name}"

    def get_field_key_prefix(self, field: FieldDescriptor = None) -> str:
        if field is None:
            field = self.fields[0]

        return f"__value__:{self.name}:{field.name}"


@dataclass
class TableRecord:
    table_definition: TableDefinition
    values: dict[FieldDescriptor, FieldValue]

    def get_primary_key(self) -> dict[FieldDescriptor, Optional[FieldValue]]:
        primary_key: dict[FieldDescriptor, Optional[FieldValue]] = dict()

        for field in self.table_definition.get_primary_key_fields():
            primary_key[field] = self.get_value_object(field)

        return primary_key

    def get_primary_key_identifier(self) -> str:
        return key_policy(self.get_primary_key())

    def get_field_key(self, field: FieldDescriptor) -> str:
        key_prefix = self.table_definition.get_field_key_prefix(field)
        key_identifier = self.get_primary_key_identifier()

        return f"{key_prefix}:{key_identifier}"

    def get_value_object(self, field_descriptor: FieldDescriptor) -> Optional[FieldValue]:
        return self.values.get(field_descriptor, None)

    def get_value(self, field_descriptor: FieldDescriptor):
        value_object = self.get_value_object(field_descriptor)
        if value_object is None:
            return None
        return value_object.value


@dataclass
class FunctionalDependency:
    determinants: list[FieldDescriptor]
    dependent: FieldDescriptor

    def get_determinant_values(self, record: TableRecord):
        determinant_values: dict[FieldDescriptor, Optional[FieldValue]] = dict()

        for determinant in self.determinants:
            determinant_values[determinant] = record.get_value_object(determinant)

        return determinant_values

    def get_dependency_identifier(self, record: TableRecord):
        return key_policy(self.get_determinant_values(record))

    def get_key(self, record: TableRecord):
        determinant_names = "&".join(sorted(determinant.name for determinant in self.determinants))
        dependent_name = self.dependent.name
        dependency_identifier = self.get_dependency_identifier(record)

        return f"__dependency_index__:{determinant_names}=>{dependent_name}:{dependency_identifier}"


def json_key_policy(values: dict[FieldDescriptor, Optional[FieldValue]]):
    values_dict = dict()

    for field_descriptor, field_value in values.items():
        if field_value is None:
            values_dict[field_descriptor.name] = None
        else:
            values_dict[field_descriptor.name] = field_value.value

    return dumps(values_dict, separators=(',', ':'), sort_keys=True)


def sha256_key_policy(values: dict[FieldDescriptor, Optional[FieldValue]]):
    return sha256(json_key_policy(values).encode("utf-8")).hexdigest()


def key_policy(*args, **kwargs):
    return json_key_policy(*args, **kwargs)
