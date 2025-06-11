from __future__ import annotations

from hash_db.models.basic_models import TableDescriptor, FieldDescriptor, FieldValue, FieldDefinition
from hash_db.exceptions import InvalidDescriptorException
from hash_db.tools.tools import get_key_generator
from hash_db.config import CoreConfiguration


class MetadataStore:
    tables: dict[str, TableDefinition]
    config: CoreConfiguration

    def __init__(self, tables: list[TableDefinition], config: CoreConfiguration | None = None):
        self.tables = self.init_tables(tables)

        if config is None:
            self.config = CoreConfiguration()
        else:
            self.config = config

    @staticmethod
    def init_tables(tables: list[TableDefinition]) -> dict[str, TableDefinition]:
        parsed_table = dict()
        for table in tables:
            parsed_table[table.table_descriptor.name] = table
        return parsed_table

    def get_table_by_name(self, table_descriptor: TableDescriptor) -> TableDefinition:
        if table_descriptor.name not in self.tables:
            raise InvalidDescriptorException(table_descriptor)

        return self.tables[table_descriptor.name]


class FunctionalDependency:
    determinants: list[FieldDescriptor]
    dependent: FieldDescriptor

    def __init__(self, determinants: list[FieldDescriptor], dependent: FieldDescriptor):
        self.determinants = determinants
        self.dependent = dependent

    def get_determinant_values(self, record: TableRecord):
        determinant_values: dict[FieldDescriptor, FieldValue | None] = dict()

        for determinant in self.determinants:
            determinant_values[determinant] = record.get_value_object(determinant)

        return determinant_values

    def get_dependency_identifier(self, metadata_store: MetadataStore, record: TableRecord):
        return get_key_generator(metadata_store.config.key_policy)(self.get_determinant_values(record))

    def get_key(self, metadata_store: MetadataStore, record: TableRecord):
        determinant_names = "&".join(sorted(determinant.name for determinant in self.determinants))
        dependent_name = self.dependent.name
        dependency_identifier = self.get_dependency_identifier(metadata_store, record)

        return f"__dependency_index__:{determinant_names}=>{dependent_name}:{dependency_identifier}"


class TableDefinition:
    table_descriptor: TableDescriptor
    fields: dict[FieldDescriptor, FieldDefinition]
    functional_dependencies: dict[FieldDescriptor, list[FunctionalDependency]]

    def __init__(self, table_descriptor: TableDescriptor, fields: list[FieldDefinition],
                 dependencies: list[FunctionalDependency] = None):

        self.table_descriptor = table_descriptor
        self.fields = self.init_fields(fields)

        if dependencies is None:
            self.functional_dependencies = dict()
        else:
            self.functional_dependencies = self.init_dependencies(dependencies)

    @staticmethod
    def init_fields(fields: list[FieldDefinition]) -> dict[FieldDescriptor, FieldDefinition]:
        parsed_fields = dict()
        for field in fields:
            parsed_fields[field.field_descriptor] = field
        return parsed_fields

    @staticmethod
    def init_dependencies(dependencies: list[FunctionalDependency]) -> dict[
        FieldDescriptor, list[FunctionalDependency]]:

        parsed_dependencies = dict()
        for dependency in dependencies:
            if dependency.dependent not in parsed_dependencies:
                parsed_dependencies[dependency.dependent] = list()

            parsed_dependencies[dependency.dependent].append(dependency)
        return parsed_dependencies

    def get_all_fields(self) -> list[FieldDescriptor]:
        return list(self.fields.keys())

    def get_primary_key_fields(self) -> list[FieldDescriptor]:
        result = []
        for field in self.fields.values():
            if field.primary_key:
                result.append(field.field_descriptor)
        return result

    def get_normal_fields(self) -> list[FieldDescriptor]:
        result = []
        for field in self.fields.values():
            if not field.primary_key:
                result.append(field.field_descriptor)
        return result

    def get_table_key(self):
        return f"__table_keys__:{self.table_descriptor.name}"

    def get_field_key_prefix(self, field: FieldDescriptor = None) -> str:
        if field is None:
            field = next(iter(self.fields.keys()))

        return f"__value__:{self.table_descriptor.name}:{field.name}"


class TableRecord:
    table_descriptor: TableDescriptor
    values: dict[FieldDescriptor, FieldValue]

    def __init__(self, table_descriptor: TableDescriptor, values: dict[FieldDescriptor, FieldValue]):
        self.table_descriptor = table_descriptor
        self.values = values

    def get_primary_key(self, metadata_store: MetadataStore) -> dict[FieldDescriptor, FieldValue | None]:
        primary_key: dict[FieldDescriptor, FieldValue | None] = dict()

        for field in metadata_store.get_table_by_name(self.table_descriptor).get_primary_key_fields():
            primary_key[field] = self.get_value_object(field)

        return primary_key

    def get_primary_key_identifier(self, metadata_store: MetadataStore) -> str:
        return get_key_generator(metadata_store.config.key_policy)(self.get_primary_key(metadata_store))

    def get_field_key(self, metadata_store: MetadataStore, field: FieldDescriptor) -> str:
        key_prefix = metadata_store.get_table_by_name(self.table_descriptor).get_field_key_prefix(field)
        key_identifier = self.get_primary_key_identifier(metadata_store)

        return f"{key_prefix}:{key_identifier}"

    def get_value_object(self, field_descriptor: FieldDescriptor) -> FieldValue | None:
        return self.values.get(field_descriptor, None)

    def get_value(self, field_descriptor: FieldDescriptor):
        value_object = self.get_value_object(field_descriptor)
        if value_object is None:
            return None
        return value_object.value
