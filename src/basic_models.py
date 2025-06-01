from dataclasses import dataclass


@dataclass(frozen=True)
class TableDescriptor:
    name: str
    alias: str | None = None

    def __hash__(self):
        return hash(self.get_alias())

    def __eq__(self, other):
        if not isinstance(other, TableDescriptor):
            return False

        return self.get_alias() == other.get_alias()

    def get_alias(self):
        if self.alias is not None:
            return self.alias
        return self.name


@dataclass(frozen=True)
class FieldDescriptor:
    name: str


@dataclass(frozen=True)
class FieldValue:
    value: str


@dataclass(frozen=True)
class FieldDefinition:
    field_descriptor: FieldDescriptor
    primary_key: bool = False


@dataclass
class ResultRow:
    values: dict[TableDescriptor, dict[FieldDescriptor, FieldValue]]


@dataclass
class JoinStatement:
    base_fields: list[tuple[TableDescriptor, FieldDescriptor]]
    target_table: TableDescriptor
    target_fields: list[FieldDescriptor]


@dataclass
class Selector:
    select_fields: dict[TableDescriptor, list[FieldDescriptor]]
    from_table: TableDescriptor
    join_statements: list[JoinStatement]
