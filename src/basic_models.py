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


class SelectorCondition:
    def __init__(self, table_descriptor: TableDescriptor, field_descriptor: FieldDescriptor, condition_data=None):
        self.table_descriptor = table_descriptor
        self.field_descriptor = field_descriptor
        self.condition_data = condition_data

    def compare(self, other_value: FieldValue):
        raise NotImplemented


class SelectorConditionEquals(SelectorCondition):
    def compare(self, other_value: FieldValue):
        if other_value is None:
            return self.condition_data == other_value

        return self.condition_data == other_value.value


class SelectorConditionIn(SelectorCondition):
    def compare(self, other_value: FieldValue):
        return other_value in self.condition_data


class SelectorConditionNot(SelectorCondition):
    def __init__(self, condition: SelectorCondition):
        super().__init__(condition.table_descriptor, condition.field_descriptor)
        self.condition = condition

    def compare(self, other_value: FieldValue):
        return not self.condition.compare(other_value)


@dataclass
class Selector:
    select_fields: dict[TableDescriptor, list[FieldDescriptor]]
    from_table: TableDescriptor
    join_statements: list[JoinStatement]
    conditions: list[SelectorCondition]

    all_needed_fields: dict[TableDescriptor, set[FieldDescriptor]] = None
    parsed_conditions: dict[TableDescriptor, dict[FieldDescriptor, list[SelectorCondition]]] = None

    def __post_init__(self):
        self.all_needed_fields = dict()

        for table, field_list in self.select_fields.items():
            self.all_needed_fields[table] = set(field_list)

        for statement in self.join_statements:
            if statement.target_table not in self.all_needed_fields:
                self.all_needed_fields[statement.target_table] = set()

            self.all_needed_fields[statement.target_table].union(set(statement.target_fields))

            for table, field in statement.base_fields:
                self.all_needed_fields[table].add(field)

        self.parsed_conditions = dict()
        for condition in self.conditions:
            if condition.table_descriptor not in self.parsed_conditions:
                self.parsed_conditions[condition.table_descriptor] = dict()

            if condition.field_descriptor not in self.parsed_conditions[condition.table_descriptor]:
                self.parsed_conditions[condition.table_descriptor][condition.field_descriptor] = []

            self.parsed_conditions[condition.table_descriptor][condition.field_descriptor].append(condition)

            if condition.table_descriptor not in self.all_needed_fields:
                self.all_needed_fields[condition.table_descriptor] = set()

            self.all_needed_fields[condition.table_descriptor].add(condition.field_descriptor)
