from querypy.datasources import DataSource
from querypy.planner import LogicalExpression, LogicalPlan
from querypy.types_ import Schema


class LogicalScan(LogicalPlan):
    def __init__(self, path: str, datasource: DataSource, projection: list[str]):
        self.path = path
        self.datasource = datasource
        self.projection = projection
        self.schema = self.derive_schema()

    def derive_schema(self) -> Schema:
        schema = self.datasource.get_schema()
        if self.projection:
            schema = schema.select(self.projection)
        return schema

    def get_schema(self) -> Schema:
        return self.schema

    def children(self) -> list["LogicalPlan"]:
        return []

    def __repr__(self):
        return f"Scan: {self.path}; projection={self.projection}"


class LogicalProjection(LogicalPlan):
    def __init__(self, input: LogicalPlan, expr: list[LogicalExpression]):
        self.input = input
        self.expr = expr

    def get_schema(self) -> Schema:
        return Schema([expr.to_field(self.inpupt) for expr in self.expr])

    def children(self) -> list["LogicalPlan"]:
        return [
            self.input,
        ]

    def __repr__(self):
        return f"{super().__repr__()}({', '.join(str(i) for i in self.expr)})"


class LogicalFilter(LogicalPlan):
    def __init__(self, input: LogicalPlan, expr: LogicalExpression):
        self.input = input
        self.expr = expr

    def get_schema(self) -> Schema:
        return self.input.get_schema()

    def children(self) -> list["LogicalPlan"]:
        return [self.input]

    def __repr__(self):
        return f"{super().__repr__()}: {self.expr}"


class LogicalAggregate(LogicalPlan):
    def __init__(
            self,
            input: LogicalPlan,
            group_by: list[LogicalExpression],
            aggr: list[LogicalExpression],
    ):
        self.input = input
        self.groupby = group_by
        self.aggr = aggr

    def get_schema(self) -> Schema:
        aggr_fields = [field.to_field(self.input) for field in self.aggr]
        groupby_fields = [field.to_field(self.input) for field in self.groupby]
        return Schema([*aggr_fields, *groupby_fields])

    def children(self) -> list["LogicalPlan"]:
        return [self.input]

    def __repr__(self):
        return super().__repr__() + f'(group_by={self.groupby}, aggregate_by={self.aggr})'


