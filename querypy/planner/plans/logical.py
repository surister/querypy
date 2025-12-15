from querypy.datasources import DataSource
from querypy.planner.expressions import LogicalExpression
from querypy.planner.expressions import LogicalPlan
from querypy.planner.expressions.logical import Aggregate as AggregateExpression
from querypy.planner.expressions.logical import Boolean
from querypy.planner.expressions.logical import Column
from querypy.types_ import Schema


class Scan(LogicalPlan):
    """
    A logical scan, reading data from a given datasource,
    it mostly delegates work to the datasource.
    """

    def __init__(self, path: str, datasource: DataSource, projection: list[str]):
        self.path = path
        self.datasource = datasource
        self.projection = projection
        self.schema = self.derive_schema()

    def derive_schema(self) -> Schema:
        """
        Returns the schema from applying the given projection to the schema that was
        read from the datasource.

        Returns
        -------
        The expected schema for the scan operation.
        """
        schema = self.datasource.get_schema()
        if self.projection:
            schema = schema.select(self.projection)
        return schema

    def get_schema(self) -> Schema:
        return self.schema

    def children(self) -> list["LogicalPlan"]:
        return []

    def __repr__(self):
        return f"Scan: '{self.path}'; projection={self.projection}"


class Projection(LogicalPlan):
    """
    A projection over a schema. Commonly to reduce the number of columns.
    """

    def __init__(self, input: LogicalPlan, expr: list[LogicalExpression]):
        self.input = input
        self.expr = expr

    def get_schema(self) -> Schema:
        return Schema([expr.to_field(self.input) for expr in self.expr])

    def children(self) -> list["LogicalPlan"]:
        return [
            self.input,
        ]

    def __repr__(self):
        return f"{super().__repr__()}({', '.join(str(i) for i in self.expr)})"


class Filter(LogicalPlan):
    """
    A plan that filters out sets of rows by a given BooleanExpression
    """

    def __init__(self, input: LogicalPlan, expr: Boolean):
        self.input = input
        self.expr = expr

    def get_schema(self) -> Schema:
        return self.input.get_schema()

    def children(self) -> list["LogicalPlan"]:
        return [self.input]

    def __repr__(self):
        return f"{super().__repr__()}: {self.expr}"


class Aggregate(LogicalPlan):
    """
    A plan that aggregates rows by grouping or by aggregate functions, typically generating
    new columns.
    """

    def __init__(
        self,
        input: LogicalPlan,
        group_by: list[Column],
        aggregate: list[AggregateExpression],
    ):
        self.input = input
        self.group_by = group_by
        self.aggregate = aggregate

    def get_schema(self) -> Schema:
        aggr_fields = [field.to_field(self.input) for field in self.aggregate]
        groupby_fields = [field.to_field(self.input) for field in self.group_by]
        return Schema([*aggr_fields, *groupby_fields])

    def children(self) -> list["LogicalPlan"]:
        return [self.input]

    def __repr__(self):
        return (
            super().__repr__()
            + f"(group_by={self.group_by}, aggregate_by={self.aggregate})"
        )


class OrderBy(LogicalPlan):
    def __init__(self, input: LogicalPlan, order_by: list[tuple[Column, bool]]):
        self.input = input
        self.order_by = order_by

    def get_schema(self) -> Schema:
        return self.input.get_schema()

    def children(self) -> list["LogicalPlan"]:
        return [self.input]

    def __repr__(self):
        return (
            super().__repr__()
            + f"({[(col, ascending) for col, ascending in self.order_by]})"
        )
