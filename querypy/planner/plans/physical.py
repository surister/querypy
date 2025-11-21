from typing import Any
from typing import Generator

from querypy.datasources import DataSource
from querypy.planner.expressions import PhysicalExpression
from querypy.planner.expressions import PhysicalPlan
from querypy.types_ import RecordBatch
from querypy.types_ import Schema


class ScanExecutionPlan(PhysicalPlan):
    """
    Physical implementation of a Scan operation.
    """

    def __init__(self, datasource: DataSource, projection: list[str]):
        self.datasource = datasource
        self.projection = projection

    def schema(self) -> Schema:
        return self.datasource.get_schema().select(self.projection)

    def children(self) -> list["PhysicalPlan"]:
        return []

    def execute(self) -> list[RecordBatch]:
        return self.datasource.scan(self.projection)

    def __repr__(self):
        return f"{self.__class__.__name__}: schema={self.schema()}, projection={self.projection}"


class ProjectionExecutionPlan(PhysicalPlan):
    def __init__(
        self, input: PhysicalPlan, schema: Schema, expr: list[PhysicalExpression]
    ):
        self.input = input
        self.schema = schema
        self.expr = expr

    def schema(self) -> Schema:
        return self.schema

    def children(self) -> list["PhysicalPlan"]:
        return [self.input]

    def execute(self) -> Generator[RecordBatch, Any, None]:
        result = self.input.execute()

        for batch in result:
            columns = [expr.evaluate(batch) for expr in self.expr]
            yield RecordBatch(self.schema, columns)

    def __repr__(self):
        return f"{super().__repr__()}({', '.join(str(i) for i in self.expr)})"
