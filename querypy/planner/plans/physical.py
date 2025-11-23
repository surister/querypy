from typing import Any
from typing import Generator

from querypy.datasources import DataSource
from querypy.planner.expressions import PhysicalExpression
from querypy.planner.expressions import PhysicalPlan
from querypy.types_ import ColumnVector, RecordBatch, Schema


class Scan(PhysicalPlan):
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


class Projection(PhysicalPlan):
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


class Filter(PhysicalPlan):
    def __init__(self, input: PhysicalPlan, expr: PhysicalExpression):
        self.input = input
        self.expr = expr

    def schema(self) -> Schema:
        return self.input.schema()

    def children(self):
        return [self.input]

    def execute(self) -> list[RecordBatch]:
        """We apply the obtained bitmask to every field of the record batch(s)"""
        input = self.input.execute()
        new_record_batches = []
        for batch in input:
            mask = self.expr.evaluate(batch)
            new_fields = []
            for field in batch.fields:
                new_values = [v for v, b in zip(field.value, mask.value) if b]
                new_field = ColumnVector(field.type, new_values, len(new_values))
                new_fields.append(new_field)
            new_record_batches.append(RecordBatch(batch.schema, new_fields))
        return new_record_batches
