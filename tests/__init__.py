from unittest.mock import MagicMock

from querypy.planner.expressions import LogicalPlan, PhysicalPlan
from querypy.planner.plans.logical import Scan
from querypy.types_ import Schema, RecordBatch, ArrowTypes, Field, ColumnVector


def create_logical_test_plan(
    children: list = None, schema: Schema = None
) -> LogicalPlan:

    class SomeLogicalPlan(Scan):
        def __init__(self, child: list = None):
            super().__init__(
                datasource=MagicMock(get_schema=lambda: MagicMock(
                select=lambda _:['MagicMockSchema'])),
                projection=None,
                path=None
            )
            print(child, 'child')

            self._children = child
            print(self._children, '_children of', self)
            self._schema = schema or []

        def children(self) -> list["LogicalPlan"]:
            return self._children

        def get_schema(self) -> Schema:
            return self._schema

    t = SomeLogicalPlan(child=children)
    t._schema = schema
    return t


def create_rb(values: list[list] | list) -> RecordBatch:
    if not isinstance(values[0], list):
        values = [
            values,
        ]

    schema = Schema(
        [
            Field(f"col_{i}", ArrowTypes.from_pyvalue(col[0]))
            for i, col in enumerate(values)
        ]
    )

    fields = [
        ColumnVector(ArrowTypes.from_pyvalue(col[0]), col, size=len(col))
        for col in values
    ]
    rb = RecordBatch(schema, fields)
    return rb


def create_physical_test_plan(values: list[list] | list) -> PhysicalPlan:
    class DummyPlan(PhysicalPlan):
        def __init__(self, rb: RecordBatch):
            self.record_batch = rb

        def children(self) -> list["PhysicalPlan"]:
            return []

        def schema(self) -> Schema:
            return self.record_batch.schema

        def execute(self) -> list[RecordBatch]:
            return [self.record_batch]

    rb = create_rb(values)
    return DummyPlan(rb)
