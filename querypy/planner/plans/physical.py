from collections import defaultdict
from typing import Any
from typing import Generator

from querypy.datasources import DataSource
from querypy.planner.expressions import PhysicalExpression
from querypy.planner.expressions import PhysicalPlan
from querypy.planner.expressions.physical import Accumulator
from querypy.planner.expressions.physical import Aggregate
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

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.expr!r}"


class HashAggregate(PhysicalPlan):
    def __init__(
        self,
        input: PhysicalPlan,
        group_expr: list[PhysicalExpression],
        aggregate_expr: list[Aggregate],
        schema: Schema,
    ):
        self.input = input
        self.group_expr = group_expr
        self.aggregate_expr = aggregate_expr
        self.schema = schema

    def schema(self) -> Schema:
        return self.input.schema()

    def children(self) -> list["PhysicalPlan"]:
        return [self.input]

    def execute(self) -> list[RecordBatch]:
        groups: defaultdict[tuple, list[Accumulator]] = defaultdict()
        rbs = []
        for batch in self.input.execute():
            # the vectors that will be grouped.
            group_input = [expr.evaluate(batch) for expr in self.group_expr]
            # the columns that will be aggregated
            aggr_input_values = [
                aggr.expr.evaluate(batch) for aggr in self.aggregate_expr
            ]

            for row_i in range(batch.row_count):
                # a tuple containing keys for a group, for example in a schema of 'id' and 'name'
                # it could be (1, 'John'). This key group will serve as keys for a grouping
                # that will be used for storing the appropiate accumulators.
                #
                # the total group_keys will be the total number of unique group keys.

                # the total accumulators will be group_keys * aggr functions.
                group_keys = tuple(
                    group_key.get_value(row_i) for group_key in group_input
                )

                accumulators = groups.setdefault(
                    group_keys,
                    [expr.create_accumulator() for expr in self.aggregate_expr],
                )

                # accumulate values, every accumulator will only see the rows of it group_keys
                for idx, accumulator in enumerate(accumulators):
                    value = aggr_input_values[idx].get_value(row_i)
                    accumulator.accumulate(value)

            # at this point, data is already accumulated, but we only have a dictionary
            # of unique key groups, and it's accumulated (from the aggregations) values, like:
            # ('Alice', 1) [MaxAccumulator(accumulated_values=1, value=1)]
            # so we need to transform this into actual data to return, in this case:
            # [('Alice'), (1), (1)]
            num_group_cols = len(self.group_expr)
            num_aggr_cols = len(self.aggregate_expr)
            columns = [[] for _ in range(num_group_cols + num_aggr_cols)]

            for row_key, accumulators in groups.items():
                for i, key_val in enumerate(row_key):
                    columns[i].append(key_val)

                # aggregate columns (final values)
                for j, accum in enumerate(accumulators):
                    columns[num_group_cols + j].append(accum.final_value())
            rbs.append(RecordBatch.from_pylists(self.schema, columns))
        return rbs

    def __repr__(self):
        return super().__repr__() + (
            f"group_by: {self.group_expr}; aggregates: {self.aggregate_expr}"
        )
