from unittest.mock import MagicMock

from querypy.planner.dataframe import DataFrame
from querypy.planner.expressions import PhysicalPlan

from querypy.planner.expressions.physical import (
    Subtract,
    LiteralInteger,
    Multiply,
    Divide,
    Add,
    Alias,
    Column, Max, Avg, Count, Sum
)
from querypy.planner.planner import create_physical_expr
from querypy.planner.plans.physical import Projection, OrderBy, HashAggregate
from querypy.planner.expressions import logical
from querypy.types_ import RecordBatch, Schema, Field, ArrowTypes, ColumnVector
from tests import create_rb, create_logical_test_plan, create_physical_test_plan


def test_math():
    assert (
        Subtract(LiteralInteger(10), LiteralInteger(5))
        .evaluate(MagicMock())
        .get_value(0)
        == 5
    )

    assert (
        Add(LiteralInteger(10), LiteralInteger(5)).evaluate(MagicMock()).get_value(0)
        == 15
    )

    assert (
        Multiply(LiteralInteger(10), LiteralInteger(5))
        .evaluate(MagicMock())
        .get_value(0)
        == 50
    )

    assert (
        Divide(LiteralInteger(10), LiteralInteger(5)).evaluate(MagicMock()).get_value(0)
        == 2.0
    )


def test_column():
    expected_index = 0
    expected_values = [1, 2]
    col = Column(expected_index)

    assert col.i == expected_index
    result = col.evaluate(create_rb([expected_values, ["ab", "cd"]]))

    assert expected_values == result.value


def test_alias():
    assert issubclass(Alias, Column)


def test_orderby():
    a = [1, 2, 3]
    b = ["c", "b", "a"]
    data = [a, b]
    plan = create_physical_test_plan(data)
    order_by_a = data.index(a)
    order_by_b = data.index(b)

    orderby = OrderBy(plan, order_by=[[Column(order_by_a), False]])

    assert plan.schema() == orderby.schema()
    assert orderby.children()[0] == plan

    rb = list(orderby.execute())[0]
    assert rb.get_field(order_by_a) == [3, 2, 1]

    orderby.order_by[0][1] = True
    rb = list(orderby.execute())[0]
    assert rb.get_field(order_by_a) == [1, 2, 3]

    # Other datatype than int; str.
    orderby = OrderBy(plan, order_by=[[Column(order_by_b), False]])
    rb = list(orderby.execute())[0]
    assert rb.get_field(order_by_b) == ["c", "b", "a"]

    orderby.order_by[0][1] = True
    rb = list(orderby.execute())[0]
    assert rb.get_field(order_by_b) == ["a", "b", "c"]


def test_aggregations():
    a = [1, 2, 3, 4, 31, 2]
    b = ["c", "b", "a", "a", "a", "c"]
    data = [a, b]
    dummy_plan = create_physical_test_plan(data)

    # Max
    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(1)],
        aggregate_expr=[Max(Column(0))],
        schema=dummy_plan.schema()
    ).execute()

    assert (aggr_result[0].fields
            == [['c', 'b', 'a'], [2, 2, 31]])
    # Avg
    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(1)],
        aggregate_expr=[Avg(Column(0))],
        schema=dummy_plan.schema()
    ).execute()

    assert (aggr_result[0].fields
            == [['c', 'b', 'a'], [1.5, 2.0, 12.666666666666666]])

    # Count
    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(1)],
        aggregate_expr=[Count(Column(0))],
        schema=dummy_plan.schema()
    ).execute()

    assert (aggr_result[0].fields
            == [['c', 'b', 'a'], [2, 1, 3]])

    # Sum
    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(1)],
        aggregate_expr=[Sum(Column(0))],
        schema=dummy_plan.schema()
    ).execute()

    assert (aggr_result[0].fields
            == [['c', 'b', 'a'], [3, 2, 38]])