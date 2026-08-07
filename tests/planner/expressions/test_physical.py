import datetime
from unittest.mock import MagicMock

import pytest

from querypy.planner.expressions.physical import (
    Subtract,
    LiteralInteger,
    Multiply,
    Divide,
    Add,
    Alias,
    Column, Max, Avg, Count, Sum, LiteralInterval
)
from querypy.planner.planner import create_physical_expr
from querypy.planner.plans.physical import OrderBy, HashAggregate
from querypy.planner.expressions import logical
from querypy.types_ import Schema, Field, ArrowTypes, IntervalType
from tests import create_rb, create_logical_test_plan, create_physical_test_plan


def test_math():
    assert (
            Subtract(LiteralInteger(10), LiteralInteger(5))
            .evaluate(MagicMock())
            .get_value(0)
            == 5
    )

    assert (
            Add(LiteralInteger(10), LiteralInteger(5)).evaluate(
                MagicMock()).get_value(0)
            == 15
    )

    assert (
            Multiply(LiteralInteger(10), LiteralInteger(5))
            .evaluate(MagicMock())
            .get_value(0)
            == 50
    )

    assert (
            Divide(LiteralInteger(10), LiteralInteger(5)).evaluate(
                MagicMock()).get_value(0)
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


def test_aggregations_correctness():
    """
    Checks that they compute the right values
    """
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

def test_count_star():
    """
    Count(*) should ignore the nullability and count everything
    """
    a = [None, 2, 3, 4, None, 2]
    b = ["c", "b", "a", "a", "a", "c"]
    data = [a, b]
    dummy_plan = create_physical_test_plan(data)

    # Count(*)
    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(1)],
        aggregate_expr=[Count(Column(-2))], # by convention count star is -2
        schema=dummy_plan.schema()
    ).execute()

    assert (aggr_result[0].fields
            == [['c', 'b', 'a'], [2, 1, 3]])

def test_count_nulls():
    """
    A counter that does not ignore nulls like count(t1) should
    not count nulls.
    """
    a = [None, 2, 3, 4, None, 2]
    b = ["c", "b", "a", "a", "a", "c"]
    data = [a, b]
    dummy_plan = create_physical_test_plan(data)

    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(1)],
        aggregate_expr=[Count(Column(0), ignore_nulls=False)], # by convention
        schema=dummy_plan.schema()
    ).execute()

    assert (aggr_result[0].fields
            == [['c', 'b', 'a'], [1, 1, 2]])

def test_aggregation_order():
    """Test right field orders when several aggregates/groupbys are present"""
    a = [1, 2, 3, 4, 31, 1]
    b = ["c", "b", "a", "a", "a", "c"]
    c = [1, 2, 3, 4, 5, 6]
    data = [a, b, c]
    dummy_plan = create_physical_test_plan(data)

    # Max
    aggr_result = HashAggregate(
        dummy_plan,
        group_expr=[Column(0), Column(1)],
        aggregate_expr=[Sum(Column(2))],
        schema=Schema([Field('somecrap',
                             type=ArrowTypes.StringType),
                       *dummy_plan.schema().fields[:2], ])
    ).execute()

def test_interval():
    li = LiteralInterval(
        1, type=IntervalType.DAY
    ).evaluate(MagicMock())

    assert li.get_value(0) == datetime.timedelta(days=1)
    assert li.get_value(1) == datetime.timedelta(days=1)
    assert li.get_value(100_000) == datetime.timedelta(days=1)

    with pytest.raises(TypeError):
        LiteralInterval(
            [1,], type=IntervalType.DAY
        ).evaluate(MagicMock())


def test_interval_mathops():
    """Test operations of mathematics between intervals"""
    input_batch = create_rb([0, 0])

    result = Add(
        LiteralInterval(1, type=IntervalType.DAY),
        LiteralInterval(2, type=IntervalType.DAY),
    ).evaluate(input_batch)

    assert result.type == ArrowTypes.IntervalType
    assert result.value == [
        datetime.timedelta(days=3),
        datetime.timedelta(days=3),
    ]

    result = Subtract(
        LiteralInterval(2, type=IntervalType.DAY),
        LiteralInterval(1, type=IntervalType.DAY),
    ).evaluate(input_batch)

    assert result.type == ArrowTypes.IntervalType
    assert result.value == [
        datetime.timedelta(days=1),
        datetime.timedelta(days=1),
    ]

def test_interval_mathops_crosstypes():
    """Test operations of mathematics between intervals and other types
    like dates"""
    dummy_pl = create_logical_test_plan(schema=Schema([]))
    expr = (
        logical.LiteralDate("1998-12-01")
        - logical.LiteralInterval("90", IntervalType.DAY)
    )

    result = create_physical_expr(expr, dummy_pl).evaluate(create_rb([0, 0]))

    assert result.type == ArrowTypes.DateType
    assert result.value == [
        datetime.date(1998, 9, 2),
        datetime.date(1998, 9, 2),
    ]
