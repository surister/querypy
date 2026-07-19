from enum import Enum
from unittest.mock import MagicMock

import pytest

from querypy.exceptions import UnknownColumnError, AlreadyExistsColumnError
from querypy.planner.expressions.logical import (
    Column,
    LiteralString,
    LiteralFloat,
    LiteralInteger,
    Boolean,
    Eq,
    Neq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    And,
    Or,
    BooleanOp,
    Alias,
    Count
)
from querypy.planner.expressions.physical import CountAccumulator, \
    NullableAwareCountAccumulator
from querypy.planner.planner import create_physical_expr, create_physical_plan
from querypy.planner.plans.logical import OrderBy, Aggregate, Scan

from querypy.types_ import Schema, Field, ArrowTypes
from tests import create_logical_test_plan


def test_column():
    expected_name = "some_column"
    c = Column(expected_name)
    assert c.name == c
    assert str(c) == "#" + expected_name


def test_column_field():
    expected_name = "some_column"
    expected_type = ArrowTypes.BooleanType
    schema = Schema([Field(expected_name, expected_type)])
    c = Column(expected_name)

    plan = create_logical_test_plan(schema=schema)

    field = c.to_field(plan)
    assert isinstance(field, Field)
    assert field.name == expected_name
    assert field.type == expected_type

    plan = create_logical_test_plan(schema=Schema([]))
    with pytest.raises(UnknownColumnError):
        c.to_field(plan)


def test_string_literal():
    expected_value = "some_value"
    lit = LiteralString(expected_value)

    assert lit.value == expected_value
    assert repr(lit) == repr(expected_value)

    field = lit.to_field(MagicMock())
    assert field.name == expected_value
    assert field.type == ArrowTypes.StringType


def test_float_literal():
    expected_value = 1.2
    lit = LiteralFloat(expected_value)

    assert lit.value == expected_value
    assert repr(lit) == repr(expected_value)

    field = lit.to_field(MagicMock())
    assert field.name == repr(expected_value)
    assert field.type == ArrowTypes.FloatType


def test_integer_literal():
    expected_value = 12
    lit = LiteralInteger(expected_value)

    assert lit.value == expected_value
    assert repr(lit) == repr(expected_value)

    field = lit.to_field(MagicMock())
    assert field.name == repr(expected_value)
    assert field.type == ArrowTypes.Int64Type


class DummyExpr:
    def __init__(self, label: str):
        self.label = label

    def __repr__(self) -> str:
        return f"<{self.label}>"


def test_booleanop():
    assert issubclass(BooleanOp, Enum)
    expected_ops_in_registry = ["=", "!=", ">", "<", ">=", "<=", "AND", "OR"]

    for op in expected_ops_in_registry:
        assert any(op == m.symbol for m in BooleanOp)

    # duplicates
    assert len(BooleanOp) == len(set(BooleanOp))


def test_bool_operators():
    cases = {
        "eq": (Eq, BooleanOp.EQ.symbol),
        "neq": (Neq, BooleanOp.NEQ.symbol),
        "gt": (Gt, BooleanOp.GT.symbol),
        "gteq": (GtEq, BooleanOp.GTEQ.symbol),
        "lt": (Lt, BooleanOp.LT.symbol),
        "lteq": (LtEq, BooleanOp.LTEQ.symbol),
        "and": (And, BooleanOp.AND.symbol),
        "or": (Or, BooleanOp.OR.symbol),
    }

    l = DummyExpr("L")
    r = DummyExpr("R")

    for expected_name, (operator, operator_symbol) in cases.items():
        expr = operator(l, r)

        assert isinstance(expr, Boolean)
        assert expr.name == expected_name
        assert expr.op == operator_symbol
        assert expr.l is l
        assert expr.r is r

        assert repr(expr) == f"({l!r} {operator_symbol} {r!r})"

        field = expr.to_field(input=MagicMock())
        assert hasattr(field, "name")
        assert field.name == expected_name


def test_alias():
    field_name = "somefiled"
    expected_name = "somenewname"
    expected_type = ArrowTypes.StringType
    plan = create_logical_test_plan(
        schema=Schema([Field(field_name, ArrowTypes.StringType)])
    )

    field = Alias(expected_name, Column(field_name)).to_field(plan)
    assert isinstance(field, Field)
    assert field.name == expected_name
    assert field.type == expected_type

    with pytest.raises(AlreadyExistsColumnError):
        # We cannot alias if the column already exists
        Alias(field_name, Column(field_name)).to_field(plan)


def test_orderby():
    field_name = "somefiled"
    orderby_cols = [(Column(field_name), True), ]
    plan = create_logical_test_plan(
        schema=Schema([Field(field_name, ArrowTypes.StringType)])
    )

    orderby = OrderBy(plan, order_by=orderby_cols)
    assert orderby.get_schema() == plan.get_schema()
    assert orderby.input == plan
    assert orderby.children()[0] == plan
    assert orderby.order_by == orderby_cols


def test_count_nulls():
    field_name = "somefield"
    schema = Schema([Field(field_name, ArrowTypes.StringType)])

    scan = create_logical_test_plan(schema=schema)

    plan = Aggregate(
        input=scan,
        group_by=[Column(field_name)],
        aggregate=[Count(Column("*")), Count(Column(field_name))],
    )

    physical = create_physical_plan(plan)
    assert physical.aggregate_expr[0].ignore_nulls == True
    assert physical.aggregate_expr[1].ignore_nulls == False

    assert isinstance(physical.aggregate_expr[0].create_accumulator(),
                      CountAccumulator)
    assert isinstance(physical.aggregate_expr[1].create_accumulator(),
                      NullableAwareCountAccumulator)
