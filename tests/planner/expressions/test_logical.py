from enum import Enum
from unittest.mock import MagicMock

import pytest

from querypy.exceptions import UnknownColumnError
from querypy.planner.expressions import LogicalPlan
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
)
from querypy.types_ import Schema, Field, ArrowTypes


def create_logical_plan(children: list = None, schema: Schema = None) -> LogicalPlan:
    class SomeLogicalPlan(LogicalPlan):
        def __init__(self, children: list = None):
            self._children = children or []
            self._schema = schema or []

        def children(self) -> list["LogicalPlan"]:
            return self._children

        def get_schema(self) -> Schema:
            return self._schema

    t = SomeLogicalPlan(children)
    t._schema = schema
    return t


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

    plan = create_logical_plan(schema=schema)

    field = c.to_field(plan)
    assert isinstance(field, Field)
    assert field.name == expected_name
    assert field.type == expected_type

    plan = create_logical_plan(schema=Schema([]))
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

    for expected_name, (ctor, expected_op) in cases.items():
        expr = ctor(l, r)

        assert isinstance(expr, Boolean)
        assert expr.name == expected_name
        assert expr.op == expected_op
        assert expr.l is l
        assert expr.r is r

        assert repr(expr) == f"{l!r} {expected_op} {r!r}"

        field = expr.to_field(input=MagicMock())
        assert hasattr(field, "name")
        assert field.name == expected_name
