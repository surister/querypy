from unittest.mock import MagicMock

from querypy.planner.expressions import PhysicalPlan
from querypy.planner.expressions.physical import (
    Subtract,
    LiteralInteger,
    Multiply,
    Divide,
    Add,
    Alias,
    Column,
)
from querypy.planner.planner import create_physical_expr
from querypy.planner.plans.physical import Projection
from querypy.planner.expressions import logical
from querypy.types_ import RecordBatch, Schema, Field, ArrowTypes, ColumnVector
from tests import create_rb


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
