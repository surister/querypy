from unittest.mock import MagicMock

from querypy.planner.expressions.physical import (
    Subtract,
    LiteralInteger,
    Multiply,
    Divide,
    Add,
)


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
