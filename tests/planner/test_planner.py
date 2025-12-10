import pytest

from querypy.exceptions import UnknownColumnError
from querypy.planner.expressions.logical import Alias, Column
from querypy.planner.planner import create_physical_expr
from querypy.types_ import ArrowTypes, Field, Schema

from tests import create_logical_plan


def test_alias():
    field_name = "somefiled"
    expected_name = "somenewname"

    plan = create_logical_plan(
        schema=Schema([Field(field_name, ArrowTypes.StringType)])
    )
    col = create_physical_expr(Alias(expected_name, Column(field_name)), plan)
    assert col.i == 0

    with pytest.raises(UnknownColumnError):
        create_physical_expr(Alias(expected_name, Column("t")), plan)
