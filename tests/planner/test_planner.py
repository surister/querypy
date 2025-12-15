import pytest

from querypy.exceptions import UnknownColumnError
from querypy.planner.expressions.logical import Alias, Column
from querypy.planner.planner import create_physical_expr, create_physical_plan
from querypy.planner.plans import logical as logical_plans
from querypy.planner.plans import physical as physical_plans
from querypy.types_ import ArrowTypes, Field, Schema

from tests import create_logical_test_plan


def test_alias():
    field_name = "somefiled"
    expected_name = "somenewname"

    plan = create_logical_test_plan(
        schema=Schema([Field(field_name, ArrowTypes.StringType)])
    )
    col = create_physical_expr(Alias(expected_name, Column(field_name)), plan)
    assert col.i == 0

    with pytest.raises(UnknownColumnError):
        create_physical_expr(Alias(expected_name, Column("t")), plan)


def test_orderby():
    field_name1 = "somefiled"
    field_name2 = "somenewname"

    plan = create_logical_test_plan(
        schema=Schema(
            [
                Field(field_name1, ArrowTypes.StringType),
                Field(field_name2, ArrowTypes.Int64Type),
            ]
        )
    )
    orderby = logical_plans.OrderBy(
        plan, order_by=[
            (Column(field_name1), False),
            (Column(field_name2), True)
        ]
    )
    orderby_plan = create_physical_plan(orderby)
    assert isinstance(orderby_plan, physical_plans.OrderBy)
    assert orderby_plan.order_by[0][0].i == 0
    assert orderby_plan.order_by[0][1] is False
    assert orderby_plan.order_by[1][0].i == 1
    assert orderby_plan.order_by[1][1] is True

    orderby = logical_plans.OrderBy(
        plan, order_by=[
            (Column('fake'), False),

        ]
    )
    with pytest.raises(UnknownColumnError):
        create_physical_plan(orderby)