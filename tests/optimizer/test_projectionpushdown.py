from unittest import mock
from unittest.mock import MagicMock

from querypy.datasources.csv import CSVDataSource
from querypy.optimizer import ProjectionPushDown
from querypy.planner.dataframe import DataFrame
from querypy.planner.expressions import LogicalPlan, LogicalExpression
from querypy.planner.expressions.logical import Column, LiteralInteger, Gt, \
    Binary, Max
from querypy.types_ import Schema
from querypy.utils import get_text_tree


def test_projectionpushdown():
    with ((mock.patch.object(CSVDataSource,
                             attribute='get_schema',
                             return_value=MagicMock(
                                 spec=Schema)))):
        plan = (DataFrame
                .scan_csv('somecsv')
                .filter("salary > 10000")
                .aggregate(["some_agg"], [Max(Column("some_max"))])
                .select(["country"])).logical_plan()

        new_plan = ProjectionPushDown().optimize(plan)
        projection_plan = new_plan.children()[0].children()[0].children()[0]
        assert projection_plan.projection == ['country', 'salary',
                                              'some_agg', 'some_max']
