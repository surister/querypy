from querypy.planner.dataframe import DataFrame
from querypy.planner.expressions.logical import Subtract, LiteralInteger, Sum, \
    Column, Add


def test_inline_operations():
    """
    Test that we can plan inline expression in Dataframe select
    """
    df = (DataFrame
          .scan_csv('./data/lineitem.csv')
          ).select(
        ['l_orderkey', 'l_partkey',
         Subtract(LiteralInteger(-1), LiteralInteger(2)),
         Add(Column('l_partkey'), LiteralInteger(2))
         ]
    )
    assert df.logical_plan()