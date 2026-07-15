"""Tests that the first tpch query can be planned."""
from querypy.planner.dataframe import DataFrame
from querypy.planner.expressions.logical import Sum, Column, Alias
from querypy.planner.planner import create_physical_plan
from querypy.types_ import RecordBatch
from querypy.utils import get_text_tree


def test_tphc_1():
    """
    SELECT
        l_returnflag,
        l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity) AS avg_qty,
        avg(l_extendedprice) AS avg_price,
        avg(l_discount) AS avg_disc,
        count(*) AS count_order
    FROM
        lineitem
    WHERE
        l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus;
    """

    df = DataFrame.scan_csv(
        "./data/lineitem.csv"
    ).aggregate(
        group_by=['l_returnflag', 'l_linestatus'],
        aggr=[
            Sum(Column("l_quantity"))
            #Alias("sum_qty", )
        ]
    )

    print('\n', get_text_tree(df.logical_plan()))

    print(get_text_tree(create_physical_plan(df.logical_plan())))

    data: list[RecordBatch] = create_physical_plan(df.logical_plan()).execute()

    # We know we'll produce only one recordbatch

    # TODO: Fill later with correct values.
    assert data[0].column_count == 1
    assert data[0].row_count == 1
