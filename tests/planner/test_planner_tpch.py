"""Tests that the first tpch query can be planned."""
from querypy.planner.dataframe import DataFrame
from querypy.planner.expressions.logical import Sum, Column, Alias, Subtract, \
    LiteralInteger, Multiply, Add, Avg, Count, LiteralDate, LiteralInterval

from querypy.planner.planner import create_physical_plan
from querypy.types_ import RecordBatch, IntervalType, Schema, Field, ArrowTypes


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

    Produces in PostgreSQL 18.3 (Ubuntu 18.3-1.pgdg22.04+1) on aarch64-unknown-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04.3) 11.4.0, 64-bit

    l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order
    A,F,12,14400,13680,13816.8,12,14400,0.05,1
    A,O,19,28500.5,25650.45,26932.9725,19,28500.5,0.1,1
    N,F,58,110320,102320,108149.6,29,55160,0.04,2
    N,O,84,142624.1,134696.9935,142423.825445,28,47541.366666666667,0.05333333333333333333,3
    R,F,67,118734.72,112298.2896,118260.221184,33.5,59367.36,0.045,2
    R,O,23,29074.4,28123.536,28686.00672,11.5,14537.2,0.04,2
    """

    expected_values = {
        'sum_disc_price': [13680.0, 25650.45, 102320.0, 134696.9935,
                           112298.28959999999, 28123.536],
        'sum_charge': [13816.8, 26932.972500000003, 108149.6,
                       142423.82544500002, 118260.221184, 28686.00672],
        'l_returnflag': ['A', 'A', 'N', 'N', 'R', 'R'],
        'l_linestatus': ['F', 'O', 'F', 'O', 'F', 'O'],
        'avg_qty': [12.0, 19.0, 29.0, 28.0, 33.5, 11.5],
        'count_order': [1, 1, 2, 3, 2, 2],
        'sum_qty': [12, 19, 58, 84, 67, 23],
        'sum_base_price': [14400.0, 28500.5, 110320.0, 142624.1, 118734.72,
                           29074.4],
        'avg_price': [14400.0, 28500.5, 55160.0, 47541.36666666667, 59367.36,
                      14537.2],
        'avg_disc': [0.05, 0.1, 0.04, 0.05333333333333334,
                     0.045000000000000005, 0.04],

    }

    df = (
        DataFrame.scan_csv(
            "./data/lineitem.csv",
            override_schema=Schema([Field('l_shipdate', ArrowTypes.DateType)])
        )
        .filter(Column("l_shipdate") <= LiteralDate('1998-12-01') -
                LiteralInterval('90', IntervalType.DAY))
        .aggregate(
            group_by=['l_returnflag', 'l_linestatus'],
            aggr=[
                Sum(
                    Multiply(Column("l_extendedprice"),
                             Subtract(LiteralInteger(1), Column('l_discount')))
                ),
                Sum(
                    Multiply(Multiply(Column("l_extendedprice"),
                                      Subtract(LiteralInteger(1),
                                               Column('l_discount'))),
                             Add(LiteralInteger(1), Column("l_tax")))
                ),
                Sum(Column("l_extendedprice")),
                Sum(Column("l_quantity")),
                Avg(Column("l_quantity")),
                Avg(Column("l_extendedprice")),
                Avg(Column("l_discount")),
                Count(Column("*"))
            ]
        )

        .select(
            [
                Alias('sum_disc_price',
                      "sum_(#l_extendedprice * (1 - #l_discount))"),
                Alias('sum_charge',
                      "sum_((#l_extendedprice * (1 - #l_discount)) * (1 + #l_tax))"),
                "l_returnflag",
                "l_linestatus",
                Alias('avg_qty', "avg_#l_quantity"),
                Alias('count_order', "count_#*"),
                Alias('sum_qty', "sum_#l_quantity"),
                Alias('sum_base_price', "sum_#l_extendedprice"),
                Alias('avg_price', "avg_#l_extendedprice"),
                Alias('avg_disc', "avg_#l_discount"),
                Alias('count_order', "count_#*")
            ]
        )
        .order_by([('l_returnflag', True), ('l_linestatus', True)])
    )


    rb: RecordBatch = list(
        create_physical_plan(df.logical_plan()).execute()
    )[0]
    assert rb.column_count == 11
    assert rb.row_count == 6

    for field, column_name in zip(rb.fields, rb.column_names()):
        assert expected_values[column_name] == field.value
