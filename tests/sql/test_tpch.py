import sqlglot

from querypy.sql.expressions import create_logical_plan
from querypy.utils import get_text_tree


def test_tpch_1():
    """
    Test the first query of tpch queries.
    """

    expected_plan = """OrderBy([(#l_returnflag, True), (#l_linestatus, True)])
    	Projection: column_count: 10, columns: [#sum_disc_price, #sum_charge, #l_returnflag, #l_linestatus, #avg_qty, #sum_qty, #count_order, #avg_disc, #sum_base_price, #avg_price]
    		Aggregate: group_keys: [#l_returnflag, #l_linestatus], aggr_funcs_len: 8, aggr_funcs: [Sum((#l_extendedprice * (1 - #l_discount))) as #sum_disc_price, Sum(((#l_extendedprice * (1 - #l_discount)) * (1 + #l_tax))) as #sum_charge, Sum(#l_extendedprice) as #sum_base_price, Sum(#l_quantity) as #sum_qty, Avg(#l_quantity) as #avg_qty, Avg(#l_extendedprice) as #avg_price, Avg(#l_discount) as #avg_disc, Count(#*) as #count_order]
    			Filter: (#l_shipdate <= ('1998-12-01'::date - 90 IntervalType.DAY))
    				Scan: './data/lineitem.csv'; projection=None
    """
    r = sqlglot.parse_one("""
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
            './data/lineitem.csv'
        WHERE
            l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY
            l_returnflag,
            l_linestatus
        ORDER BY
            l_returnflag ASC,
            l_linestatus;
    """)

    plan = create_logical_plan(r)
    assert get_text_tree(plan, verbose=True) == expected_plan
