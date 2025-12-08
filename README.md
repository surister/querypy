Querypy is a query engine based on Columnar data, with inspiration on [Apache Arrow](https://arrow.apache.org/).

It is a pure Python implementation based on the book [How query engines work by 
Andy Groove](https://www.howqueryengineswork.com).
This is for educational purposes only, for no reason one should use this for any real work.

<img width="4580" height="1504" alt="b(16)" src="https://github.com/user-attachments/assets/b6b87d65-0270-439e-8c58-58d3977c3448" />


## What's implemented:
A logical layer with:
* Logical expressions: `Column`, `Literal`, `Boolean` and `Binary` expressions
(`Eq`, `Neq`, `Gt`, `GtEq`, `Lt`, `LtEq`, `And`, `Or`), Math expressions (`Add`, `Subtract`, `Mult`, `Div`), and
`Aggregates` expressions (`GroupBy`, `Count`, `Max`, `Min`, `Sum`, `Avg`).
* Logical plans: `Scan`, `Projection` (select), `Filter`, `Projection`.

A columnar based physical layer with:
* Physical expressions: `Column`, `Literal`, `Boolean` and `Binary` expressions, and `Aggregate`.
* Physical plans: `Scan`, `Projection` (select), `Filter`, `HashAggregate`.

A type system with:
`ArrowTypes` (`Bool`, `Ints`, `Ints`, `Strings`...), `ColumnVector`, `LiteralValueVector`,
`Field`, `Schema` and `RecordBatch`.

A planner that translates a logical plan into a physical plan.

A rule-based optimizer with:
* Projection pushdown

A dataframe-like API to easily build logical plans.

## Example
The following query:

```sql
SELECT country, AVG(salary)
FROM employee
WHERE salary > 40000
GROUP BY country;
```

Produces the following logical plan:

```python
plan = Projection(
    Aggregate(
        Filter(
            Scan("employees", CSVDataSource("./data/employees.csv"), []),
            Gt(Column("salary"), LiteralInteger(40000)),
        ),
        [Column("country")],
        [Avg(Column("salary"))],
    ),
    [Column("country"), Column("salary")],
)
print(get_text_tree(plan))
# Projection(#country, #salary)
# 	Aggregate(group_by=[#country, #salary], aggregate_by=[])
# 		Filter: #salary > 40000
# 			Scan: 'employees'; projection=[]
```

which produces the following physical plan:

```python
physical_plan = create_physical_plan(plan)
print(get_text_tree(physical_plan))
# Projection: (#0, #1)
# 	HashAggregate: group_by: [#2, #3]; aggregates: []
# 		Filter: Gt(#3, 40000)
# 			Scan: schema=Schema(Int32Type:id, StringType:name, StringType:country, Int32Type:salary), projection=[]
```

And executes:

```python
print(physical_plan.execute())
# RecordBatch(
#     fields=[['ES', 'US', 'ES', 'ES', 'FR', 'ES', 'US', 'ES', 'FR', 'IT',...],
#             [45200, 67800, 51350, 60220, 47200, 74400, 81200, 49910, 654...]],
#     columns=['country', 'salary'],
#     num_cols=2,
#     row_count=18
# )
```
While it is based in Apache Arrow, classes like `RecordBatch` are pure 
python implementations, not `pyarrow.RecordBatch` for example. Also, all 
operations are pure python.

# Project completeness.

This project is purely educational, objectives are self-assigned challenges.

It will be considered done when:

- [ ] it can run the Q1 of TPC-H benchmark (performance not important):
```sql
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
```
- [ ] It implements joins (common ones)
- [ ] It implements the following rule-based optimizations: projection 
  push-down, predicate push-down and common subexpressions elimination.
- [ ] 100% test coverage.
- [ ] Good docstring in most important structures (arbitrary goal).

Additional challenges:
- [ ] Support SQL (Pratt Parser)
- [ ] Support subexpressions and subquery decorrelation optimization.
- [ ] Cost based optimization on Parquet 
