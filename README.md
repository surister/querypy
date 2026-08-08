Querypy is a small query engine written in pure Python. It uses
column-oriented batches inspired by Apache Arrow, but it does not depend on
any arrow implementation.

The project follows the structure from
[How Query Engines Work](https://www.howqueryengineswork.com/): build a logical
plan, lower it to a physical plan, then execute that plan over column vectors.

## What Exists

Querypy currently has:

- A logical expression layer with columns, literals, aliases, boolean
  expressions, arithmetic expressions, interval/date literals, and aggregate
  expressions.
- Logical plans for `Scan`, `Projection`, `Filter`, `Aggregate`, and `OrderBy`.
- A physical expression layer that evaluates expressions over `RecordBatch`
  inputs.
- Physical plans for CSV scan, projection, filtering, hash aggregation, and
  ordering.
- A small type system: `ArrowTypes`, `Field`, `Schema`, `ColumnVector`,
  `LiteralValueVector`, and `RecordBatch`.
- A planner that translates logical plans and expressions into 
  physical execution nodes.
- A rule-based optimizer.
- A dataframe-like API for constructing logical plans.

The main execution model is:

```text
CSVDataSource -> RecordBatch -> PhysicalExpression vectors -> PhysicalPlan
```

`HashAggregate` is the only aggregate implementation. Like PostgreSQL's
`HashAggregate`, it does not require sorted input; it keeps Python accumulator
objects keyed by group tuples.

## Example

This example groups the bundled TPC-H-like `lineitem` sample by return flag:

```python
from querypy.planner.dataframe import DataFrame
from querypy.planner.expressions.logical import Avg, Column, Sum
from querypy.planner.planner import create_physical_plan
from querypy.utils import get_text_tree

df = (
    DataFrame.scan_csv("./data/lineitem.csv")
    .aggregate(
        ["l_returnflag"],
        [
            Sum(Column("l_quantity")),
            Avg(Column("l_discount")),
        ],
    )
    .order_by([("l_returnflag", True)])
)

print(get_text_tree(df.logical_plan()))

physical_plan = create_physical_plan(df.logical_plan())
print(get_text_tree(physical_plan))

record_batch = list(physical_plan.execute())[0]
print(record_batch.column_names())
print([field.value for field in record_batch.fields])
```

The logical plan is:

```text
OrderBy([(#l_returnflag, True)])
	Aggregate: group_keys:[#l_returnflag], aggregate_count: 2
		Scan: './data/lineitem.csv'; projection=None
```

The physical plan is:

```text
OrderBy: [(#0, True)]
	HashAggregate: group_by: [#8]; aggregates: [Sum(#4), Avg(#6)]
		Scan: schema=Schema(Int32Type:l_orderkey, Int32Type:l_partkey, Int32Type:l_suppkey, Int32Type:l_linenumber, Int32Type:l_quantity, FloatType:l_extendedprice, FloatType:l_discount, FloatType:l_tax, StringType:l_returnflag, StringType:l_linestatus, StringType:l_shipdate, StringType:l_commitdate, StringType:l_receiptdate, StringType:l_shipinstruct, StringType:l_shipmode, StringType:l_comment), projection=None
```

The result is:

```python
["l_returnflag", "sum_#l_quantity", "avg_#l_discount"]
[
    ["A", "N", "R"],
    [45, 164, 119],
    [0.053333333333333344, 0.04666666666666666, 0.044],
]
```

## CSV Data Source

`CSVDataSource` reads one local CSV file into a single in-memory `RecordBatch`.
It does not chunk, stream, spill, or parallelize.

Type inference is intentionally simple:

- integers are detected with `str.isdigit()`;
- floats are detected by attempting `float(value)` when the value contains `.`;
- everything else is a string;
- empty values become `None` during scan;
- date parsing is available through an explicit `override_schema`.

For example, TPC-H Q1 uses `override_schema` to make `l_shipdate` a date:

```python
from querypy.planner.dataframe import DataFrame
from querypy.types_ import ArrowTypes, Field, Schema

df = DataFrame.scan_csv(
    "./data/lineitem.csv",
    override_schema=Schema([Field("l_shipdate", ArrowTypes.DateType)]),
)
```

## Optimizer

The optimizer currently contains one rule:

- `ProjectionPushDown`: walks a logical plan and pushes required column names
  into the `Scan` projection.

There is no predicate pushdown yet. A filter remains a `Filter` plan node above
its input unless the caller manually constructs a different plan.

## Running Tests

Install/use the project through `uv`, then run:

```bash
PYTHONPATH=. uv run pytest -q
```


## Current Limitations

This is a teaching implementation, so several database-engine pieces are
deliberately absent or incomplete:

- There is no SQL parser.
- There are no joins.
- There is no cost-based optimizer.
- There is no physical `GroupAggregate`; aggregation is hash-based only.
- There is no memory management, spilling, WAL, MVCC, locking, or storage layer.
- CSV scan reads one file into one in-memory batch.
- Boolean expression planning/execution is incomplete. Some logical operators
  exist as constructors, but not every operator is lowered or executable for
  every type.
- Interval support is backed by Python `datetime.timedelta`, so day intervals
  work naturally. Month and year intervals do not model PostgreSQL's
  calendar-dependent interval semantics.
- `OrderBy` uses a boolean flag where `True` means ascending and `False` means
  descending.

## Goals

- [x] Run the TPC-H Q1 shape over the bundled `lineitem` sample with asserted
  output.
- [x] Implement projection pushdown.
- [ ] Implement predicate pushdown.
- [ ] Implement common joins.
- [ ] Add a SQL parser.
- [ ] Add a cost-based optimizer.
- [ ] Improve type checking and operator support in the logical/physical
  expression layers.
- [ ] Improve documentation and docstrings for the important structures.
