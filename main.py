from querypy.datasources.csv import CSVDataSource
from querypy.planner.dataframe import DataFrame
from querypy.planner.dataframe import col
from querypy.planner.dataframe import lit
from querypy.planner.expressions import ColumnarExpr
from querypy.planner.expressions import Eq
from querypy.planner.expressions import LiteralExpr
from querypy.planner.expressions import Sum
from querypy.planner.logical import Aggregate
from querypy.planner.logical import Filter
from querypy.planner.logical import Projection
from querypy.planner.logical import Scan

# SELECT id, state FROM employee WHERE state = 'CO'

"""
SELECT id,
       country,
       salary
FROM employee
WHERE country = 'ES'
  AND salary > 40_000
GROUP BY salary
"""

# This is the order of the query generated query plan, scan -> group by -> filter -> projection
# we filter late on purpose to later test the filter push down optimization.

# csv: id, first_name, second_name, state, job_title, salary
csv = CSVDataSource("employee.csv")
scan = Scan("employee", csv, [])

group_by = Aggregate(scan, [ColumnarExpr("id"), ColumnarExpr("country")],
                     [Sum(ColumnarExpr("salary"))])
filter = Filter(group_by, Eq(ColumnarExpr("country"), LiteralExpr("ES")))
projection_elements = [ColumnarExpr("id"), ColumnarExpr("country"), ColumnarExpr("salary")]
plan = Projection(filter, projection_elements)

print('free plan:\n', plan.get_tree())

plan = Projection(
    Filter(
        Aggregate(
            Scan("employee", CSVDataSource("employee.csv"), []),
            [ColumnarExpr("id"), ColumnarExpr("country")], [Sum(ColumnarExpr("salary"))]
        ),
        Eq(ColumnarExpr("country"), LiteralExpr("ES")),
    ),
    [ColumnarExpr("id"), ColumnarExpr("country"), ColumnarExpr("salary")],
)
print('compact plan:\n', plan.get_tree())

df = (DataFrame
      .scan_csv('somefiles.csv')
      .aggregate([ColumnarExpr("country"), ColumnarExpr("salary")], [Sum(ColumnarExpr("salary"))])
      .filter(Eq(ColumnarExpr("country"), LiteralExpr("ES")))
      .select([ColumnarExpr("id"), ColumnarExpr("country"), ColumnarExpr("salary")])
      )

print('dataframe free:\n', df.logical_plan().get_tree())

df = (DataFrame
      .scan_csv('somefiles.scv')
      .aggregate(['salary'], Sum(ColumnarExpr("salary")))
      .filter("country = 'ES'")
      .select(["id", "country", "salary"])
      )



print('dataframe compact:\n', df.logical_plan().get_tree())

"""
val df = ctx.csv(employeeCsv)
   .filter(col("state") eq lit("CO"))
   .select(listOf(
       col("id"),
       col("first_name"),
       col("last_name"),
       col("salary"),
       (col("salary") mult lit(0.1)) alias "bonus"))
   .filter(col("bonus") gt lit(1000))"""

