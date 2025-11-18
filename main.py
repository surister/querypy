from querypy.datasources.csv import CSVDataSource
from querypy.planner.expressions import ColumnarExpr
from querypy.planner.expressions import Eq
from querypy.planner.expressions import LiteralExpr
from querypy.planner.logical import Filter
from querypy.planner.logical import Projection
from querypy.planner.logical import Scan

# SELECT id, state FROM employee WHERE state = 'CO'
# csv: id, first_name, second_name, state, job_title, salary
csv = CSVDataSource('employee.csv')
scan = Scan('employee', csv, [])
filterExpr = Eq(ColumnarExpr("state"), LiteralExpr("CO"))
selection = Filter(scan, filterExpr)
projection_elements = [
    ColumnarExpr("id"),
    ColumnarExpr("state")
]

plan = Projection(selection, projection_elements)

print(plan.get_tree())

# Projection(#id, #state)
# 	Filter: #state = 'CO'
# 		Scan: employee; projection=[]

Projection(
    Filter(
        Scan("employee", CSVDataSource("employee.csv"), []),
        Eq(ColumnarExpr("country"), LiteralExpr("ES"))
    ),
    [ColumnarExpr("id"),
     ColumnarExpr("country")]
)
print(plan.get_tree())
