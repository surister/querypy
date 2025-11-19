"""
interface DataFrame {

  /** Apply a projection */
  fun project(expr: List<LogicalExpr>): DataFrame

  /** Apply a filter */
  fun filter(expr: LogicalExpr): DataFrame

  /** Aggregate */
  fun aggregate(groupBy: List<LogicalExpr>,
                aggregateExpr: List<AggregateExpr>): DataFrame

  /** Returns the schema of the data that will be produced by this DataFrame. */
  fun schema(): Schema

  /** Get the logical plan */
  fun logicalPlan() : LogicalPlan

}
"""
from querypy.datasources.csv import CSVDataSource
from querypy.planner import LogicalExpression
from querypy.planner import LogicalPlan
from querypy.planner.expressions import AggregateExpr
from querypy.planner.expressions import ColumnarExpr
from querypy.planner.expressions import Eq
from querypy.planner.expressions import LiteralExpr
from querypy.planner.expressions import LiteralLongExpr
from querypy.planner.logical import Aggregate
from querypy.planner.logical import Filter
from querypy.planner.logical import Projection
from querypy.planner.logical import Scan
from querypy.types_ import Schema


class DataFrame:
    def __init__(self, plan: LogicalPlan):
        self._plan = plan

    def select(self, expr: list[LogicalExpression] | list[str]) -> "DataFrame":
        if expr and isinstance(expr[0], str):
            expr = [ColumnarExpr(col) for col in expr]
        return DataFrame(Projection(self._plan, expr))

    def filter(self, expr: str | LogicalExpression):
        """
        Filter, supports direct Expression or common syntax.

        Examples:

        >>> filter(country='ES')
        >>> filter('value=1')
        >>> filter(col('country') == lit('ES'))

        """
        if isinstance(expr, str):
            print(expr)
            l, r = expr.split('=')
            if not l or not r:
                raise SyntaxError('Filter syntax is not correct')
            r = r.strip()

            if r.isdigit():
                r = LiteralLongExpr(int(r))
            else:
                # naive support comparison between columns.
                if "'" in r:
                    r = r.replace("'", "")
                    r = LiteralExpr(str(r))
                else:
                    r = r.replace('"', '')
                    r = ColumnarExpr(str(r))

            expr = Eq(ColumnarExpr(l.strip()), r)
        return DataFrame(Filter(self._plan, expr))

    def aggregate(self, group_by: list[LogicalExpression] | list[str], aggr: list[AggregateExpr]):

        if group_by and isinstance(group_by[0], str):
            group_by = [ColumnarExpr(col) for col in group_by]
        return DataFrame(Aggregate(self._plan, group_by, aggr))

    def schema(self) -> Schema:
        return self._plan.get_schema()

    def logical_plan(self) -> LogicalPlan:
        return self._plan

    @classmethod
    def scan_csv(cls, path: str, fields: list[str] = None) -> "DataFrame":
        return DataFrame(Scan(path, CSVDataSource(path), fields or []))


class LogicalExpressionWrapper:
    def __init__(self, log: LogicalExpression, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def col(cls, value):
        return ColumnarExpr(value)


def col(name: str) -> ColumnarExpr:
    """
    Reference to a column.
    """
    return ColumnarExpr(name)


def lit(value: int | str) -> LogicalExpression:
    """Reference to a value, it can be either an integer or a string."""
    if isinstance(value, int):
        return LiteralLongExpr(value)
    return LiteralExpr(value)


