from querypy.datasources.csv import CSVDataSource
from querypy.planner.expressions import (
    LogicalExpression,
    LogicalPlan,
    logical as logical_expression,
)
from querypy.planner.expressions.logical import Column
from querypy.planner.plans import logical as logical_plan
from querypy.types_ import Schema


class DataFrame:
    """A DataFrame API class that makes creating logical plans easier..

    Methods
    -------
    select(expr: list[LogicalExpression] | list[str])
        Adds a projection plan, it parses strings into `logical.Column`
    filter(expr: str | LogicalExpression)
        Adds a filter plan, it parses strings into `logical.Column`
    aggregate(group_by: list[LogicalExpression] | list[str], aggr: list[AggregateExpr])
        Adds an aggregate plan.
    schema()
        The schema of the logical plan.
    logical_plan()
        The logical plan.
    """

    def __init__(self, plan: LogicalPlan):
        self._plan = plan

    def select(
        self, columns: list[logical_expression.Column] | list[str]
    ) -> "DataFrame":
        """Selects the given columns.

        Parameters
        ----------
        columns : list[LogicalExpression] | list[str]
            The list of columns to select.

        Returns
        -------
        DataFrame
            A dataframe with a projection in its query plan.
        """

        match columns:
            # This only checks that the first one is a string.
            case [str(),*_]:
                columns = [Column(col) for col in columns]
        return DataFrame(logical_plan.Projection(self._plan, columns))

    def filter(self, expr: str | logical_expression.Boolean) -> "DataFrame":
        """Applies a filter plan.

        Parameters
        ----------
        expr : str | LogicalExpression
            The expressions to filter the dataframe.

        Returns
        -------
        DataFrame
            A dataframe with a filter in its query plan.

        Raises
        ------
        SyntaxError
            If the filter syntax is not correct.

        Examples
        --------
        """
        if isinstance(expr, str):
            l, r = None, None
            # this parsing logic is very easy to break, but it's fine as we don't
            # want to implement more complex parsing capabilities.
            for op in logical_expression.boolean_operators[
                ::-1
            ]:  # try to match longe operators first
                if op in expr:
                    l, r = expr.split(op)
                    continue

            if not l or not r:
                raise SyntaxError("Filter syntax is not correct")

            r = r.strip()

            if r.isdigit():
                r = logical_expression.LiteralInteger(int(r))
            else:
                # naive support comparison parsing between columns.
                if "'" in r:
                    r = r.replace("'", "")
                    r = logical_expression.LiteralString(str(r))
                else:
                    r = r.replace('"', "")
                    r = logical_expression.Column(str(r))

            expr = logical_expression.Eq(logical_expression.Column(l.strip()), r)
        return DataFrame(logical_plan.Filter(self._plan, expr))

    def aggregate(
        self,
        group_by: list[LogicalExpression] | list[str],
        aggr: list[logical_expression.Aggregate],
    ):
        """_summary_.

        Parameters
        ----------
        group_by : list[LogicalExpression] | list[str]
            _description_
        aggr : list[AggregateExpr]
            _description_

        Returns
        -------
        _type_
            _description_
        """
        if group_by and isinstance(group_by[0], str):
            group_by = [logical_expression.Column(col) for col in group_by]

        return DataFrame(logical_plan.Aggregate(self._plan, group_by, aggr))

    def schema(self) -> Schema:
        return self._plan.get_schema()

    def logical_plan(self) -> LogicalPlan:
        return self._plan

    @classmethod
    def scan_csv(cls, path: str, fields: list[str] = None) -> "DataFrame":
        """Reads the `fields` from a csv files in a given `path`.

        It performs very basic csv parsing, more diverse csv formats might not be
        compatible.

        Parameters
        ----------
        path : str
            The local filesystem path for the csv. e.g. 'employees.csv', it only supports
             one file.
        fields : list[str]
            The fields to read (Default value = None)

        Returns
        -------
        'DataFrame'
            A dataframe with a plan to read csv in its logical plan.
        """
        return DataFrame(logical_plan.Scan(path, CSVDataSource(path), fields))


def col(name: str) -> logical_expression.Column:
    """Reference to a column.

    Parameters
    ----------
    name : str
        The name of a column.

    Returns
    -------
    ColumnarExpr
        A reference to that column.
    """
    return logical_expression.Column(name)


def lit(value: int | str) -> logical_expression.Literal:
    """Reference to a literal value, it can be either a string or an integer.

    Parameters
    ----------
    value : int | str
        The value to reference.

    Returns
    -------
    LogicalExpression
        The reference to the literal value.
    """
    if isinstance(value, int):
        return logical_expression.LiteralInteger(value)
    return logical_expression.LiteralString(value)
