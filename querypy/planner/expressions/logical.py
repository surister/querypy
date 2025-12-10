import functools
from enum import Enum

from querypy.exceptions import UnknownColumnError, AlreadyExistsColumnError
from querypy.planner.expressions import LogicalExpression
from querypy.planner.expressions import LogicalPlan
from querypy.types_ import ArrowTypes
from querypy.types_ import Field


class Column(LogicalExpression):
    """Represents a reference to a named column."""

    def __init__(self, name: str):
        """
        Parameters
        ----------
        name : str
            The name of the column.
        """
        self.name = name

    def to_field(self, input: LogicalPlan) -> Field:
        """The column on its `Field` representation.

        Parameters
        ----------
        input : LogicalPlan
            The logical plan where the column name will be extracted from.

        Returns
        -------
        Field
            A field.

        Raises
        ------
        QueryEngineError
            If the column does not exist in the logical plan schema.
        """
        for field in input.get_schema().fields:
            if field.name == self.name:
                return field
        raise UnknownColumnError(self.name)

    def __repr__(self):
        return "#" + self.name


class Literal(LogicalExpression):
    """Represents an untyped literal"""

    def to_field(self, input: "LogicalPlan") -> Field:
        raise NotImplemented()


class LiteralString(Literal):
    """Represents a literal string value

    Example
    -------
    >>> LiteralString('somevalue')
    'somevalue'
    """

    def __init__(self, value: str):
        self.value = value

    def to_field(self, input: LogicalPlan):
        return Field(self.value, ArrowTypes.StringType)

    def __repr__(self):
        return repr(self.value)


class LiteralInteger(Literal):
    """Represents a literal long value"""

    def __init__(self, value: int):
        self.value = value

    def to_field(self, _: LogicalPlan):
        return Field(str(self.value), ArrowTypes.Int64Type)

    def __repr__(self):
        return repr(self.value)


class LiteralFloat(Literal):
    """Represents a literal float value"""

    def __init__(self, value: float):
        self.value = value

    def __repr__(self):
        return repr(self.value)

    def to_field(self, _: LogicalPlan):
        return Field(str(self.value), ArrowTypes.FloatType)


class Binary(LogicalExpression):
    """An expression that represents a binary operation, binary in the sense
    that two operands interact in an operation. For example the sum of two integers (a + b)
    would be a binary operations where the left operand (a) interacts with the right operand (r)
    in a sum operation.

    This class is the base class to implement other binary operations like
     sum, equality, reduction...

    Attributes
    ----------
    name : str
        The name of the operation.
    op : str
        The character(s) that represent tha operations, e.g. in a sum it would be the character `+`.
    l : LogicalExpression
        The left operand of the operation.
    r : LogicalExpression
        The right operand of the operation.
    """

    def __init__(self, name: str, op: str, l: LogicalExpression, r: LogicalExpression):
        self.name = name
        self.op = op
        self.l = l
        self.r = r

    def to_field(self, input: LogicalPlan):
        """Not implemented."""
        raise NotImplementedError()

    def __repr__(self):
        return f"{self.l} {self.op} {self.r}"


class Boolean(Binary):
    """A binary expression whose results are boolean: True/False.

    Use to implement things like equality operators (=, <, =>...)
    """

    def to_field(self, input: LogicalPlan):
        return Field(self.name, ArrowTypes.BooleanType)


def _boolean_expression(name: str, op: str, l: LogicalExpression, r: LogicalExpression):
    """Constructor for boolean expressions."""
    return Boolean(name, op, l, r)


class BooleanOp(Enum):
    """Represent an operation with a boolean result."""

    EQ = ("eq", "=")
    NEQ = ("neq", "!=")
    GT = ("gt", ">")
    GTEQ = ("gteq", ">=")
    LT = ("lt", "<")
    LTEQ = ("lteq", "<=")
    AND = ("and", "AND")
    OR = ("or", "OR")

    def __init__(self, logical_name: str, symbol: str):
        self.logical_name = logical_name
        self.symbol = symbol


Eq = functools.partial(
    _boolean_expression, BooleanOp.EQ.logical_name, BooleanOp.EQ.symbol
)
Neq = functools.partial(
    _boolean_expression, BooleanOp.NEQ.logical_name, BooleanOp.NEQ.symbol
)
Gt = functools.partial(
    _boolean_expression, BooleanOp.GT.logical_name, BooleanOp.GT.symbol
)
GtEq = functools.partial(
    _boolean_expression, BooleanOp.GTEQ.logical_name, BooleanOp.GTEQ.symbol
)
Lt = functools.partial(
    _boolean_expression, BooleanOp.LT.logical_name, BooleanOp.LT.symbol
)
LtEq = functools.partial(
    _boolean_expression, BooleanOp.LTEQ.logical_name, BooleanOp.LTEQ.symbol
)
And = functools.partial(
    _boolean_expression, BooleanOp.AND.logical_name, BooleanOp.AND.symbol
)
Or = functools.partial(
    _boolean_expression, BooleanOp.OR.logical_name, BooleanOp.OR.symbol
)

# Literals implement some python dunder methods that allow us to easily construct advanced
# expressions more naturally, for example `Column('salary') > LiteralInteger(1000)` will return a
# `Gt(Column('salary'), LiteralInteger(1000))`
# We could do fancy macro's/code generation here, but let's not. It'll be easier to read and
# to implement every method in single lines.

Column.__eq__ = lambda s, o: Eq(s, o)
LiteralString.__eq__ = lambda s, o: Eq(s, o)
LiteralFloat.__eq__ = lambda s, o: Eq(s, o)
LiteralInteger.__eq__ = lambda s, o: Eq(s, o)

Column.__ne__ = lambda s, o: Neq(s, o)
LiteralString.__ne__ = lambda s, o: Neq(s, o)
LiteralInteger.__ne__ = lambda s, o: Neq(s, o)
LiteralFloat.__ne__ = lambda s, o: Neq(s, o)

Column.__gt__ = lambda s, o: Gt(s, o)
LiteralString.__gt__ = lambda s, o: Gt(s, o)
LiteralInteger.__gt__ = lambda s, o: Gt(s, o)
LiteralFloat.__gt__ = lambda s, o: Gt(s, o)

Column.__ge__ = lambda s, o: GtEq(s, o)
LiteralString.__ge__ = lambda s, o: GtEq(s, o)
LiteralInteger.__ge__ = lambda s, o: GtEq(s, o)
LiteralFloat.__ge__ = lambda s, o: GtEq(s, o)

Column.__and__ = lambda s, o: And(s, o)
LiteralString.__eq__ = lambda s, o: And(s, o)
LiteralInteger.__eq__ = lambda s, o: And(s, o)
LiteralFloat.__eq__ = lambda s, o: And(s, o)

Column.__and__ = lambda s, o: Or(s, o)
LiteralString.__and__ = lambda s, o: Or(s, o)
LiteralInteger.__and__ = lambda s, o: Or(s, o)
LiteralFloat.__and__ = lambda s, o: Or(s, o)


class MathExpr(Binary):
    """A mathematical expression."""

    def to_field(self, input: LogicalPlan) -> Field:
        return Field(self.name, self.l.to_field(input).type)


def _math_expression(name: str, op: str, l: LogicalExpression, r: LogicalExpression):
    """Constructor for mathematical expressions"""
    return MathExpr(name, op, l, r)


class MathOp(Enum):
    """Represent an operation with a numeric result."""

    Add = ("add", "+")
    Subtract = ("sub", "-")
    Multiply = ("mul", "*")
    Divide = ("div", "/")

    def __init__(self, logical_name: str, symbol: str):
        self.logical_name = logical_name
        self.symbol = symbol


# There are other mathematical expressions that could be implemented
# but are not because no particular reason other than being brief.
# pr's are welcome.
Add = functools.partial(_math_expression, MathOp.Add.name, MathOp.Add.symbol)
Subtract = functools.partial(
    _math_expression, MathOp.Subtract.name, MathOp.Subtract.symbol
)
Multiply = functools.partial(
    _math_expression, MathOp.Multiply.name, MathOp.Multiply.symbol
)
Divide = functools.partial(_math_expression, MathOp.Divide.name, MathOp.Divide.symbol)

Column.__add__ = lambda s, o: Add(s, o)
LiteralString.__add__ = lambda s, o: Add(s, o)
LiteralInteger.__add__ = lambda s, o: Add(s, o)
LiteralFloat.__add__ = lambda s, o: Add(s, o)

Column.__sub__ = lambda s, o: Subtract(s, o)
LiteralString.__sub__ = lambda s, o: Subtract(s, o)
LiteralInteger.__sub__ = lambda s, o: Subtract(s, o)
LiteralFloat.__sub__ = lambda s, o: Subtract(s, o)


class Aggregate(LogicalExpression):
    """An aggregate expression, aggregations typically return a scalar value from
    a set of values, for example the count of a column.

    Attributes
    ----------
    name : str
        The name of the aggregation.
    expr : LogicalExpression
        The internal logical expression. An aggregate expression is an aggregate
         over another expression, for example: Sum(Column('salary')).

    Methods
    -------
    to_field(input: LogicalPlan)
        The field representing the expression.
    """

    def __init__(self, name: str, expr: LogicalExpression):
        self.name = name
        self.expr = expr

    def to_field(self, input: LogicalPlan):
        return Field(
            f"{self.name.lower()}_{str(self.expr)}", self.expr.to_field(input).type
        )

    def __repr__(self):
        return f"{self.name}({self.expr})"


def _aggregate_expression(name: str, input: LogicalExpression):
    """Constructor for aggregate expressions"""
    return Aggregate(name, input)


Count = functools.partial(_aggregate_expression, "COUNT")
Max = functools.partial(_aggregate_expression, "MAX")
Min = functools.partial(_aggregate_expression, "MIN")
Sum = functools.partial(_aggregate_expression, "SUM")
Avg = functools.partial(_aggregate_expression, "AVG")


class Alias(LogicalExpression):
    """
    Renames the given column to the new name if unless it's already in use.
    """

    def __init__(self, name: str, expr: Column):
        self.name = name
        self.expr = expr

    def to_field(self, input: "LogicalPlan") -> Field:
        for field in input.get_schema().fields:
            if field.name == self.name and field is not self:
                raise AlreadyExistsColumnError(self.name)

        field = self.expr.to_field(input)
        return Field(self.name, field.type)

    def __repr__(self):
        return f"{self.expr} as #{self.name}"
