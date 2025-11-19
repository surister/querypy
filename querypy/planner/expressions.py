import functools

from querypy.planner import LogicalExpression
from querypy.planner.logical import LogicalPlan
from querypy.types_ import ArrowTypes
from querypy.types_ import Field


class ColumnarExpr(LogicalExpression):
    """
    Represents a reference to a named column.
    """

    def __init__(self, name: str):
        self.name = name

    def to_field(self, input: LogicalPlan) -> Field:
        for field in input.get_schema().fields:
            if field.name == self.name:
                return field
        raise Exception(f"No column named {self.name}")

    def __repr__(self):
        return "#" + self.name


class LiteralExpr(LogicalExpression):
    """
    Represents a literal value, e.g. 'num_eggs / 12'
    """

    def __init__(self, expr: str):
        self.expr = expr

    def to_field(self, input: LogicalPlan):
        return Field(self.expr, ArrowTypes.StringType)

    def __repr__(self):
        return repr(self.expr)


class LiteralLongExpr(LogicalExpression):
    """
    Represents a literal long value.
    """

    def __init__(self, lit: int):
        self.lit = lit

    def to_field(self, input: LogicalPlan):
        return Field(str(self.lit), ArrowTypes.Int64Type)

    def __repr__(self):
        return repr(self.lit)


class BinaryExpr(LogicalExpression):
    def __init__(self, name: str, op: str, l: LogicalExpression, r: LogicalExpression):
        self.name = name
        self.op = op
        self.l = l
        self.r = r

    def to_field(self, input: LogicalPlan):
        raise NotImplemented()

    def __repr__(self):
        return f"{self.l} {self.op} {self.r}"


class MathExpr(LogicalExpression):
    def to_field(self, input: LogicalPlan) -> Field:
        return Field("mult", self.l.to_field(input).type)


class BooleanBinaryExpr(BinaryExpr):
    def to_field(self, input: LogicalPlan):
        return Field(self.name, ArrowTypes.BooleanType)


class AggregateExpr(LogicalExpression):
    def __init__(self, name: str, expr: LogicalExpression):
        self.name = name
        self.expr = expr

    def to_field(self, input: LogicalPlan):
        return Field(self.name, self.expr.to_field(input).type)

    def __repr__(self):
        return f"{self.name}({self.expr})"


class CountAggregateExpr(AggregateExpr):
    def to_field(self, input: LogicalPlan):
        return Field("COUNT", ArrowTypes.Int32Type)


def _boolean_binary_expr(
        name: str, op: str, l: LogicalExpression, r: LogicalExpression
):
    return BooleanBinaryExpr(name, op, l, r)


def _math_expr(name: str, op: str, l: LogicalExpression, r: LogicalExpression):
    return MathExpr(name, op, l, r)


def _aggregate_expr(name: str, input: LogicalPlan):
    return AggregateExpr(name, input)


# We implement operators in the literals, this allows us to have a easy to use
# api, like col('some_col) != 1)
# We could do fancy macro's/code generation here, let's not. It'll be easier to read
# to implement every method in single lines.

# Logical
Eq = functools.partial(_boolean_binary_expr, "eq", "=")
Neq = functools.partial(_boolean_binary_expr, "neq", "!=")
Gt = functools.partial(_boolean_binary_expr, "gt", ">")
GtEq = functools.partial(_boolean_binary_expr, "gteq", ">=")
Lt = functools.partial(_boolean_binary_expr, "lt", "<")
LtEq = functools.partial(_boolean_binary_expr, "lteq", "<=")

And = functools.partial(_boolean_binary_expr, "and", "AND")
Or = functools.partial(_boolean_binary_expr, "or", "OR")

ColumnarExpr.__eq__ = lambda s, o: Eq(s, o)
LiteralExpr.__eq__ = lambda s, o: Eq(s, o)

ColumnarExpr.__ne__ = lambda s, o: Neq(s, o)
LiteralExpr.__ne__ = lambda s, o: Neq(s, o)

ColumnarExpr.__gt__ = lambda s, o: Gt(s, o)
LiteralExpr.__gt__ = lambda s, o: Gt(s, o)

ColumnarExpr.__ge__ = lambda s, o: GtEq(s, o)
LiteralExpr.__ge__ = lambda s, o: GtEq(s, o)

ColumnarExpr.__and__ = lambda s, o: Eq(s, o)
LiteralExpr.__eq__ = lambda s, o: Eq(s, o)

# Math.
Add = functools.partial(_math_expr, "add", "+")
Subtract = functools.partial(_math_expr, "subtract", "-")

ColumnarExpr.__add__ = lambda s, o: Sum(s, o)
LiteralExpr.__sub__ = lambda s, o: Subtract(s, o)

Sum = functools.partial(_aggregate_expr, "SUM")
Min = functools.partial(_aggregate_expr, "MIN")
