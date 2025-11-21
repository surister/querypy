from querypy.planner.expressions import PhysicalExpression
from querypy.types_ import ArrowType
from querypy.types_ import ArrowTypes
from querypy.types_ import ColumnVector
from querypy.types_ import LiteralValueVector
from querypy.types_ import RecordBatch


class Column(PhysicalExpression):
    """
    Represents a physical column by its index (i) within the record batch.
    """

    def __init__(self, i: int):
        self.i = i

    def evaluate(self, input: RecordBatch) -> ColumnVector:
        """

        Returns
        -------
        The actual data wrapped in a `ColumnVector`
        """
        return input.get_field(self.i)

    def __repr__(self):
        return f"#{self.i}"


class LiteralString(PhysicalExpression):
    """
    Represents a vector of literal strings.
    """

    def __init__(self, value: str):
        self.value = value

    def evaluate(self, input: RecordBatch) -> ColumnVector:
        return LiteralValueVector(ArrowTypes.StringType, [self.value], input.row_count)


class Binary(PhysicalExpression):
    """
    Physical implementation of a binary operation.
    """

    def __init__(self, l: PhysicalExpression, r: PhysicalExpression):
        self.l = l
        self.r = r

    def evaluate(self, input: RecordBatch) -> ColumnVector:
        ll = self.l.evaluate(input)
        lr = self.r.evaluate(input)
        assert ll.size == lr.size, "columns have distinct row sizes"

        if ll.type != lr.type:
            raise TypeError("Binary expression operands do not have the same type")
        return self.evaluate(ll, lr)  # no way this is right


class Eq(PhysicalExpression):
    """
    Physical implementation of equality.
    """

    def __init__(self, l: PhysicalExpression, r: PhysicalExpression):
        self.l = l
        self.r = r

    def evaluate(self, l, r, arrowtype: ArrowType) -> bool:
        return l.evaluate() == r.evaluate()
