import abc
import typing

from querypy.planner.expressions import PhysicalExpression
from querypy.types_ import ArrowType
from querypy.types_ import ArrowTypes
from querypy.types_ import ColumnVector
from querypy.types_ import ColumnVectorABC
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

    def evaluate(self, input: RecordBatch) -> ColumnVectorABC:
        return LiteralValueVector(ArrowTypes.StringType, self.value, input.row_count)


class LiteralInteger(PhysicalExpression):
    """
    Represents a vector of literal integers.
    """

    def __init__(self, value: int):
        self.value = value

    def evaluate(self, input: RecordBatch) -> ColumnVectorABC:
        return LiteralValueVector(ArrowTypes.Int32Type, self.value, input.row_count)


class LiteralFloat(PhysicalExpression):
    """
    Represents a vector of literal integers.
    """

    def __init__(self, value: float):
        self.value = value

    def evaluate(self, input: RecordBatch) -> ColumnVectorABC:
        return LiteralValueVector(ArrowTypes.FloatType, self.value, input.row_count)


class Binary(PhysicalExpression):
    """
    Physical implementation of a binary operation.
    """

    def __init__(self, l: PhysicalExpression, r: PhysicalExpression):
        self.l = l
        self.r = r

    def evaluate(self, input: RecordBatch) -> tuple[ColumnVectorABC, ColumnVectorABC]:
        ll = self.l.evaluate(input)
        lr = self.r.evaluate(input)

        assert ll.size == lr.size, "columns have distinct row sizes"
        if ll.type != lr.type:
            raise TypeError(
                f"Binary expression operands do not have the same type: l {ll.type} r {lr.type}"
            )
        return ll, lr


class Boolean(Binary):
    def evaluate(self, input: RecordBatch) -> ColumnVectorABC:
        ll, lr = super().evaluate(input)
        mask = [
            int(self.compare(ll.get_value(i), lr.get_value(i), ll.type))
            for i in range(len(ll))
        ]
        return ColumnVector(ArrowTypes.Int8Type, mask, ll.size)

    @abc.abstractmethod
    def compare(self, l, r, t: ArrowType) -> bool:
        """Evaluates the left and right value to boolean operation"""
        pass


class Eq(Boolean):
    """
    Physical implementation of equality.
    """

    def compare(self, l, r, t) -> bool:
        # We currently don't use the type, since most python object
        # implement equality without much work.
        return l == r


class Gt(Boolean):
    def compare(self, l, r, t: ArrowType) -> bool:
        return l > r


class Lt(Boolean):
    def compare(self, l, r, t: ArrowType) -> bool:
        return l < r


class Accumulator(abc.ABC):
    @abc.abstractmethod
    def accumulate(self, value):
        pass

    @abc.abstractmethod
    def final_value(self) -> typing.Any:
        pass


class MaxAccumulator(Accumulator):
    def __init__(self):
        self.accumulated_values = 0
        self.value = None

    def accumulate(self, value):
        if self.value is None or value > self.value:
            self.value = value
        self.accumulated_values += 1

    def final_value(self) -> typing.Any:
        return self.value

    def __repr__(self):
        return self.__class__.__name__ + f'(accumulated_values={self.accumulated_values}, value={self.value})'


class Aggregate(PhysicalExpression, abc.ABC):
    def __init__(self, expr: PhysicalExpression):
        self.expr = expr

    @abc.abstractmethod
    def create_accumulator(self) -> Accumulator:
        pass


class Max(Aggregate):
    def create_accumulator(self) -> Accumulator:
        return MaxAccumulator()

    def evaluate(self, input: RecordBatch) -> ColumnVector:
        pass

    def __repr__(self):
        return self.__repr__() + f"({self.expr})"
