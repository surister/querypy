import abc
from enum import Enum


class ArrowType:
    """Represents an arrow type"""

    def __init__(self, name: str = ""):
        self.name = name

    def __repr__(self):
        return self.name


class IntType(ArrowType):
    def __init__(self, maxsize: int, some: bool):
        super().__init__()
        self.maxsize = maxsize
        self.some = some


class FloatingPointPrecision(Enum):
    SINGLE = "single"
    DOUBLE = "double"


class FloatingPoint(ArrowType):
    def __init__(self, precision: FloatingPointPrecision):
        super().__init__()
        self.precision = precision


class NamedParameters(type):
    """
    Metaclass that adds an attribute `name` to all class attributes. The value
     of the name will be the variable literal that was used.

    Example
    -------
    class SomeAttribute:
        pass

    class SomeClass(metaclass=NamedParameters):
        one = SomeAttribute()

    After creation, `SomeClass` will be added an attribute `name` with
    the value `one` (str).

    """

    def __new__(cls, name, bases, attrs, **kwargs):
        super_new = super().__new__
        _new_class = super_new(cls, name, bases, attrs, **kwargs)

        for k, v in attrs.items():
            if isinstance(v, ArrowType):
                v.name = k
        return _new_class


class ArrowTypes(metaclass=NamedParameters):
    BooleanType = ArrowType()
    Int8Type = IntType(8, True)
    Int16Type = IntType(16, True)
    Int32Type = IntType(32, True)
    Int64Type = IntType(64, True)
    UInt8Type = IntType(8, True)
    UInt16Type = IntType(16, True)
    UInt32Type = IntType(32, True)
    UInt64Type = IntType(64, True)
    FloatType = FloatingPoint(FloatingPointPrecision.SINGLE)
    DoubleType = FloatingPoint(FloatingPointPrecision.DOUBLE)
    StringType = ArrowType()


class ColumnVector(abc.ABC):
    """
    Represents a Column holding a vector of the
    same type.

    In the Python context, a vector is a list/tuple.
    """

    def __init__(self, type: ArrowType, size: int):
        self.type = type
        self.size = size

    def __len__(self):
        return self.size

    @abc.abstractmethod
    def get_value(self, i):
        pass


class LiteralValueVector(ColumnVector):
    def __init__(self, type: ArrowType, size: int):
        super().__init__(type, size)

    def get_value(self, i):
        return None


class Field:
    """
    Represents a field in a known schema.
    """

    def __init__(self, name: str, type: ArrowType):
        self.name = name
        self.type = type


class Schema:
    """
    Represents a collection of fields.
    """

    def __init__(self, fields: list[Field]):
        self.fields = fields


class RecordBatch:
    """
    Represents a collection of same length columns (ColumVector).

    A record batch is composed of several columns, e.g. ['a', 'b'] that have
    vectors of values. e.g. [1, 2, 3]

         +---+-----+
         | a |  b  |
         +---+-----+
         | 1 | 'a' |
         | 2 | 'b' |
         | 3 | 'c' |
         +---+-----+

    """

    def __init__(self, schema: Schema, fields: list[ColumnVector]):
        self.schema = schema
        self.fields = fields

    @property
    def row_count(self):
        """
        The depth of rows of the columns.

        The total amount of rows of a RecordBatch can
         be calculated by `row_count x column_count`
        """
        if not self.fields:
            return 0
        return len(self.fields[0])

    @property
    def column_count(self):
        return len(self.fields)

    def get_field(self, i):
        return self.fields[i]
