import abc
import typing
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

    @classmethod
    def from_pyvalue(cls, v):
        match v:
            case str():
                return cls.StringType
            case int():
                return cls.Int32Type
            case _:
                raise Exception(f"Type {v} not supported")


class ColumnVectorABC(abc.ABC):
    """
    Represents a Column holding a vector of the
    same type.

    In the Python context, a vector is a list/tuple.
    """

    def __init__(self, type: ArrowType, value: list, size: int):
        self.type = type
        self.size = size
        self.value = value

    def __len__(self):
        return self.size

    @abc.abstractmethod
    def get_value(self, i):
        pass

    def __repr__(self):
        max_width = 60
        repr_ = repr(self.value)
        if len(repr_) > max_width:
            repr_ = repr_[:max_width] + "..."
            if not repr_.endswith("]"):
                repr_ += "]"
        return repr_


class ColumnVector(ColumnVectorABC):
    def get_value(self, i):
        if 0 > i <= len(self):
            raise IndexError()
        return self.value[i]


class LiteralValueVector(ColumnVectorABC):
    """
    Represents a vector that holds just a literal value, in every get_value
    it returns the value.
    """

    def __init__(self, type: ArrowType, value: typing.Any, size: int):
        super().__init__(type, value, size)

    def get_value(self, i):
        if 0 > i <= self.size:
            raise IndexError()
        return self.value


class Field:
    """
    Represents a field with a known name and data type.
    """

    def __init__(self, name: str, type: ArrowType):
        self.name = name
        self.type = type

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


class Schema:
    """
    Represents a collection of fields.
    """

    def __init__(self, fields: list[Field]):
        self.fields = fields

    def select(self, names: list[str]):
        """
        Returns a schema with the given names, if any individual name
        does not exist, it raises an Exception.
        """
        if not names:
            return self

        fields = [field for field in self.fields if field.name in names]
        return Schema(fields)

    def get_index_by_name(self, name: str):
        """Given a name, return's its index, if not found it returns -1"""
        for i, field in enumerate(self.fields):
            if field.name == name:
                return i
        return -1

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(map(lambda f: str(f.type) + ':' + f.name, self.fields))})"


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

    def column_names(self) -> list[str]:
        return list(map(lambda x: x.name, self.schema.fields))

    @classmethod
    def from_pylists(cls, schema: Schema, values: list[list]):
        """Builds a record batch  from a list holding lists of values.

        Example
        -------
        rb = RecordBatch.from_pylist(Schema(), [[1, 2, 3], ['v1', 'v2', 'v3']])
        """
        columns = []
        for column, values in zip(schema.fields, values):
            columns.append(ColumnVector(column.type, values, len(values)))
        return cls(schema, columns)

    @property
    def row_count(self):
        """
        The depth of rows of the columns.

        The total amount of rows of a RecordBatch can
         be calculated by `row_count x column_count`
        """
        if not self.fields:
            return 0
        return self.fields[0].size

    @property
    def column_count(self):
        return len(self.fields)

    def get_field(self, i):
        return self.fields[i]

    def __repr__(self):
        return f"{self.__class__.__name__}(fields={self.fields}, columns={self.column_names()}, num_cols={self.column_count}, row_count={self.row_count}"
