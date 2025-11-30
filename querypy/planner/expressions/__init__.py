import abc

from querypy.types_ import ColumnVector, RecordBatch, Schema
from querypy.types_ import Field


class LogicalPlan(abc.ABC):
    """
    Represents a relation (a set of tuples) with a known schema.

    Logical plans contain children logical plans, forming a Tree. They only hold
    logical information and are typically composed by other `LogicalPlan`s and
    `LogicalExpression`s. It does not know anything about the actual physical data
    layout or how things will be executed.

    You can think of it as the schematics of a room, where many together compose the
    schematics of an entire house. It tells you what to do, not how to do it.
    """

    @abc.abstractmethod
    def get_schema(self) -> Schema:
        pass

    @abc.abstractmethod
    def children(self) -> list["LogicalPlan"]:
        pass

    def __repr__(self):
        return self.__class__.__name__


class LogicalExpression(abc.ABC):
    """
    Represents an Expression on its logical form, meaning that it just
    holds logical information and references to typed values.
    """

    @abc.abstractmethod
    def to_field(self, input: "LogicalPlan") -> Field:
        """
        Returns the field representation of the Expression, not every expression
        will have this implemented, as some cannot be represented as Field.

        Parameters
        ----------
        input : LogicalPlan
            The logical plan from which this Expression belongs to.

        Returns
        -------
        Field
            The field representing the Expression.

        Raises
        ------
        NotImplementedError
            If not implemented.
        """
        pass


class PhysicalPlan(abc.ABC):
    @abc.abstractmethod
    def schema(self) -> Schema:
        pass

    @abc.abstractmethod
    def execute(self) -> list[RecordBatch]:
        pass

    @abc.abstractmethod
    def children(self) -> list["PhysicalPlan"]:
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}: "


class PhysicalExpression(abc.ABC):
    """
    The physical representation of logical expressions, meaning that it knows
    about the data layout and have specific working implementations.

    An example is that in columnar data, in a logical form we would
    reference a column by its name, but it's physical reference
    would be the index of the column, for performance reasons.

    One logical expression might have different physical expressions. For example
    a join can be implemented using different algorithms: nested loop join, hash join...
    """

    @abc.abstractmethod
    def evaluate(self, input: RecordBatch) -> ColumnVector:
        """
        The evaluation of the specific implementation of the Expressions, it returns real, physical,
        modified data.
        """
        pass

    def __repr__(self):
        return self.__class__.__name__
