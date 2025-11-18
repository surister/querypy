import abc

from querypy.types_ import RecordBatch
from querypy.types_ import Schema


class LogicalPlan(abc.ABC):
    """
    Represents a relation (a set of tuples) with a known schema.

    Logical plans contain children logical plans.,
    """

    @abc.abstractmethod
    def get_schema(self) -> Schema:
        pass

    @abc.abstractmethod
    def children(self) -> list["LogicalPlan"]:
        pass

    def get_tree(self, indent: int = 0):
        """
        Original tree printing code in Kotlin from: _How query engines work by Andy Groove_

        fun format(plan: LogicalPlan, indent: Int = 0): String {
          val b = StringBuilder()
          0.rangeTo(indent).forEach { b.append("\t") }
          b.append(plan.toString()).append("\n")
          plan.children().forEach { b.append(format(it, indent+1)) }
          return b.toString()
        }
        """
        output = ""
        output += "\t" * indent
        output += repr(self)
        output += "\n"
        for children in self.children():
            tree = children.get_tree(indent + 1)
            output += tree
        return output

    def __repr__(self):
        return self.__class__.__name__


class LogicalExpression(abc.ABC):
    @abc.abstractmethod
    def to_field(self, input: 'LogicalPlan'):
        pass

class PhysicalExpression(abc.ABC):
    
    @abc.abstractmethod
    def evaluate(self, input: RecordBatch):
        pass