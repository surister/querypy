import abc

from querypy.types_ import RecordBatch
from querypy.types_ import Schema


class DataSource(abc.ABC):
    @abc.abstractmethod
    def get_schema(self) -> Schema:
        pass

    @abc.abstractmethod
    def scan(self, projection: list[str]) -> list[RecordBatch]:
        pass
