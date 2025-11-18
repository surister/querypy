from querypy.datasources import DataSource
from querypy.types_ import RecordBatch
from querypy.types_ import Schema


class CSVDataSource(DataSource):
    def __init__(self, path: str):
        self.path = path

    def get_schema(self) -> Schema:
        pass

    def scan(self, projection: list[str]) -> list[RecordBatch]:
        pass
