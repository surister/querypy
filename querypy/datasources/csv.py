"""_summary_."""

import csv
import functools

from querypy.datasources import DataSource
from querypy.types_ import ArrowTypes
from querypy.types_ import Field
from querypy.types_ import RecordBatch
from querypy.types_ import Schema


class CSVDataSource(DataSource):
    """A datasource to read csv files.

    Attributes
    ----------
    path : str
        The path of the csv file.

    Methods
    -------
    get_schema()
        The schema of the csv, it's cached so accessing it many times will not
        trigger unnecessary I/O, to clear the cached schema run `reset_schema_cache`
    scan(projection: list[str])
        Reads the provided filepath, it only reads the provided columns, if not
        provided it'll read all.
    """

    def __init__(self, path: str):
        self.path = path

    def reset_schema_cache(self):
        """
        Resets the cached schema.
        """
        raise NotImplemented()

    @functools.lru_cache
    def get_schema(self) -> Schema:
        """Gets the schema of the file. Only the first row is used to [detect] the datatypes.

        Returns
        -------
        Schema
            The schema of the csv file.
        """
        with open(self.path) as f:
            reader = csv.reader(f)
            columns = next(reader)
            first_row = next(reader)
        fields = []
        for name, value in zip(columns, first_row):
            v = int(value) if value.isdigit() else value
            fields.append(Field(name, ArrowTypes.from_pyvalue(v)))
        return Schema(fields)

    def scan(self, projection: list[str]) -> list[RecordBatch]:
        """Scans the rows sequentially, creates a lists of values e.g. [[1,2,3], ['a','b','c']]
        and returns a `RecordBatch`.


        Parameters
        ----------
        projection : list[str]
            The columns to read.

        Returns
        -------
        list[RecordBatch]
            The read record batch. It does not implement chunking or reading bigger than
            memory so the list will only contain one `RecordBatch`
        """
        with open(self.path) as f:
            reader = csv.reader(f)
            columns = next(reader)
            if projection:
                columns = [col for col in columns if col in projection]
            first_row = next(reader)

            fields = []
            values = [[] for _ in range(len(columns))]

            for name, value in zip(columns, first_row):
                v = int(value) if value.isdigit() else value
                fields.append(Field(name, ArrowTypes.from_pyvalue(v)))

                # Append the first row.
                values[first_row.index(value)].append(v)

            schema = Schema(fields)

            # Exhaust reader while appending values appropriately.
            try:
                while reader:
                    for i, value in enumerate(next(reader)):
                        if i < len(columns):
                            v = int(value) if value.isdigit() else value
                            v = None if v == "" else v
                            values[i].append(v)
            except StopIteration:
                pass

            return [RecordBatch.from_pylists(schema, values)]
