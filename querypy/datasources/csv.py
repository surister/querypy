"""_summary_."""

import csv
import datetime
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

    def __init__(self, path: str, override_schema: Schema = None):
        self.override_schema = override_schema
        self.path = path

    def parse_value(self, value, name: str):
        if self.override_schema:
            if field := self.override_schema.get_field_by_name(name):
                return self.override_type(value, field.type)

        if value.isdigit():
            return int(value)

        if "." in value:
            try:
                return float(value)
            except ValueError:
                pass
        return value

    def reset_schema_cache(self):
        """
        Resets the cached schema.
        """
        raise NotImplemented()

    def override_type(self, value, new_type):
        match new_type:
            case ArrowTypes.DateType:
                return datetime.date.fromisoformat(value)
            case _:
                raise ValueError(f'unsupported cast {value} to {new_type}')

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
            value = self.parse_value(value, name)
            fields.append(
                Field(name, ArrowTypes.from_pyvalue(value))
            )
        return Schema(fields)

    def scan(self,
             projection: list[str],
             override_types: None | list = None) -> list[RecordBatch]:
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

            for i, (name, value) in enumerate(zip(columns, first_row)):
                v = self.parse_value(value, name)

                fields.append(Field(name, ArrowTypes.from_pyvalue(v)))

                # Append the first row.
                values[i].append(v)

            schema = Schema(fields)

            # Exhaust reader while appending values appropriately.
            try:
                while reader:
                    for i, value in enumerate(next(reader)):
                        if i < len(columns):
                            v = self.parse_value(value, schema.fields[i].name)
                            v = None if v == "" else v
                            values[i].append(v)
            except StopIteration:
                pass

            return [RecordBatch.from_pylists(schema, values)]
