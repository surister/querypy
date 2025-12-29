import tempfile

from querypy.datasources.csv import CSVDataSource

import csv


def test_csv():
    header = ["str", "int", "bool"]
    rows = (
        ['a', 1, True],
        ['b', 2, False],
        ['c', 3, None],
        ['d', 4, True]
    )

    with tempfile.NamedTemporaryFile(mode="w+", newline="") as temp:
        writer = csv.writer(temp)
        writer.writerow(header)
        writer.writerows(rows)
        temp.seek(0)

        source = CSVDataSource(temp.name)
        schema = source.get_schema()

        assert len(header) == len(schema.fields)
        assert header == list(map(lambda f: f.name, schema.fields))

        rbs = source.scan([])
        assert sum(map(lambda rb: rb.row_count, rbs)) == 4
        assert sum(map(lambda rb: rb.column_count, rbs)) == 3
        assert rbs[0].get_field(0).value == ['a', 'b', 'c', 'd']
        assert rbs[0].get_field(1).value == [1, 2, 3, 4]
        assert rbs[0].get_field(2).value == ['True', 'False', None, 'True']