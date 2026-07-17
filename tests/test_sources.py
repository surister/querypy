import tempfile

from querypy.datasources.csv import CSVDataSource

import csv


def test_csv():
    header = ["str", "int", "bool", "float"]
    rows = (
        ['a', 1, True, 1.0],
        ['b', 2, False, 2.3],
        ['c', 3, None, 33.5],
        ['d', 4, True, 46.4]
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
        assert sum(map(lambda rb: rb.column_count, rbs)) == 4
        assert rbs[0].get_field(0).value == ['a', 'b', 'c', 'd']
        assert rbs[0].get_field(1).value == [1, 2, 3, 4]
        assert rbs[0].get_field(2).value == ['True', 'False', None, 'True']
        assert rbs[0].get_field(3).value == [1.0, 2.3, 33.5, 46.4]


def test_csv_col_index():
    """reproduces a bug where if different columns have same values a
    repeated row could be added"""
    header = ["str", "int", "bool", "int2"]
    rows = (
        ['a', 1, True, 1],
        ['b', 2, False, 6],
        ['c', 3, None, 5],
        ['d', 4, True, 7]
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
        assert sum(map(lambda rb: rb.column_count, rbs)) == 4
        assert rbs[0].get_field(0).value == ['a', 'b', 'c', 'd']
        assert rbs[0].get_field(1).value == [1, 2, 3, 4]
        assert rbs[0].get_field(2).value == ['True', 'False', None, 'True']
