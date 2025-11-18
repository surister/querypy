Pure python query engine based in Arrow.

It is a pure python implementation of the book `How query engines work by Andy Groove`. This is
for educational purposes only, under no reason one should use this.


```python
Projection(
    Filter(
        Scan("employee", CSVDataSource("employee.csv"), []),
        Eq(ColumnarExpr("country"), LiteralExpr("ES"))
    ),
    [ColumnarExpr("id"),
     ColumnarExpr("country")]
)
print(plan.get_tree())
```
Output:

```
Projection(#id, #state)
	Filter: #state = 'CO'
		Scan: employee; projection=[]
```

While it is based in Apache Arrow, classes like `RecordBatch` are mocks, not `pyarrow.RecordBatch`.
