Pure python query engine based in Arrow.

It is a pure python implementation of the book [How query engines work by Andy Groove](https://www.howqueryengineswork.com).
This is for educational purposes only, under no reason one should use this for any real work.

Implementing the following query:

```sql
SELECT id, country, salary
FROM employee
WHERE country = 'ES'
  AND salary > 40000
GROUP BY id, country, salary;
```


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
