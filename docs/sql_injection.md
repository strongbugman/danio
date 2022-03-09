# SQL Injection

Danio use `databases`'s way(actually `sqlarchemy`'s `bindparams`) to avoid SQL Injection.All passed param will be escaped.

## SQLMarker

SQLMarker is the base class to generate raw SQL.The `mark` method will create a placeholder for SQL value binding:
```python
class SQLMarker:
    class ID:
        def __init__(self, value: int = 0) -> None:
            ...
        def get_add(self) -> int:
            ...

    field: typing.Optional[Field] = None
    _var_index: ID = dataclasses.field(default_factory=ID)
    _vars: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    def mark(self, value: typing.Any) -> str:
        ...
```

eg:
```python
print(f"INSERT INTO HighScores(name, score) VALUES (:{self.mark(name)}, :{self.mark(score)}))  # print "INSERT INTO HighScores(name, score) VALUES (:var0, :var1)"
print(self._vars)  # {"var0": "name", "var1": 1}
```
Then pass SQL and all vars to database's `execute`
```python
await database.execute(sql, self._vars)
```
