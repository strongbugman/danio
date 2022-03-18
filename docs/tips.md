# Tips

There are some tips around danio.

## JSON

Thanks [orjson](https://github.com/ijl/orjson),  we can covert danio model instance to json str with one line code:
```python
orjson.dumps(Cat(name="cc"))
```

## Update?

For `update`, `create_or_update` and `upset` will return a `updated` variable:
```python
updated = await Cat(id=3, name="new name").update()
cat, created, updated = await Cat(id=3, name="new name").create_update((Cat.id, ))
created, updated = await Cat.upsert(
    [
        dict(id=3, name="new name"),
    ],
    update_fields=["name"],
)
```

For MySQL, `updated` will be `False` if row data is same as incoming update data, but SQLite will return True in this case.
