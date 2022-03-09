# Tips

There are some tips around danio.

## JSON

Thanks [orjson](https://github.com/ijl/orjson),  we can covert danio model instance to json str with one line code:
```python
orjson.dumps(Cat(name="cc"))
```
