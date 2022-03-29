# Make queries

Danio making queries by model's instance method and class method.


## Base method

There are basic instance method:

### create

insert instance to database

```python
async def create(
    self: MODEL_TV,
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = (),
    validate: bool = True,
)
```

### update

update instance data to database

```python
async def update(
    self: MODEL_TV,
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = (),
    validate: bool = True,
) -> bool
```

### save

insert or update instance data to database

```python
async def save(
    self: MODEL_TV,
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = (),
    force_insert=False,
    validate: bool = True,
) -> MODEL_TV
```

### refetch

refetch instance data from database by primary key

```python
async def refetch(
    self: MODEL_TV,
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = tuple(),
) -> MODEL_TV:
```

### delete

``` python
async def delete(
    self,
    database: typing.Optional[Database] = None,
) -> bool
```

eg:

```python
await Cat(name="dangdang", age=5).create()
await Cat(id=1, name="dangdang", age=5).update()
await Cat(name="dangdang", age=5).save()  # call create
await Cat(id=1, name="dangdang", age=5).save()  # call update
await Cat(id=1).delete()
```

### `database` and `fields` params

All query method support `database` param, if not set danio will call `cls.get_database` to obtain one database instance.And we can pass a database with transaction, eg:
```python
db = Cat.get_database(danio.Operation.UPDATE, Cat.table_name)
async with db.transaction():
    cat = await Cat.where(Cat.id == 1, database=db).for_update().fetch_one()
    if cat:
        cat.name += "_updated"
        await cat.save()
```

All query method with model layer(interact with model instance), support `fields` param, means only those fields will be select, insert or update, eg:
```python
cat = await Cat.where().fetch_one(fields=[Cat.name])
print(cat.id)  # will be 0(default id value)
await Cat(id=1, name="dangdang", age=5).update(fields=[Cat.name])  # only name field will be update in database
```

## Where Chain

Danio use `where chain` to express and execute basic SQL actually.


```python
@classmethod
def where(
    cls: typing.Type[MODEL_TV],
    *conditions: SQLExpression,
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = tuple(),
    raw="",
    is_and=True,
) -> Crud[MODEL_TV]
```

### Read(Model layer)

Get instance or raw data back from database:

* where().fetch_all - select all matched data and return a list of model instance
* where().fetch_one - select first matched data and return one model instance

eg:
```python
cats = await Cat.where().fetch_all()
await cats[-1].delete()
cat = await Cat.where().fetch_one()
await cat.delete()
```

### Other operation

Except `fetch_all`, `fetch_one`, there are raw sql operation method(without model layer) for where chain:

* fetch_count - select count

    `async def fetch_count(self) -> int`

* fetch_row - fetch row data from database

    `async def fetch_row(self, fields: typing.Sequence[Field] = tuple()) -> typing.List[typing.Mapping]`

* update - update data by condition without model layer

    `async def update(self, **data) -> int`

* delete - without model layer too

    `async def delete(self) -> int`

* for_update - select with `UPDATE` lock

    `def for_update(self: CRUD_TV) -> CRUD_TV`

* for_select - select with `SHARE` lock

    `def for_select(self: CRUD_TV) -> CRUD_TV`

* use_index - select with index hints

    `def use_index(self: CRUD_TV, indexes: typing.Sequence[str], _for: str = "") -> CRUD_TV`

* force_index - select with index hints

    `def force_index(self: CRUD_TV, indexes: typing.Sequence[str], _for: str = "") -> CRUD_TV`

* ignore_index - select with index hints

    `def ignore_index(self: CRUD_TV, indexes: typing.Sequence[str], _for: str = "") -> CRUD_TV`

* limit

    `def limit(self, n: int) -> Crud`

* offset

    `def offset(self, n: int) -> Crud`

* order_by

    `def order_by(self, f: typing.Union[Field, SQLExpression], asc=True) -> Crud`


eg:
```python
await Cat.where().delete()
count = await Cat.where().fetch_count()
print(count)  # will be 0
await Cat.where().update(name="all_updated")
```

eg:
```python
await Cat.where().offset(20).limit(10).fetch_all()
await Cat.where().order_by(Cat.name, asc=False).fetch_all()
await Cat.where().for_update().fetch_all()
await Cat.where().for_share().fetch_all()
```

### Where Condition

Danio overwrite field class's some magic method like `__eq__`, `__gt__` and other else for simplify express sql condition.

```python
def __eq__(self, other: typing.Any) -> SQLExpression:  # type: ignore[override]
    ...
...
```

eg:
```python
await Cat.where(Cat.id == 1).delete()
await Cat.where(Cat.id == 1).fetch_one()
await Cat.where(Cat.name == "old").update(name="new")
```

Danio support simple expressions for now:

* ==
* ">"
* ">="
* <
* <=
* !=

Danio also support sql `like` and `contain` condition, eg:
```python
await Cat.where(Cat.id.contains([1, 2, 3])).fetch_all()
await Cat.where(Cat.name.like("%name%")).fetch_all()  # for char/text field only
```

Danio also support complicated expression:
```python
await Cat.where((Cat.id + 1) > u.id).fetch_one()  ## `id` + 1 > 1
await Cat.where((Cat.id < 10) | (Cat.id > 20)).fetch_one()  ## id < 10 or id > 20
```


And we can combine multiple condition by `where chain`, eg:
```python
await Cat.where(Cat.id > 10, Cat.id < 20).fetch_all()  # 10 < id < 20
await Cat.where(Cat.id > 10).where(Cat.id < 20).fetch_all()  # 10 < id < 20
await Cat.where(Cat.id < 10).where(Cat.id > 20, is_and=False).fetch_all()  # id < 10 or id > 20
```