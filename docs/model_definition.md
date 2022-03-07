# Define Model

Danio use python's dataclasses as model layer.A danio model is basically a `dataclass` with special method and variable.

## Field

Danio use `danio.field` function to define a field in model

```python
def field(
    field_cls=Field,
    type="",
    name="",
    comment="",
    default=Field.FieldDefault,
    auto_increment=False,
    enum: typing.Optional[typing.Type[enum.Enum]] = None,
) -> typing.Any
```

eg:

```python
import danio

@dataclasses.dataclass
class Cat(danio.Model):
    name: str = danio.field(danio.CharField, comment="cat name")
    age: int = danio.field(danio.IntField)
```
There are the corresponding database table schema:
```sql
CREATE TABLE `cat` (
`id` int(10) NOT NULL AUTO_INCREMENT COMMENT '',
`name` varchar(255) NOT NULL  COMMENT 'cat name',
`age` int(10) NOT NULL  COMMENT '',
PRIMARY KEY (`id`),
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;;
```

We can define more detail by `danio.field` params:

* Define a different name in database:

    `name: str = danio.filed(danio.ChareField, name="name_in_database")`

* Define a custom type:

    `name: str = danio.filed(danio.ChareField, type="varchar(16)")`

* Define default value(Only affect model layer, **will not** set database field default'):

    `age: int = danio.field(danio.IntField, default=1)`

* Define auto increment:

    `id: int = danio.field(danio.IntField, auto_increment=True)`

* Define enum class(Only affect model layer, danio will load database value to defined enum):

    `gender: Gender = danio.field(danio.IntField, enum=Gender, default=Gender.FEMALE)`

Danio provide fields for now(It's easy to define custom field too):

* TinyField, SmallIntField, IntField, BigIntField
* BoolField(actually use tinyint in database by default), FloatField, DecimalField 
* CharField, TextField
* TimeField, DateField, DateTimeField
* JsonField(actually use varchar in database by default)

## Index

Danio store index information in model's classvar `_table_*_keys`, eg:

```python
@dataclasses.dataclass
class UserProfile(danio.Model):
    user_id: int = danio.field(danio.IntField)
    level: int = danio.field(danio.IntField)

    _table_primary_key: typing.ClassVar[typing.Any] = danio.Model.id
    _table_index_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Union[Field, str], ...], ...]
    ] = ((level, user_id,),)
    _table_unique_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Union[Field, str], ...], ...]
    ] = (("user_id",),)
```

There are the corresponding database table schema:

```sql
...
PRIMARY KEY (`id`),
UNIQUE KEY `user_id_471_uiq` (`user_id`),
KEY `level_user_id_231_idx` (`level`, `user_id`)
...
```

## Model Inherit

Danio use dataclasses's way to inherit, define a base model first:
```python
@dataclasses.dataclass
class Pet(danio.Model):
    name: str = danio.field(danio.CharField)
    age: int = danio.field(danio.IntField)

    _table_abstracted: typing.ClassVar[bool] = True
    _table_index_keys = ((age,),)
```
`_table_abstracted=True` means no pet table in database.
Then we inherit Pet:
```python
@dataclasses.dataclass
class Cat(Pet):
    weight: int = danio.field(danio.IntField)
```
So Cat has 4 field now: *id, name, age and weight*.We can disable or redefine a field:
```python
@dataclasses.dataclass
class Dog(Pet):
    name: str = ""
    age: int = danio.field(danio.SmallIntField)
```
Now Cat got 3 fields: *id, age, weight*.We can still use `name` variable as a normal dataclass's variable.And all Cat and Dog got same one index by field age.We can change this index too:
```python
@dataclasses.dataclass
class Fish(Pet):
    _table_index_keys = ((Pet.name,),)
```
Or add new one to the original index:
```python
@dataclasses.dataclass
class Fish(Pet):
    _table_index_keys = Pet._table_index_keys + ((Pet.name,),)
```

## Config database

### Table name

Danio obtain model schema's table name by `get_table_name` method, just join table prefix and model name by default:

```python
@classmethod
def get_table_name(cls) -> str:
    return cls._table_prefix + cls.__name__.lower()
```

### Database

For model's database instance, defined by `get_database` method:

```python
def get_database(
    cls, operation: Operation, table: str, *args, **kwargs
) -> Database
```

We can define this at a base model:

```python
db = danio.Database(
    "mysql://root:letmein@server:3306/test",
    maxsize=3,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
)

@dataclasses.dataclass
class BaseModel(danio.Model):
    @classmethod
    def get_database(
        cls, operation: danio.Operation, table: str, *args, **kwargs
    ) -> danio.Database:
        return db
```

Now any model inherit BaseModel will get `db` as database.

And we can set more than one database instance:

```python
# read_db = ...
# config_db = ...
# write_db = ...
@classmethod
def get_database(
    cls, operation: danio.Operation, table: str, *args, **kwargs
) -> danio.Database:
    if operation == danio.Operation.READ:
        return read_db
    elif table.startswith("config_"):
        return config_db
    else:
        return write_db
```