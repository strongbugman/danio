# Custom field

It's easy to define a custom danio field, main code:
```python
@dataclasses.dataclass
class Field:
    TYPE: typing.ClassVar[str] = ""

    name: str = ""
    model_name: str = ""
    default: typing.Any = NoDefault  # for model layer
    type: str = ""
    primary: bool = False
    auto_increment: bool = False
    comment: str = ""
    enum: typing.Optional[typing.Type[enum.Enum]] = None

    def to_python(self, value: typing.Any) -> typing.Any:
        """From databases raw data to python"""

    def to_database(self, value: typing.Any) -> typing.Any:
        """From python to databases raw"""
```

## Custom default Type

Danio will set field type to `TYPE` class var by default, eg:
```python
@dataclasses.dataclass
class MyIntField(danio.IntField):
    TYPE = "int(10)"
```

## Custom raw type conversion

Danio use `Field.to_python` and `Field.to_database` to convert field value type between model value and database value.


## Example

Let's see `danio.JsonField` definition:
```python
@dataclasses.dataclass(eq=False)
class JsonField(Field):
    TYPE: typing.ClassVar[str] = "varchar(2048)"

    default: typing.Any = dataclasses.field(default_factory=dict)

    def to_python(self, value: str) -> typing.Any:
        return json.loads(value)

    def to_database(self, value: typing.Any) -> str:
        return json.dumps(value)
```