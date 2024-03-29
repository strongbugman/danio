from __future__ import annotations

import asyncio
import copy
import dataclasses
import decimal
import enum
import itertools
import json
import random
import typing
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce

import sqlalchemy

from . import exception, utils
from .database import Database

T = typing.TypeVar("T")
SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")
MIGRATION_TV = typing.TypeVar("MIGRATION_TV", bound="Migration")
MARKER_TV = typing.TypeVar("MARKER_TV", bound="SQLMarker")
CRUD_TV = typing.TypeVar("CRUD_TV", bound="Crud")
CASE_TV = typing.TypeVar("CASE_TV", bound="SQLCase")


def join(*contents, delimiter=" ") -> str:
    return delimiter.join(tuple(filter(lambda c: c, contents)))


def V(value: typing.Any) -> str:
    return (
        sqlalchemy.text(":df")
        .bindparams(df=value)
        .compile(compile_kwargs={"literal_binds": True})
    )


@dataclasses.dataclass
class Field:
    class FieldDefault:
        pass

    class NoDefault:
        pass

    TYPE: typing.ClassVar[str] = ""

    name: str = ""
    model_name: str = ""
    default: typing.Any = NoDefault
    _default: typing.Any = NoDefault
    type: str = ""
    primary: bool = False
    auto_increment: bool = False
    not_null: bool = True
    comment: str = ""
    enum: typing.Optional[typing.Type[enum.Enum]] = None

    @property
    def default_value(self) -> typing.Any:
        if self._default is self.NoDefault:
            if callable(self.default):
                self._default = self.default()
            else:
                self._default = copy.copy(self.default)

        return self._default

    def __post_init__(self):
        if not self.type and self.TYPE:
            self.type = self.TYPE

        if self.enum and not isinstance(self.default, self.enum):
            self.default = list(self.enum)[0]

    def __eq__(self, other: typing.Any) -> SQLExpression:  # type: ignore[override]
        return SQLExpression(field=self, values=[(SQLExpression.Operator.EQ, other)])

    def __gt__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.GT, other)])

    def __lt__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.LT, other)])

    def __ge__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.GE, other)])

    def __le__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.LE, other)])

    def __ne__(self, other: object) -> SQLExpression:  # type: ignore[override]
        return SQLExpression(field=self, values=[(SQLExpression.Operator.NE, other)])

    def __add__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.ADD, other)])

    def __sub__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.SUB, other)])

    def __mul__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.MUL, other)])

    def __truediv__(self, other: object) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.DIV, other)])

    def contains(self, values: object) -> SQLExpression:
        if not values:
            raise ValueError("Empty values")

        return SQLExpression(field=self, values=[(SQLExpression.Operator.IN, values)])

    def like(self, value: typing.Any) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.LK, value)])

    def case(
        self,
        expression: SQLExpression,
        value: typing.Any,
        default: typing.Any = None,
    ) -> SQLCase:
        return SQLCase(
            field=self, default=default or self.default, cast_type=self.type
        ).case(expression, value)

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        not_null = "NOT NULL" if self.not_null else ""
        if type == type.MYSQL:
            return f"{type.quote(self.name)} {self.type} {not_null} {'AUTO_INCREMENT' if self.auto_increment else ''} COMMENT '{self.comment}'"
        elif type == type.POSTGRES:
            # only support "serail" type for auto increment field
            return f"{type.quote(self.name)} {self.type} {'PRIMARY KEY' if self.primary else ''} {not_null}"
        else:
            return f"{type.quote(self.name)} {self.type} {'PRIMARY KEY' if self.primary else ''} {'AUTOINCREMENT' if self.auto_increment else ''} {not_null if not self.primary else ''}"

    def to_python(self, value: typing.Any) -> typing.Any:
        """From databases raw to python"""
        if self.enum:
            return self.enum(value)
        return value

    def to_database(self, value: typing.Any) -> typing.Any:
        """From python to databases raw"""
        if self.enum and isinstance(value, self.enum):
            return value.value
        return value


@dataclasses.dataclass(eq=False)
class IntField(Field):
    TYPE: typing.ClassVar[str] = "int"

    default: int = 0


@dataclasses.dataclass(eq=False)
class SmallIntField(IntField):
    TYPE: typing.ClassVar[str] = "smallint"


@dataclasses.dataclass(eq=False)
class TinyIntField(IntField):
    TYPE: typing.ClassVar[str] = "tinyint"


@dataclasses.dataclass(eq=False)
class BoolField(Field):
    TYPE: typing.ClassVar[str] = "tinyint"

    default: bool = False

    def to_database(self, value: typing.Any) -> typing.Any:
        if isinstance(value, bool):
            value = int(value)
        return super().to_database(value)

    def to_python(self, value: typing.Any) -> typing.Any:
        if isinstance(value, int):
            value = bool(value)
        return super().to_python(value)


@dataclasses.dataclass(eq=False)
class BigIntField(IntField):
    TYPE: typing.ClassVar[str] = "bigint"


@dataclasses.dataclass(eq=False)
class FloatField(Field):
    TYPE: typing.ClassVar[str] = "float"

    default: float = 0


@dataclasses.dataclass(eq=False)
class DecimalField(Field):
    TYPE: typing.ClassVar[str] = "decimal(4,2)"

    default: decimal.Decimal = decimal.Decimal()


@dataclasses.dataclass(eq=False)
class CharField(Field):
    TYPE: typing.ClassVar[str] = "varchar(255)"

    default: str = ""


@dataclasses.dataclass(eq=False)
class TextField(CharField):
    TYPE: typing.ClassVar[str] = "text"

    default: str = ""


@dataclasses.dataclass(eq=False)
class BytesField(Field):
    TYPE: typing.ClassVar[str] = "binary(24)"

    default: bytes = b""


@dataclasses.dataclass(eq=False)
class BlobField(Field):
    TYPE: typing.ClassVar[str] = "blob"

    default: bytes = b""


@dataclasses.dataclass(eq=False)
class TimeField(Field):
    """Using timedelta other than time which only support from '00:00:00' to '23:59:59'"""

    TYPE: typing.ClassVar[str] = "time"

    default: timedelta = timedelta(0)


@dataclasses.dataclass(eq=False)
class DateField(Field):
    TYPE: typing.ClassVar[str] = "date"

    default: typing.Callable = lambda: datetime.now().date()  # noqa


@dataclasses.dataclass(eq=False)
class DateTimeField(Field):
    TYPE: typing.ClassVar[str] = "datetime"

    default: typing.Callable = datetime.now


@dataclasses.dataclass(eq=False)
class JsonField(CharField):
    TYPE: typing.ClassVar[str] = "varchar(2048)"

    default: typing.Any = dataclasses.field(default_factory=dict)

    def to_python(self, value: typing.Any) -> typing.Any:
        if value and isinstance(value, str):
            return json.loads(value)
        else:
            return super().to_python(value)

    def to_database(self, value: typing.Any) -> str:
        return json.dumps(value)


def field(
    field_cls=Field,
    type="",
    name="",
    comment="",
    default=Field.FieldDefault,
    primary=False,
    auto_increment=False,
    not_null=True,
    enum: typing.Optional[typing.Type[enum.Enum]] = None,
) -> typing.Any:
    extras = {}
    if default is not Field.FieldDefault:  # default to field default, allow None
        extras["default"] = default

    return field_cls(
        name=name,
        type=type,
        comment=comment,
        primary=primary,
        auto_increment=auto_increment,
        not_null=not_null,
        enum=enum,
        **extras,
    )


@dataclasses.dataclass
class Index:
    fields: typing.List[Field]
    unique: bool
    name: str = ""

    def __post_init__(self):
        if not self.name:
            self.name = f"{'_'.join(f.name for f in self.fields)[:15]}_{random.randint(1, 10000)}{'_uiq' if self.unique else '_idx'}"

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        if type == type.MYSQL:
            return join(
                "UNIQUE " if self.unique else "",
                "KEY",
                type.quote(self.name),
                f"({', '.join(type.quote(f.name) for f in self.fields)})",
            )
        else:
            return ""


class RelationField(typing.Generic[T]):
    def __init__(
        self, fetcher: typing.Callable[[T], typing.Any], auto: bool = False
    ) -> None:
        self.fetcher = fetcher
        self.auto = auto

    def set_model_name(self, model_name: str):
        self.model_name = model_name

    async def load(self, instance: T) -> typing.Any:
        result = self.fetcher(instance)
        if asyncio.iscoroutine(result):
            return await result
        else:
            return result


@dataclasses.dataclass(eq=False)
class Schema:
    name: str
    indexes: typing.List[Index] = dataclasses.field(default_factory=list)
    fields: typing.List[Field] = dataclasses.field(default_factory=list)
    relation_fields: typing.List[RelationField] = dataclasses.field(
        default_factory=list
    )
    abstracted: bool = False

    @utils.cached_property
    def primary_field(self) -> Field:
        for f in self.fields:
            if f.primary:
                return f
        raise exception.SchemaException("Primary field not found!")

    def __sub__(self, other: object) -> Migration:
        if other is None:
            return Migration(schema=self, old_schema=None)
        assert isinstance(other, Schema)
        # fields
        self_fields = {f.name: f for f in self.fields}
        other_fields = {f.name: f for f in other.fields}
        change_type_fields = []
        for f in self.fields:
            if f.name in other_fields:
                type1 = f.type.lower()
                type2 = other_fields[f.name].type.lower()
                # postgresql serial field
                type1 = type1.replace("serial", "int")
                type2 = type2.replace("serial", "int")
                # integer
                type1 = type1.replace("integer", "int")
                type2 = type2.replace("integer", "int")
                # int int(10)...
                if "int" in type1:
                    type1 = type1.split("(")[0]
                if "int" in type2:
                    type2 = type2.split("(")[0]
                if type1 != type2:
                    change_type_fields.append(f)
        # indexes
        self_indexes = {
            (i.unique, tuple(f.name for f in i.fields)): i for i in self.indexes
        }
        other_indexes = {
            (i.unique, tuple(f.name for f in i.fields)): i for i in other.indexes
        }

        return Migration(
            schema=self,
            old_schema=other,
            add_indexes=[
                self_indexes[k] for k in (self_indexes.keys()) - (other_indexes.keys())
            ],
            drop_indexes=[
                other_indexes[k] for k in (other_indexes.keys()) - (self_indexes.keys())
            ],
            add_fields=[f for f in self.fields if f.name not in other_fields],
            drop_fields=[f for f in other.fields if f.name not in self_fields],
            change_type_fields=change_type_fields,
        )

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, Schema)
        return not bool((self - other).to_sql())

    def sync_index_name(self: SCHEMA_TV, other: SCHEMA_TV) -> SCHEMA_TV:
        if self != other:
            raise ValueError("Not migrated!")
        serialize = (
            lambda idx: f"{idx.unique}_{'_'.join(sorted(f.name for f in idx.fields))}"
        )
        indexes = {serialize(idx): idx for idx in other.indexes}
        for idx in self.indexes:
            idx.name = indexes[serialize(idx)].name
        return self

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        assert self.primary_field

        if type == type.MYSQL:
            keys = [f"PRIMARY KEY ({type.quote(self.primary_field.name)})"]
            postfix = (
                " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
            )
        elif type == type.POSTGRES:
            keys = []
            postfix = ";"
        else:
            keys = []
            postfix = ";"

        if type == type.MYSQL:
            keys.extend([index.to_sql(type=type) for index in self.indexes])

        sql = (
            f"CREATE TABLE {type.quote(self.name)} (\n"
            + ",\n".join(
                itertools.chain((v.to_sql(type=type) for v in self.fields), keys)
            )
            + f"\n){postfix}"
        )
        if type != Database.Type.MYSQL:
            _sqls = []
            for index in self.indexes:
                _sqls.append(
                    f"CREATE {'UNIQUE ' if index.unique else ' '}INDEX {type.quote(index.name)} on {type.quote(self.name)} ({', '.join(f'{type.quote(f.name)}' for f in index.fields)});"
                )
            sql += "\n".join(_sqls)
        if type == type.POSTGRES:
            _sqls = []
            for f in self.fields:
                if f.comment:
                    _sqls.append(
                        f'COMMENT ON COLUMN "{self.name}"."{f.name}" is \'{f.comment}\';'
                    )
            sql += "\n".join(_sqls)
        return sql

    @classmethod
    def detect_field_type(cls, _: Database, field_type: str) -> typing.Type[Field]:
        if utils.contains(field_type, ("json",)):
            return JsonField
        elif utils.contains(field_type, ("int", "serial")):
            return IntField
        elif utils.contains(field_type, ("boolean",)):
            return BoolField
        elif utils.contains(field_type, ("float", "real", "double")):
            return FloatField
        elif utils.contains(field_type, ("numeric", "decimal", "money")):  # TODO: money
            return DecimalField
        elif utils.contains(field_type, ("binary", "blob", "bytea", "bit")):
            return BytesField
        elif utils.contains(field_type, ("date",)):
            return DateField
        elif utils.contains(field_type, ("time",)):
            return TimeField
        elif utils.contains(field_type, ("datetime", "timestamp")):
            return DateTimeField
        elif utils.contains(field_type, ("char", "text", "character", "clob")):
            return CharField
        else:
            return Field


@dataclasses.dataclass
class Migration:
    schema: typing.Optional[Schema]
    old_schema: typing.Optional[Schema]
    drop_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    add_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    change_type_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    add_indexes: typing.List[Index] = dataclasses.field(default_factory=list)
    drop_indexes: typing.List[Index] = dataclasses.field(default_factory=list)

    def __invert__(self: MIGRATION_TV) -> MIGRATION_TV:
        change_type_fields: typing.List[Field] = []
        if self.old_schema:
            changed_fields = {f.name for f in self.change_type_fields}
            old_fields = {f.name: f for f in self.old_schema.fields}
            if self.schema:
                change_type_fields.extend(
                    old_fields[f.name]
                    for f in self.schema.fields
                    if f.name in changed_fields
                )

        return self.__class__(
            schema=self.old_schema,
            old_schema=self.schema,
            add_fields=self.drop_fields,
            drop_fields=self.add_fields,
            change_type_fields=change_type_fields,
            add_indexes=self.drop_indexes,
            drop_indexes=self.add_indexes,
        )

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        sqls = []
        if self.schema and not self.old_schema:
            return self.schema.to_sql(type=type)
        elif self.old_schema and not self.schema:
            sqls.append(f"DROP TABLE {type.quote(self.old_schema.name)}")
        elif self.schema and self.old_schema:
            if self.old_schema.name != self.schema.name:
                sqls.append(
                    f"ALTER TABLE {type.quote(self.old_schema.name)} RENAME {type.quote(self.schema.name)}"
                )
            for i in self.drop_indexes:
                if type == type.MYSQL:
                    if not set(f.name for f in i.fields) & set(
                        f.name for f in self.drop_fields
                    ):
                        sqls.append(
                            f"ALTER TABLE {type.quote(self.schema.name)} DROP INDEX {type.quote(i.name)}"
                        )
                else:
                    sqls.append(f"DROP INDEX {type.quote(i.name)}")
            for f in self.add_fields:
                sqls.append(
                    f"ALTER TABLE {type.quote(self.schema.name)} ADD COLUMN {f.to_sql(type=type)}"
                )
                if not isinstance(f.default_value, f.NoDefault):
                    sqls[-1] += f" DEFAULT {V(f.to_database(f.default_value))}"
                    if type != Database.Type.SQLITE:
                        sqls.append(
                            f"ALTER TABLE {type.quote(self.schema.name)} ALTER COLUMN {type.quote(f.name)} DROP DEFAULT"
                        )
            for f in self.drop_fields:
                sqls.append(
                    f"ALTER TABLE {type.quote(self.schema.name)} DROP COLUMN {type.quote(f.name)}"
                )
            for f in self.change_type_fields:
                if type == type.SQLITE:
                    raise exception.OperationException(
                        "Type changing not allowed in SQLite"
                    )
                elif type == type.MYSQL:
                    sqls.append(
                        f"ALTER TABLE {type.quote(self.schema.name)} MODIFY {type.quote(f.name)} {f.type}"
                    )
                else:
                    sqls.append(
                        f"ALTER TABLE {type.quote(self.schema.name)} ALTER COLUMN {type.quote(f.name)} TYPE {f.type}"
                    )
            for i in self.add_indexes:
                sqls.append(
                    f"CREATE {'UNIQUE ' if i.unique else ''}INDEX {type.quote(i.name)} on {type.quote(self.schema.name)} ({','.join(type.quote(f.name) for f in i.fields)})"
                )
        if sqls:
            sqls[-1] += ";"

        return ";\n".join(sqls)


@enum.unique
class Operation(enum.IntEnum):
    CREATE = 1
    READ = 2
    UPDATE = 3
    DELETE = 4


# query builder
@dataclasses.dataclass
class SQLMarker:
    class ID:
        def __init__(self, value: int = 0) -> None:
            self.value: int = value

        def get_add(self) -> int:
            v = self.value
            self.value += 1
            return v

    field: typing.Optional[Field] = None
    _var_index: ID = dataclasses.field(default_factory=ID)
    _vars: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    def mark(self, value: typing.Any) -> str:
        k = f"var{self._var_index.get_add()}"
        self._vars[k] = value
        return k

    def sync(self: MARKER_TV, other: SQLMarker) -> MARKER_TV:
        self._vars = other._vars
        self._var_index = other._var_index
        return self

    def _parse(
        self, value: typing.Any, type: Database.Type = Database.Type.MYSQL
    ) -> str:
        if isinstance(value, Field):
            return type.quote(value.name)
        elif isinstance(value, SQLMarker):
            return value.sync(self).to_sql(type=type)
        elif self.field:
            return f":{self.mark(self.field.to_database(value))}"
        else:
            return f":{self.mark(value)}"

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        """For _parse method"""
        return ""


@dataclasses.dataclass
class SQLExpression(SQLMarker):
    class Operator(enum.Enum):
        ADD = "+"
        SUB = "-"
        MUL = "*"
        DIV = "/"
        EQ = "="
        NE = "!="
        GT = ">"
        GE = ">="
        LT = "<"
        LE = "<="
        LK = "LIKE"
        IN = "IN"
        AND = "AND"
        OR = "OR"

    values: typing.List[typing.Tuple[Operator, typing.Any]] = dataclasses.field(
        default_factory=list
    )

    def __eq__(self, other: typing.Any) -> SQLExpression:  # type: ignore[override]
        self.values.append((self.Operator.EQ, other))
        return self

    def __gt__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.GT, other))
        return self

    def __lt__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.LT, other))
        return self

    def __ge__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.GE, other))
        return self

    def __le__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.LE, other))
        return self

    def __ne__(self, other: object) -> SQLExpression:  # type: ignore[override]
        self.values.append((self.Operator.NE, other))
        return self

    def __add__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.ADD, other))
        return self

    def __sub__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.SUB, other))
        return self

    def __mul__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.MUL, other))
        return self

    def __truediv__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.DIV, other))
        return self

    def __and__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.AND, other))
        return self

    def __or__(self, other: object) -> SQLExpression:
        self.values.append((self.Operator.OR, other))
        return self

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        assert self.field

        sql = f"{type.quote(self.field.name)}"
        for op, value in self.values:
            if op == self.Operator.IN:
                if isinstance(value, SQLMarker):
                    sql += f" {op.value} ({self._parse(value, type=type)})"
                else:
                    sql += f" {op.value} ({', '.join(self._parse(v, type=type) for v in value)})"
            elif op == self.Operator.LK:
                sql += f" {op.value} :{self.mark(value)}"
            elif isinstance(value, SQLExpression):
                sql = f"({sql}) {op.value} ({self._parse(value, type=type)})"
            else:
                sql += f" {op.value} {self._parse(value, type=type)}"
        return sql


@dataclasses.dataclass
class SQLCase(SQLMarker):
    cases: typing.List[typing.Tuple[SQLExpression, typing.Any]] = dataclasses.field(
        default_factory=list
    )
    default: typing.Any = None
    cast_type: str = "text"

    def case(self: CASE_TV, expression: SQLExpression, value: typing.Any) -> CASE_TV:
        self.cases.append((expression, value))
        return self

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        assert self.cases
        if type == type.POSTGRES:
            cast_type = "\:\:" + self.cast_type  # noqa
        else:
            cast_type = ""

        sql = "CASE"
        for ex, v in self.cases:
            sql += f" WHEN {self._parse(ex, type=type)} THEN {self._parse(v, type=type)}{cast_type}"
        sql += f" ELSE {self._parse(self.default, type=type)}{cast_type} END"

        return sql


@dataclasses.dataclass
class BaseSQLBuilder(SQLMarker):
    pass


@dataclasses.dataclass
class Crud(BaseSQLBuilder):
    schema: typing.Optional[Schema] = None
    _where: typing.Optional[SQLExpression] = None
    _raw_where: str = ""
    _limit: int = 0
    _offset: int = 0
    _order_by: typing.List[typing.Union[Field, SQLExpression]] = dataclasses.field(
        default_factory=list
    )
    _order_by_asc: typing.List[bool] = dataclasses.field(default_factory=list)
    _for_update: bool = False
    _for_share: bool = False
    _use_indexes: typing.List[
        typing.Tuple[typing.Sequence[str], str]
    ] = dataclasses.field(default_factory=list)
    _ignore_indexes: typing.List[
        typing.Tuple[typing.Sequence[str], str]
    ] = dataclasses.field(default_factory=list)
    _force_indexes: typing.List[
        typing.Tuple[typing.Sequence[str], str]
    ] = dataclasses.field(default_factory=list)
    _selected_fields: typing.List[Field] = dataclasses.field(default_factory=list)

    def where(
        self: CRUD_TV,
        *conditions: SQLExpression,
        raw="",
        is_and=True,
    ) -> CRUD_TV:
        if conditions:
            _where = reduce(lambda c1, c2: c1 & c2 if is_and else c1 | c2, conditions)
            if self._where:
                self._where = self._where & _where
            else:
                self._where = _where
        elif raw:
            self._raw_where = raw

        return self

    def select(self: CRUD_TV, *fields: Field) -> CRUD_TV:
        self._selected_fields.extend(fields)
        return self

    def limit(self: CRUD_TV, n: int) -> CRUD_TV:
        self._limit = n
        return self

    def offset(self: CRUD_TV, n: int) -> CRUD_TV:
        self._offset = n
        return self

    def order_by(
        self: CRUD_TV, *f: typing.Union[Field, SQLExpression], asc=True
    ) -> CRUD_TV:
        self._order_by.extend(f)
        self._order_by_asc.extend([asc] * len(f))
        return self

    def for_update(self: CRUD_TV) -> CRUD_TV:
        self._for_update = True
        return self

    def for_share(self: CRUD_TV) -> CRUD_TV:
        self._for_share = True
        return self

    def use_index(
        self: CRUD_TV, indexes: typing.Sequence[str], _for: str = ""
    ) -> CRUD_TV:
        self._use_indexes.append((indexes, _for))
        return self

    def ignore_index(
        self: CRUD_TV, indexes: typing.Sequence[str], _for: str = ""
    ) -> CRUD_TV:
        self._ignore_indexes.append((indexes, _for))
        return self

    def force_index(
        self: CRUD_TV, indexes: typing.Sequence[str], _for: str = ""
    ) -> CRUD_TV:
        self._force_indexes.append((indexes, _for))
        return self

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        return self.to_select_sql(type=type)[:-1]

    def to_select_sql(
        self,
        count=False,
        type: Database.Type = Database.Type.MYSQL,
        fields: typing.Iterable[Field] = tuple(),
        ignore_fields: typing.Iterable[Field] = tuple(),
    ) -> str:
        assert self.schema

        self._selected_fields.extend(fields)
        if not count:
            _ignore_fields = {f.name for f in ignore_fields}
            sql = f"SELECT {', '.join(f'{type.quote(f.name)}' for f in self._selected_fields or self.schema.fields if f.name not in _ignore_fields)} FROM {type.quote(self.schema.name)}"
        else:
            sql = f"SELECT COUNT(*) FROM {type.quote(self.schema.name)}"
        if type == type.MYSQL:
            for indexes in self._use_indexes:
                sql += f" USE INDEX {type.quote(indexes[1]) if indexes[1] else ''} ({','.join(type.quote(s) for s in indexes[0])}) "
            for indexes in self._ignore_indexes:
                sql += f" IGNORE INDEX {type.quote(indexes[1]) if indexes[1] else ''} ({','.join(type.quote(s) for s in indexes[0])}) "
            for indexes in self._force_indexes:
                sql += f" FORCE INDEX {indexes[1] if indexes[1] else ''} ({','.join(type.quote(s) for s in indexes[0])}) "
        elif type == type.SQLITE:
            _use_indexes = self._use_indexes or self._force_indexes
            if _use_indexes:
                sql += f" INDEXED BY {type.quote(_use_indexes[0][0][0])} "
            if self._ignore_indexes:
                sql += " NOT INDEXED "
        else:
            if self._use_indexes or self._force_indexes or self._ignore_indexes:
                raise exception.OperationException(
                    "For PostgreSQL - index hints not supported"
                )
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql(type=type)}"
        elif self._raw_where:
            sql += f" WHERE {self._raw_where}"
        if not count:
            if self._order_by:
                _order_by_sql = ", ".join(
                    [
                        f"{type.quote(od.name) if isinstance(od, Field) else od.sync(self).to_sql(type=type)} {'ASC' if self._order_by_asc[i] else 'DESC'}"
                        for i, od in enumerate(self._order_by)
                    ]
                )

                sql += f" ORDER BY {_order_by_sql}"
            if self._limit:
                sql += f" LIMIT :{self.mark(self._limit)}"
            if self._offset:
                assert self._limit, "Offset need limit"
                sql += f" OFFSET :{self.mark(self._offset)}"
            if self._for_update:
                if type == type.SQLITE:
                    raise exception.OperationException(
                        "For SQLite - do not support FOR UPDATE lock"
                    )
                else:
                    sql += " FOR UPDATE"
            elif self._for_share and type != Database.Type.SQLITE:
                if type == type.SQLITE:
                    raise exception.OperationException(
                        "For SQLite - do not support FOR UPDATE lock"
                    )
                elif type == type.MYSQL:
                    sql += " LOCK IN SHARE MODE"
                else:
                    sql += " FOR SHARE"

        return sql + ";"

    def to_delete_sql(self, type: Database.Type = Database.Type.MYSQL):
        assert self.schema

        sql = f"DELETE from {type.quote(self.schema.name)}"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql(type=type)}"
        return sql + ";"

    def to_update_sql(
        self,
        data: typing.Dict[str, typing.Any],
        type: Database.Type = Database.Type.MYSQL,
    ):
        assert self.schema

        fields = {f.model_name: f for f in self.schema.fields}
        _sqls = []
        for k, v in data.items():
            if isinstance(v, SQLMarker):
                _sqls.append(f"{type.quote(k)} = {v.sync(self).to_sql(type=type)}")
            else:
                _sqls.append(
                    f"{type.quote(k)} = :{self.mark(fields[k].to_database(v))}"
                )
        sql = f"UPDATE {type.quote(self.schema.name)} SET {', '.join(_sqls)}"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql(type=type)}"
        return sql + ";"


@dataclasses.dataclass
class Insert(BaseSQLBuilder):
    schema: typing.Optional[Schema] = None
    insert_data: typing.Sequence[typing.Dict[str, str]] = dataclasses.field(
        default_factory=list
    )
    update_fields: typing.Iterable[str] = dataclasses.field(default_factory=list)
    conflict_targets: typing.Iterable[str] = dataclasses.field(default_factory=list)

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        assert self.insert_data
        assert self.schema

        fields = {f.name: f for f in self.schema.fields}

        keys = list(self.insert_data[0].keys())
        vars = []
        for d in self.insert_data:
            _vars = []
            for k in keys:
                _vars.append(self.mark(fields[k].to_database(d[k])))
            vars.append(_vars)
        sql = f"INSERT INTO {type.quote(self.schema.name)} ({', '.join(map(lambda x: f'{type.quote(x)}', keys))}) VALUES"
        insert_value_sql = []
        for vs in vars:
            insert_value_sql.append(f"({', '.join(':' + v for v in vs)})")
        sql = sql + ", ".join(insert_value_sql)
        # upsert
        if self.update_fields:
            update_value_sql = []
            _sql = ""
            if type == type.MYSQL:
                if self.conflict_targets:
                    raise exception.OperationException(
                        "For MySQL - conflict_target not support"
                    )
                _sql = " ON DUPLICATE KEY UPDATE "
                for k in self.update_fields:
                    update_value_sql.append(f"{k} = VALUES({k})")
            elif type == type.SQLITE:
                conflict_target = (
                    f"({','.join(self.conflict_targets)})"
                    if self.conflict_targets
                    else ""
                )
                _sql = f" ON CONFLICT{conflict_target} DO UPDATE SET "
                for k in self.update_fields:
                    update_value_sql.append(f"{k} = excluded.{k}")
            else:
                if not self.conflict_targets:
                    raise exception.OperationException(
                        "For PostgresSQL - conflict_target must be provided"
                    )
                _sql = (
                    f" ON CONFLICT ({','.join(self.conflict_targets)}) DO UPDATE SET "
                )
                for k in self.update_fields:
                    update_value_sql.append(f"{k} = EXCLUDED.{k}")
            sql += _sql + ", ".join(update_value_sql)
        if type == type.POSTGRES:
            sql += f" RETURNING {self.schema.primary_field.name}"
        return sql + ";"


@dataclasses.dataclass
class CaseUpdate(BaseSQLBuilder):
    schema: typing.Optional[Schema] = None
    OPERATION: typing.ClassVar[Operation] = Operation.UPDATE
    data: typing.List[typing.Dict[str, str]] = dataclasses.field(default_factory=list)

    def to_sql(self, type: Database.Type = Database.Type.MYSQL):
        assert self.data
        assert self.schema

        fields = {f.name: f for f in self.schema.fields}

        parse_data: typing.DefaultDict[str, typing.Dict[str, typing.Any]] = defaultdict(
            dict
        )
        primary_values = []
        for d in self.data:
            for k, v in d.items():
                if k == self.schema.primary_field.name:
                    primary_values.append(v)
                else:
                    parse_data[k][d[self.schema.primary_field.name]] = fields[
                        k
                    ].to_database(v)
        sql = f"UPDATE {type.quote(self.schema.name)} SET"
        _sqls = []
        for k, vs in parse_data.items():
            case = SQLCase(cast_type=fields[k].type)
            for pv, v in vs.items():
                case.case(self.schema.primary_field == pv, v)
            _sqls.append(f" {type.quote(k)} = {case.sync(self).to_sql(type=type)}")
        sql += ", ".join(_sqls)
        sql += f" WHERE {(self.schema.primary_field.contains(primary_values)).sync(self).to_sql(type=type)};"

        return sql
