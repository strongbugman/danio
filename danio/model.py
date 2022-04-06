"""
Base ORM model with CRUD
"""
from __future__ import annotations

import copy
import dataclasses
import decimal
import enum
import itertools
import json
import random
import re
import typing
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce

import cached_property
import sqlalchemy

from . import exception
from .database import Database
from .utils import class_property

MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")
SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")
MIGRATION_TV = typing.TypeVar("MIGRATION_TV", bound="Migration")
CRUD_TV = typing.TypeVar("CRUD_TV", bound="Crud")
MARKER_TV = typing.TypeVar("MARKER_TV", bound="SQLMarker")
CASE_TV = typing.TypeVar("CASE_TV", bound="SQLCase")


@dataclasses.dataclass
class Field:
    class FieldDefault:
        pass

    class NoDefault:
        pass

    TYPE: typing.ClassVar[str] = ""

    name: str = ""
    model_name: str = ""
    default: typing.Any = NoDefault  # for model layer
    type: str = ""
    primary: bool = False
    auto_increment: bool = False
    not_null: bool = True
    comment: str = ""
    enum: typing.Optional[typing.Type[enum.Enum]] = None

    @property
    def default_value(self) -> typing.Any:
        if callable(self.default):
            return self.default()
        else:
            return copy.copy(self.default)

    def __post_init__(self):
        if not self.type and self.TYPE:
            self.type = self.TYPE

        if self.enum and not isinstance(self.default, self.enum):
            self.default = list(self.enum)[0]

    def __hash__(self):
        if self.type.lower().startswith("int"):
            # for int(10) integer int
            type = "int"
        else:
            type = self.type.lower()
        if "serial" in type:
            # for postgresql serial field
            type = type.replace("serial", "int")
        return hash(
            (
                self.name,
                type.lower(),
            )
        )

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

    def contains(self, values: typing.Sequence) -> SQLExpression:
        if not values:
            raise ValueError("Empty values")

        return SQLExpression(field=self, values=[(SQLExpression.Operator.IN, values)])

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
        qt = Database.get_quote(type)
        not_null = " NOT NULL " if self.not_null else " "
        if type == Database.Type.MYSQL:
            return f"{qt}{self.name}{qt} {self.type}{not_null}{'AUTO_INCREMENT ' if self.auto_increment else ' '}COMMENT '{self.comment}'"
        elif type == Database.Type.POSTGRES:
            # only support "serail" type for auto increment field
            return f"{qt}{self.name}{qt} {self.type}  {'PRIMARY KEY ' if self.primary else ' '}{not_null}"
        else:
            return f"{qt}{self.name}{qt} {self.type} {'PRIMARY KEY ' if self.primary else ' '}{'AUTOINCREMENT ' if self.auto_increment else ' '}{not_null if not self.primary else ''}"

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
class FLoatField(Field):
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

    def like(self, value: typing.Any) -> SQLExpression:
        return SQLExpression(field=self, values=[(SQLExpression.Operator.LK, value)])


@dataclasses.dataclass(eq=False)
class TextField(CharField):
    TYPE: typing.ClassVar[str] = "text"

    default: str = ""


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

    def to_python(self, value: str) -> typing.Any:
        return json.loads(value)

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

    def __hash__(self):
        return hash((self.unique, tuple(f.name for f in self.fields)))

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, Index)
        return self.__hash__() == other.__hash__()

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        qt = Database.get_quote(type)
        if type == Database.Type.MYSQL:
            return (
                f"{'UNIQUE ' if self.unique else ''}KEY "
                f"{qt}{self.name}{qt} "
                f"({', '.join(f'{qt}{f.name}{qt}' for f in self.fields)})"
            )
        else:
            return ""


@dataclasses.dataclass
class Schema:
    name: str
    indexes: typing.Set[Index] = dataclasses.field(default_factory=set)
    fields: typing.Set[Field] = dataclasses.field(default_factory=set)
    abstracted: bool = False
    model: typing.Optional[typing.Type[Model]] = None

    @cached_property.cached_property
    def primary_field(self) -> Field:
        for f in self.fields:
            if f.primary:
                return f
        raise exception.SchemaException("Primary field not found!")

    def __hash__(self):
        return hash(
            (
                self.name,
                tuple(f for f in sorted(self.fields, key=lambda f: f.__hash__())),
                self.primary_field,
                tuple(i for i in sorted(self.indexes, key=lambda f: f.__hash__())),
            )
        )

    def __eq__(self, other: object):
        assert isinstance(other, Schema)
        return self.__hash__() == other.__hash__()

    def __sub__(self, other: object) -> Migration:
        if other is None:
            return Migration(schema=self, old_schema=None)
        assert isinstance(other, Schema)
        # fields
        self_fields = {f.name: f for f in self.fields}
        other_fields = {f.name: f for f in other.fields}
        _add_fields = {
            f.name: f
            for f in self.fields
            if f.name not in other_fields or hash(f) != hash(other_fields[f.name])
        }
        _drop_fields = {
            f.name: f
            for f in other.fields
            if f.name not in self_fields or hash(f) != hash(self_fields[f.name])
        }
        change_type_fields = []
        change_type_field_names = set(f.name for f in _add_fields.values()) & set(
            f.name for f in _drop_fields.values()
        )
        for f_name in change_type_field_names:
            field = _add_fields[f_name]
            _add_fields.pop(f_name)
            _drop_fields.pop(f_name)
            change_type_fields.append(field)

        return Migration(
            schema=self,
            old_schema=other,
            add_indexes=list(set(self.indexes) - set(other.indexes)),
            drop_indexes=list(set(other.indexes) - set(self.indexes)),
            add_fields=list(_add_fields.values()),
            drop_fields=list(_drop_fields.values()),
            change_type_fields=change_type_fields,
        )

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        assert self.primary_field
        qt = Database.get_quote(type)

        if type == Database.Type.MYSQL:
            keys = [f"PRIMARY KEY ({qt}{self.primary_field.name}{qt})"]
            postfix = (
                " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
            )
        elif type == Database.Type.POSTGRES:
            keys = []
            postfix = ";"
        else:
            keys = []
            postfix = ";"

        if type == Database.Type.MYSQL:
            keys.extend([index.to_sql(type=type) for index in self.indexes])

        sql = (
            f"CREATE TABLE {qt}{self.name}{qt} (\n"
            + ",\n".join(
                itertools.chain((v.to_sql(type=type) for v in self.fields), keys)
            )
            + f"\n){postfix}"
        )
        if type != Database.Type.MYSQL:
            _sqls = []
            for index in self.indexes:
                _sqls.append(
                    f"CREATE {'UNIQUE ' if index.unique else ' '}INDEX {qt}{index.name}{qt} on {qt}{self.name}{qt} ({', '.join(f'{qt}{f.name}{qt}' for f in index.fields)});"
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
    def from_model(cls: typing.Type[SCHEMA_TV], m: typing.Type[Model]) -> SCHEMA_TV:
        schema = cls(name=m.table_name, model=m)
        schema.abstracted = m.__dict__.get("_table_abstracted", False)
        # fields
        for f in dataclasses.fields(m):
            if isinstance(f.default, Field):
                f.default.model_name = f.name
                if not f.default.name:
                    f.default.name = f.name
                    f.default.__post_init__()
                schema.fields.add(f.default)
        fields = {f.model_name: f for f in schema.fields}
        # index
        for i, index_keys in enumerate((m._table_index_keys, m._table_unique_keys)):
            for keys in index_keys:
                if keys:
                    _fields = []
                    for key in keys:
                        if isinstance(key, Field):
                            _fields.append(key)
                        elif isinstance(key, str) and key in fields:
                            _fields.append(fields[key])
                        else:
                            raise exception.SchemaException(
                                f"Index: {keys} not supported"
                            )
                    schema.indexes.add(Index(fields=_fields, unique=i == 1))
        return schema

    @classmethod
    async def from_db(
        cls: typing.Type[SCHEMA_TV], database: Database, m: typing.Type[Model]
    ) -> typing.Optional[SCHEMA_TV]:
        schema = cls(name=m.table_name, model=m)
        model_names = {f.name: f.model_name for f in m.schema.fields}
        if database.type == Database.Type.MYSQL:
            field_name_pattern = re.compile(r"`([^ ,]*)`")
            try:
                for line in (
                    await database.fetch_all(f"SHOW CREATE TABLE {m.table_name}")
                )[0][1].split("\n")[1:-1]:
                    if "PRIMARY KEY" in line:
                        db_name = field_name_pattern.findall(line)[0]
                        for f in schema.fields:
                            if db_name == f.name:
                                f.primary = True
                                break
                    elif "KEY" in line:
                        fields = {f.name: f for f in schema.fields}
                        index_fields = []
                        _names = field_name_pattern.findall(line)
                        index_name = _names[0]
                        index_fields = [fields[n] for n in _names[1:]]
                        schema.indexes.add(
                            Index(
                                fields=index_fields,
                                unique="UNIQUE" in line,
                                name=index_name,
                            )
                        )
                    else:
                        db_name = field_name_pattern.findall(line)[0]
                        name = model_names.get(db_name, "")
                        schema.fields.add(
                            Field(
                                name=db_name,
                                type=line.split("`")[-1].split(" ")[1],
                                model_name=name,
                                auto_increment="AUTO_INCREMENT" in line,
                                not_null="NOT NULL" in line,
                            )
                        )
            except Exception as e:
                if "doesn't exist" in str(e):
                    return None
                raise e
        elif database.type == Database.Type.SQLITE:
            field_name_pattern = re.compile(r"`([^ ,]*)`")
            for d in await database.fetch_all(
                f"SELECT * FROM sqlite_schema WHERE tbl_name = '{m.table_name}';"
            ):
                if d[0] == "table":
                    for line in d[4].split("\n")[1:]:
                        names = field_name_pattern.findall(line)
                        if names:
                            db_name = names[0]
                            name = model_names.get(db_name, "")
                            primary = False
                            if "PRIMARY" in line:
                                primary = True
                            auto_increment = False
                            if "AUTOINCREMENT" in line:
                                auto_increment = True
                            schema.fields.add(
                                Field(
                                    name=db_name,
                                    type=line.split("`")[-1].split(" ")[1],
                                    model_name=name,
                                    primary=primary,
                                    auto_increment=auto_increment,
                                )
                            )
                elif d[0] == "index":
                    fields = {f.name: f for f in schema.fields}
                    _names = field_name_pattern.findall(d[4])
                    index_fields = [fields[n] for n in _names[2:]]
                    schema.indexes.add(
                        Index(
                            fields=index_fields,
                            unique="UNIQUE" in d[4],
                            name=d[1],
                        )
                    )
        else:
            for d in await database.fetch_all(
                f"SELECT * FROM information_schema.columns WHERE table_name = '{m.table_name}';"
            ):
                field_type = d["data_type"]
                if field_type == "character varying":
                    field_type = f"varchar({d['character_maximum_length']})"
                elif field_type == "character":
                    field_type = f"char({d['character_maximum_length']})"
                else:
                    field_type = field_type
                schema.fields.add(
                    Field(
                        name=d["column_name"],
                        model_name=model_names.get(d["column_name"], ""),
                        type=field_type,
                    )
                )
            for d in await database.fetch_all(
                f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{m.table_name}';"
            ):
                fields = {f.name: f for f in schema.fields}
                _names = d["indexdef"].split("(")[-1].split(")")[0].split(", ")
                index_fields = [fields[n] for n in _names]
                if d["indexname"].endswith("_pkey"):
                    primary_field = index_fields[0]
                    primary_field.primary = True
                else:
                    schema.indexes.add(
                        Index(
                            fields=index_fields,
                            unique="UNIQUE" in d["indexdef"],
                            name=d["indexname"],
                        )
                    )

        return schema if schema.fields else None


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
        qt = Database.get_quote(type)

        sqls = []
        if self.schema and not self.old_schema:
            return self.schema.to_sql(type=type)
        elif self.old_schema and not self.schema:
            sqls.append(f"DROP TABLE {qt}{self.old_schema.name}{qt}")
        elif self.schema and self.old_schema:
            if self.old_schema.name != self.schema.name:
                sqls.append(
                    f"ALTER TABLE {qt}{self.old_schema.name}{qt} RENAME {qt}{self.schema.name}{qt}"
                )
            for i in self.drop_indexes:
                if type == Database.Type.MYSQL:
                    if not set(i.fields) & set(self.drop_fields):
                        sqls.append(
                            f"ALTER TABLE {qt}{self.schema.name}{qt} DROP INDEX {qt}{i.name}{qt}"
                        )
                else:
                    sqls.append(f"DROP INDEX {qt}{i.name}{qt}")
            for f in self.add_fields:
                sqls.append(
                    f"ALTER TABLE {qt}{self.schema.name}{qt} ADD COLUMN {f.to_sql(type=type)}"
                )
                if not isinstance(f.default_value, f.NoDefault):
                    sqls[
                        -1
                    ] += f" DEFAULT {sqlalchemy.text(':df').bindparams(df=f.to_database(f.default_value)).compile(compile_kwargs={'literal_binds': True})}"
                    if type != Database.Type.SQLITE:
                        sqls.append(
                            f"ALTER TABLE {qt}{self.schema.name}{qt} ALTER COLUMN {qt}{f.name}{qt} DROP DEFAULT"
                        )
            for f in self.drop_fields:
                sqls.append(
                    f"ALTER TABLE {qt}{self.schema.name}{qt} DROP COLUMN {qt}{f.name}{qt}"
                )
            for f in self.change_type_fields:
                if type == type.SQLITE:
                    raise exception.OperationException(
                        "Type changing not allowed in SQLite"
                    )
                elif type == type.MYSQL:
                    sqls.append(
                        f"ALTER TABLE {qt}{self.schema.name}{qt} MODIFY {qt}{f.name}{qt} {f.type}"
                    )
                else:
                    sqls.append(
                        f"ALTER TABLE {qt}{self.schema.name}{qt} ALTER COLUMN {qt}{f.name}{qt} TYPE {f.type}"
                    )
            for i in self.add_indexes:
                sqls.append(
                    f"CREATE {'UNIQUE ' if i.unique else ''}INDEX {qt}{i.name}{qt} on {qt}{self.schema.name}{qt} ({','.join(qt + f.name + qt for f in i.fields)})"
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


@dataclasses.dataclass
class Model:
    id: int = field(IntField, primary=True, auto_increment=True)
    # for table schema
    _table_prefix: typing.ClassVar[str] = ""
    _table_index_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = tuple()
    _table_unique_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = tuple()
    _table_abstracted: typing.ClassVar[
        bool
    ] = True  # do not impact subclass, default false for every class except defined as true
    _schema: typing.ClassVar[typing.Optional[Schema]] = None

    @class_property
    @classmethod
    def table_name(cls) -> str:
        return cls.get_table_name()

    @class_property
    @classmethod
    def schema(cls) -> Schema:
        if not cls._schema or not cls._schema.model or cls._schema.model is not cls:
            cls._schema = Schema.from_model(cls)
        return cls._schema

    @property
    def primary(self) -> int:
        assert self.schema.primary_field
        return getattr(self, self.schema.primary_field.model_name)

    def __post_init__(self):
        self.after_init()

    def after_init(self):
        for f in self.schema.fields:
            value = getattr(self, f.model_name)
            if isinstance(value, Field):
                setattr(self, f.model_name, f.default_value)

    async def after_read(self):
        pass

    async def before_create(self, validate: bool = True):
        if validate:
            await self.validate()

    async def after_create(self):
        pass

    async def before_update(self, validate: bool = True):
        if validate:
            await self.validate()

    async def after_update(self):
        pass

    async def before_save(self, validate: bool = True):
        if validate:
            await self.validate()

    async def after_save(self):
        pass

    async def before_delete(self):
        pass

    async def after_delete(self):
        pass

    async def validate(self):
        for f in self.schema.fields:
            value = getattr(self, f.model_name)
            # choices
            if f.enum:
                if isinstance(value, enum.Enum):
                    value = value.value
                if value not in set((c.value for c in f.enum)):
                    raise exception.ValidateException(
                        f"{self.__class__.__name__}.{f.model_name} value: {value} not in choices: {f.enum}"
                    )
            # no default
            if isinstance(value, f.NoDefault):
                raise exception.ValidateException(
                    f"{self.table_name}.{f.model_name} required!"
                )

    def dump(self, fields: typing.Sequence[Field] = ()) -> typing.Dict[str, typing.Any]:
        """Dump model to dict with only database fields"""
        data = {}
        field_names = {f.name for f in fields}
        for f in self.schema.fields:
            if field_names and f.name not in field_names:
                continue
            data[f.name] = getattr(self, f.model_name)

        return data

    async def create(
        self: MODEL_TV,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        validate: bool = True,
    ):
        data = self.dump(fields=fields)
        if (
            self.schema.primary_field.name in data
            and not data[self.schema.primary_field.name]
        ):
            data.pop(self.schema.primary_field.name)
        await self.before_create(validate=validate)
        setattr(
            self,
            self.schema.primary_field.model_name,
            (
                await Insert(
                    model=self.__class__, database=database, insert_data=[data]
                ).exec()
            )[0],
        )
        await self.after_create()
        return self

    async def update(
        self: MODEL_TV,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        validate: bool = True,
    ) -> bool:
        """
        For PostgreSQL/SQLite, always return True
        """
        assert self.primary
        await self.before_update(validate=validate)
        data = self.dump(fields=fields)
        rowcount = await self.__class__.where(
            self.schema.primary_field == self.primary,
            database=database,
        ).update(**data)
        await self.after_update()
        return rowcount > 0

    async def save(
        self: MODEL_TV,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        force_insert=False,
        validate: bool = True,
    ) -> MODEL_TV:
        await self.before_save(validate=validate)
        if self.primary and not force_insert:
            await self.update(database=database, fields=fields, validate=False)
        else:
            await self.create(database=database, fields=fields, validate=False)
        await self.after_save()
        return self

    async def delete(
        self,
        database: typing.Optional[Database] = None,
    ) -> bool:
        await self.before_delete()
        row_count = await self.__class__.where(
            self.schema.primary_field == self.primary, database=database
        ).delete()
        await self.after_delete()
        return row_count > 0

    async def refetch(
        self: MODEL_TV,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = tuple(),
    ) -> MODEL_TV:
        new = await self.__class__.where(
            self.schema.primary_field == self.primary, database=database
        ).fetch_one(fields=fields)
        db_fields = {f.model_name for f in fields} or {
            f.model_name for f in self.schema.fields
        }
        for f in dataclasses.fields(self):
            if f.name in db_fields:
                setattr(self, f.name, getattr(new, f.name))
        return self

    async def get_or_create(
        self: MODEL_TV,
        key_fields: typing.Sequence[Field],
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        validate: bool = True,
        for_update: bool = False,
    ) -> typing.Tuple[MODEL_TV, bool]:
        if not database:
            # using write db by default
            database = self.__class__.get_database(Operation.CREATE, self.table_name)
        conditions = []
        created = False
        for f in key_fields:
            conditions.append(f == getattr(self, f.model_name))
        where = self.__class__.where(*conditions, database=database, fields=fields)
        if for_update:
            where = where.for_update()
        ins = await where.fetch_one()
        if not ins:
            try:
                ins = await self.create(
                    database=database, fields=fields, validate=validate
                )
                created = True
            except exception.IntegrityError as e:
                where = self.__class__.where(
                    *conditions, database=database, fields=fields
                )
                if for_update:
                    where = where.for_update()
                ins = await where.fetch_one()
                if not ins:
                    raise e
        assert ins
        return ins, created

    async def create_or_update(
        self: MODEL_TV,
        key_fields: typing.Sequence[Field],
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        update_fields: typing.Sequence[Field] = (),
        for_update: bool = True,
        validate: bool = True,
    ) -> typing.Tuple[MODEL_TV, bool, bool]:
        """Try get_or_create then update.

        For SQLite, updated will always be True.
        """
        if not database:
            database = self.__class__.get_database(Operation.CREATE, self.table_name)
        if fields and self.schema.primary_field.name not in (f.name for f in fields):
            fields = list(fields)
            fields.append(self.schema.primary_field)

        created = False
        updated = False
        async with database.transaction():
            ins, created = await self.get_or_create(
                key_fields,
                database=database,
                fields=fields,
                validate=validate,
                for_update=for_update,
            )
            if not created:
                setattr(self, self.schema.primary_field.model_name, ins.primary)
                updated = await self.update(validate=validate, fields=update_fields)
                ins = self
        return ins, created, updated

    @classmethod
    def get_table_name(cls) -> str:
        return cls._table_prefix + cls.__name__.lower()

    @classmethod
    def get_database(
        cls, operation: Operation, table: str, *args, **kwargs
    ) -> Database:
        """Get database instance, route database by operation"""

    @classmethod
    def load(
        cls: typing.Type[MODEL_TV], rows: typing.List[typing.Mapping]
    ) -> typing.List[MODEL_TV]:
        """Load DB data to model"""
        instances = []
        for row in rows:
            data = {}
            for f in cls.schema.fields:
                if f.name in row:
                    data[f.model_name] = f.to_python(row[f.name])
            instances.append(cls(**data))
        return instances

    @classmethod
    def where(
        cls: typing.Type[MODEL_TV],
        *conditions: SQLExpression,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = tuple(),
        raw="",
        is_and=True,
    ) -> Crud[MODEL_TV]:
        cls.schema
        return Crud(model=cls, fields=fields, database=database).where(
            *conditions, is_and=is_and, raw=raw
        )

    @classmethod
    async def upsert(
        cls,
        insert_data: typing.List[typing.Dict[str, typing.Any]],
        database: typing.Optional[Database] = None,
        update_fields: typing.Sequence[str] = (),
        conflict_targets: typing.Sequence[str] = (),
    ) -> typing.Tuple[bool, bool]:
        """
        Using insert on duplicate:
          https://dev.mysql.com/doc/refman/5.6/en/insert-on-duplicate.html
          https://www.sqlite.org/lang_upsert.html
          https://www.postgresql.org/docs/9.4/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING

        For MySQL, rowcount=2 means the table has been updated
        For SQLITE, rowcount!=0 will always be true
        For PostgreSQL, last_id != 0 and rowcount != 0 always be true
        """
        insert = Insert(
            model=cls,
            database=database,
            insert_data=insert_data,
            update_fields=update_fields,
            conflict_targets=conflict_targets,
        )
        last_id, rowcount = await insert.exec()
        if insert.get_database().type == Database.Type.MYSQL:
            return rowcount == 1, rowcount == 2
        else:
            return last_id != 0, rowcount != 0

    @classmethod
    async def bulk_create(
        cls: typing.Type[MODEL_TV],
        instances: typing.Sequence[MODEL_TV],
        fields: typing.Sequence[Field] = (),
        database: typing.Optional[Database] = None,
        validate: bool = True,
    ) -> typing.Sequence[MODEL_TV]:
        assert cls.schema.primary_field
        for ins in instances:
            await ins.before_create(validate=validate)

        if not database:
            database = cls.get_database(Operation.CREATE, cls.table_name)
        data = [ins.dump(fields=fields) for ins in instances]
        if database.type != Database.Type.MYSQL:
            for d in data:
                if (
                    cls.schema.primary_field.name in d
                    and not d[cls.schema.primary_field.name]
                ):
                    d.pop(cls.schema.primary_field.name)
            if len({len(d) for d in data}) != 1:
                raise exception.OperationException(
                    "For SQLite or PostgreSQL, all instances either have primary key value or none"
                )

        next_ins_id = (
            await Insert(model=cls, database=database, insert_data=data).exec()
        )[0]
        if database.type == Database.Type.MYSQL:
            for ins in instances:
                if not ins.primary:
                    setattr(ins, cls.schema.primary_field.model_name, next_ins_id)
                next_ins_id = ins.primary + 1
        else:
            for ins in reversed(instances):
                if not ins.primary:
                    setattr(ins, cls.schema.primary_field.model_name, next_ins_id)
                next_ins_id = ins.primary - 1

        for ins in instances:
            await ins.after_create()
        return instances

    @classmethod
    async def bulk_update(
        cls: typing.Type[MODEL_TV],
        instances: typing.Sequence[MODEL_TV],
        fields: typing.Sequence[Field] = (),
        database: typing.Optional[Database] = None,
        validate: bool = True,
    ) -> typing.Sequence[MODEL_TV]:
        assert cls.schema.primary_field
        for ins in instances:
            await ins.before_update(validate=validate)

        data = []
        for ins in instances:
            assert ins.primary, "Need primary"
            data.append(ins.dump(fields=fields))
            data[-1][cls.schema.primary_field.name] = ins.primary

        await CaseUpdate(
            model=cls,
            database=database,
            data=data,
        ).exec()

        for ins in instances:
            await ins.after_update()
        return instances

    @classmethod
    async def bulk_delete(
        cls,
        instances: typing.Sequence[MODEL_TV],
        database: typing.Optional[Database] = None,
    ) -> int:
        for ins in instances:
            await ins.before_delete()
        row_count = await cls.where(
            cls.schema.primary_field.contains(tuple(ins.primary for ins in instances)),
            database=database,
        ).delete()
        for ins in instances:
            await ins.after_delete()
        return row_count

    # deprecation
    @classmethod
    async def count(cls, *args, **kwargs):
        warnings.warn("Will discard", DeprecationWarning)
        return await cls.where(*args, **kwargs).fetch_count()

    @classmethod
    async def select(cls, *args, **kwargs):
        warnings.warn("Will discard", DeprecationWarning)
        return await cls.where(*args, **kwargs).fetch_all()

    @classmethod
    async def get(cls, *args, **kwargs):
        warnings.warn("Will discard", DeprecationWarning)
        return await cls.where(*args, **kwargs).fetch_one()


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
            return value.name
        elif isinstance(value, SQLMarker):
            return value.sync(self).to_sql(type=type)
        elif self.field:
            return f":{self.mark(self.field.to_database(value))}"
        else:
            return f":{self.mark(value)}"

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        """For _parse method"""


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
        qt = Database.get_quote(type)

        sql = f"{qt}{self.field.name}{qt}"
        for op, value in self.values:
            if op == self.Operator.IN:
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
        if type == Database.Type.POSTGRES:
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
    OPERATION: typing.ClassVar[Operation] = Operation.READ

    model: typing.Optional[typing.Type[Model]] = None
    database: typing.Optional[Database] = None

    def get_database(self, operation: typing.Optional[Operation] = None):
        assert self.model
        if not self.database:
            return self.model.get_database(
                operation or self.OPERATION, self.model.table_name
            )
        else:
            return self.database


@dataclasses.dataclass
class Crud(BaseSQLBuilder, typing.Generic[MODEL_TV]):
    OPERATION: typing.ClassVar[Operation] = Operation.READ

    model: typing.Optional[typing.Type[MODEL_TV]] = None
    fields: typing.Sequence[Field] = tuple()
    database: typing.Optional[Database] = None
    _where: typing.Optional[SQLExpression] = None
    _raw_where: str = ""
    _limit: int = 0
    _offset: int = 0
    _order_by: typing.Optional[typing.Union[Field, SQLExpression]] = None
    _order_by_asc: bool = False
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

    async def fetch_all(
        self, fields: typing.Sequence[Field] = tuple()
    ) -> typing.List[MODEL_TV]:
        assert self.model
        self.fields = fields if fields else self.fields
        database = self.get_database(Operation.READ)
        instances = self.model.load(
            await database.fetch_all(self.to_select_sql(type=database.type), self._vars)
        )
        for ins in instances:
            await ins.after_read()

        return instances

    async def fetch_one(
        self, fields: typing.Sequence[Field] = tuple()
    ) -> typing.Optional[MODEL_TV]:
        assert self.model
        self.fields = fields if fields else self.fields
        database = self.get_database(Operation.READ)
        data = await database.fetch_one(
            self.to_select_sql(type=database.type), self._vars
        )
        if data:
            ins = self.model.load([data])[0]
            await ins.after_read()
            return ins
        else:
            return None

    async def fetch_row(
        self, fields: typing.Sequence[Field] = tuple()
    ) -> typing.List[typing.Mapping]:
        self.fields = fields if fields else self.fields
        database = self.get_database(Operation.READ)
        return await database.fetch_all(
            self.to_select_sql(type=database.type), self._vars
        )

    async def fetch_count(self) -> int:
        database = self.get_database(Operation.READ)
        data = await database.fetch_one(
            self.to_select_sql(count=True, type=database.type), self._vars
        )
        assert data
        return data[0]

    async def delete(self) -> int:
        database = self.get_database(Operation.DELETE)
        return (
            await database.execute(self.to_delete_sql(type=database.type), self._vars)
        )[1]

    async def update(self, **data) -> int:
        database = self.get_database(Operation.UPDATE)
        return (
            await database.execute(
                self.to_update_sql(data, type=database.type), self._vars
            )
        )[1]

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

    def limit(self: CRUD_TV, n: int) -> CRUD_TV:
        self._limit = n
        return self

    def offset(self: CRUD_TV, n: int) -> CRUD_TV:
        self._offset = n
        return self

    def order_by(
        self: CRUD_TV, f: typing.Union[Field, SQLExpression], asc=True
    ) -> CRUD_TV:
        self._order_by = f
        self._order_by_asc = asc
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

    def to_select_sql(
        self, count=False, type: Database.Type = Database.Type.MYSQL
    ) -> str:
        assert self.model
        qt = Database.get_quote(type)

        if not count:
            sql = f"SELECT {', '.join(f'{qt}{f.name}{qt}' for f in self.fields or self.model.schema.fields)} FROM {qt}{self.model.table_name}{qt}"
        else:
            sql = f"SELECT COUNT(*) FROM {qt}{self.model.table_name}{qt}"
        if type == type.MYSQL:
            for indexes in self._use_indexes:
                sql += f" USE INDEX {indexes[1] if indexes[1] else ''} ({','.join(indexes[0])}) "
            for indexes in self._ignore_indexes:
                sql += f" IGNORE INDEX {indexes[1] if indexes[1] else ''} ({','.join(indexes[0])}) "
            for indexes in self._force_indexes:
                sql += f" FORCE INDEX {indexes[1] if indexes[1] else ''} ({','.join(indexes[0])}) "
        elif type == type.SQLITE:
            _use_indexes = self._use_indexes or self._force_indexes
            if _use_indexes:
                sql += f" INDEXED BY {_use_indexes[0][0][0]} "
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
                if isinstance(self._order_by, Field):
                    sql += f" ORDER BY {qt}{self._order_by.name}{qt} {'ASC' if self._order_by_asc else 'DESC'}"
                elif isinstance(self._order_by, SQLExpression):
                    sql += f" ORDER BY {self._order_by.sync(self).to_sql(type=type)} {'ASC' if self._order_by_asc else 'DESC'}"
            if self._limit:
                sql += f" LIMIT :{self.mark(self._limit)}"
            if self._offset:
                assert self._limit, "Offset need limit"
                sql += f" OFFSET :{self.mark(self._offset)}"
            if self._for_update:
                if type == Database.Type.SQLITE:
                    raise exception.OperationException(
                        "For SQLite - do not support FOR UPDATE lock"
                    )
                else:
                    sql += " FOR UPDATE"
            elif self._for_share and type != Database.Type.SQLITE:
                if type == Database.Type.SQLITE:
                    raise exception.OperationException(
                        "For SQLite - do not support FOR UPDATE lock"
                    )
                elif type == Database.Type.MYSQL:
                    sql += " LOCK IN SHARE MODE"
                else:
                    sql += " FOR SHARE"

        return sql + ";"

    def to_delete_sql(self, type: Database.Type = Database.Type.MYSQL):
        assert self.model
        qt = Database.get_quote(type)

        sql = f"DELETE from {qt}{self.model.table_name}{qt}"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql(type=type)}"
        return sql + ";"

    def to_update_sql(
        self,
        data: typing.Dict[str, typing.Any],
        type: Database.Type = Database.Type.MYSQL,
    ):
        assert self.model
        qt = Database.get_quote(type)

        fields = {f.model_name: f for f in self.model.schema.fields}
        _sqls = []
        for k, v in data.items():
            if isinstance(v, SQLMarker):
                _sqls.append(f"{qt}{k}{qt} = {v.sync(self).to_sql(type=type)}")
            else:
                _sqls.append(f"{qt}{k}{qt} = :{self.mark(fields[k].to_database(v))}")
        sql = f"UPDATE {qt}{self.model.table_name}{qt} SET {', '.join(_sqls)}"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql(type=type)}"
        return sql + ";"


@dataclasses.dataclass
class Insert(BaseSQLBuilder):
    OPERATION: typing.ClassVar[Operation] = Operation.CREATE
    insert_data: typing.Sequence[typing.Dict[str, str]] = dataclasses.field(
        default_factory=list
    )
    update_fields: typing.Sequence[str] = dataclasses.field(default_factory=list)
    conflict_targets: typing.Sequence[str] = dataclasses.field(default_factory=list)

    async def exec(self) -> typing.Tuple[int, int]:
        """
        return last_id, row_count
        """
        database = self.get_database()
        return await database.execute(self.to_sql(type=database.type), self._vars)

    def to_sql(self, type: Database.Type = Database.Type.MYSQL) -> str:
        assert self.insert_data
        assert self.model
        qt = Database.get_quote(type)
        fields = {f.name: f for f in self.model.schema.fields}

        keys = list(self.insert_data[0].keys())
        vars = []
        for d in self.insert_data:
            _vars = []
            for k in keys:
                _vars.append(self.mark(fields[k].to_database(d[k])))
            vars.append(_vars)
        sql = f"INSERT INTO {qt}{self.model.table_name}{qt} ({', '.join(map(lambda x: f'{qt}{x}{qt}', keys))}) VALUES"
        insert_value_sql = []
        for vs in vars:
            insert_value_sql.append(f"({', '.join(':' + v for v in vs)})")
        sql = sql + ", ".join(insert_value_sql)
        # upsert
        if self.update_fields:
            update_value_sql = []
            _sql = ""
            if type == Database.Type.MYSQL:
                if self.conflict_targets:
                    raise exception.OperationException(
                        "For MySQL - conflict_target not support"
                    )
                _sql = " ON DUPLICATE KEY UPDATE "
                for k in self.update_fields:
                    update_value_sql.append(f"{k} = VALUES({k})")
            elif type == Database.Type.SQLITE:
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
        if type == Database.Type.POSTGRES:
            sql += f" RETURNING {self.model.schema.primary_field.name}"
        return sql + ";"


@dataclasses.dataclass
class CaseUpdate(BaseSQLBuilder):
    OPERATION: typing.ClassVar[Operation] = Operation.UPDATE
    data: typing.List[typing.Dict[str, str]] = dataclasses.field(default_factory=list)

    async def exec(self) -> int:
        database = self.get_database()
        await database.execute(self.to_sql(type=database.type), self._vars)
        return 1

    def to_sql(self, type: Database.Type = Database.Type.MYSQL):
        assert self.data
        assert self.model
        qt = Database.get_quote(type)
        fields = {f.name: f for f in self.model.schema.fields}

        parse_data: typing.DefaultDict[str, typing.Dict[str, typing.Any]] = defaultdict(
            dict
        )
        primary_values = []
        for d in self.data:
            for k, v in d.items():
                if k == self.model.schema.primary_field.name:
                    primary_values.append(v)
                else:
                    parse_data[k][d[self.model.schema.primary_field.name]] = fields[
                        k
                    ].to_database(v)
        sql = f"UPDATE {qt}{self.model.table_name}{qt} SET"
        _sqls = []
        for k, vs in parse_data.items():
            case = SQLCase(cast_type=fields[k].type)
            for pv, v in vs.items():
                case.case(self.model.schema.primary_field == pv, v)
            _sqls.append(f" {qt}{k}{qt} = {case.sync(self).to_sql(type=type)}")
        sql += ", ".join(_sqls)
        sql += f" WHERE {(self.model.schema.primary_field.contains(primary_values)).sync(self).to_sql(type=type)};"

        return sql
