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
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import reduce

import pymysql
from pymysql import converters

from .database import Database
from .exception import SchemaException, ValidateException
from .utils import class_property

MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")
SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")
MIGRATION_TV = typing.TypeVar("MIGRATION_TV", bound="Migration")
CURD_TV = typing.TypeVar("CURD_TV", bound="Curd")
MARKER_TV = typing.TypeVar("MARKER_TV", bound="SQLMarker")
CASE_TV = typing.TypeVar("CASE_TV", bound="SQLCase")


@dataclasses.dataclass
class Field:
    class FieldDefault:
        pass

    class NoDefault:
        pass

    COMMENT_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"COMMENT '(.*)'")
    TYPE: typing.ClassVar[str] = ""

    name: str = ""
    model_name: str = ""
    default: typing.Any = NoDefault  # for model layer
    describe: str = ""
    type: str = ""
    auto_increment: bool = False
    comment: str = ""
    enum: typing.Optional[typing.Type[enum.Enum]] = None

    @property
    def default_value(self) -> typing.Any:
        if callable(self.default):
            return self.default()
        else:
            return copy.copy(self.default)

    def __post_init__(self):
        # from schema sql
        if self.describe:
            self.auto_incrment = "AUTO_INCREMENT" in self.describe
            self.type = self.describe.split(" ")[1]
            tmp = self.COMMENT_PATTERN.findall(self.describe)
            if tmp:
                self.comment = tmp[0]
        # from model field
        if not self.describe and not self.type and self.TYPE:
            self.type = self.TYPE
        if not self.describe and self.name and self.type:
            self.describe = f"`{self.name}` {self.type} NOT NULL {'AUTO_INCREMENT ' if self.auto_increment else ' '}COMMENT '{self.comment}'"

        if self.enum and not isinstance(self.default, self.enum):
            self.default = list(self.enum)[0]

    def __hash__(self):
        return hash(
            (self.name, self.type.split("(")[0] if "int" in self.type else self.type)
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
        return SQLCase(field=self, default=default or self.default).case(
            expression, value
        )

    def to_sql(self) -> str:
        assert self.describe
        return self.describe

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
    TYPE: typing.ClassVar[str] = "int(10)"

    default: int = 0


@dataclasses.dataclass(eq=False)
class SmallIntField(IntField):
    TYPE: typing.ClassVar[str] = "smallint(6)"


@dataclasses.dataclass(eq=False)
class TinyIntField(IntField):
    TYPE: typing.ClassVar[str] = "tinyint(4)"


@dataclasses.dataclass(eq=False)
class BoolField(Field):
    TYPE: typing.ClassVar[str] = "tinyint(1)"

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
    TYPE: typing.ClassVar[str] = "bigint(20)"


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
class ComplexField(Field):
    def to_database(self, value: typing.Any) -> str:
        return str(value)


@dataclasses.dataclass(eq=False)
class TimeField(ComplexField):
    """Using timedelta other than time which only support from '00:00:00' to '23:59:59'"""

    TYPE: typing.ClassVar[str] = "time"

    default: timedelta = timedelta(0)


@dataclasses.dataclass(eq=False)
class DateField(ComplexField):
    TYPE: typing.ClassVar[str] = "date"

    default: date = date.fromtimestamp(0)


@dataclasses.dataclass(eq=False)
class DateTimeField(ComplexField):
    TYPE: typing.ClassVar[str] = "datetime"

    default: datetime = datetime.fromtimestamp(0)


@dataclasses.dataclass(eq=False)
class JsonField(Field):
    TYPE: typing.ClassVar[str] = "varchar(2048)"

    default: typing.Any = dataclasses.field(default_factory=dict)

    def to_python(self, value: str) -> typing.Any:
        return json.loads(value)

    def to_database(self, value: typing.Any) -> str:
        if not isinstance(value, str):
            return json.dumps(value)
        else:
            return value


def field(
    field_cls=Field,
    type="",
    name="",
    comment="",
    default=Field.FieldDefault,
    auto_increment=False,
    enum: typing.Optional[typing.Type[enum.Enum]] = None,
) -> typing.Any:
    extras = {}
    if (
        default is not Field.FieldDefault
    ):  # default to field default, allow None defalut
        extras["default"] = default

    return field_cls(
        name=name,
        type=type,
        comment=comment,
        auto_increment=auto_increment,
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
            self.name = f"`{'_'.join(f.name for f in self.fields)[:15]}_{random.randint(1, 10000)}{'_uiq' if self.unique else '_idx'}` "

    def __hash__(self):
        return hash((self.unique, tuple(f.name for f in self.fields)))

    def __eq__(self, other: object) -> bool:
        assert isinstance(other, Index)
        return self.__hash__() == other.__hash__()

    def to_sql(self) -> str:
        return (
            f"{'UNIQUE ' if self.unique else ''}KEY "
            f"{self.name}"
            f"({', '.join(f'`{f.name}`' for f in self.fields)})"
        )


@dataclasses.dataclass
class Schema:
    POSTFIX: typing.ClassVar[
        str
    ] = "ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    FIELD_NAME_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"`([^ ,]*)`")

    name: str
    primary_field: Field = Field()
    indexes: typing.Set[Index] = dataclasses.field(default_factory=set)
    fields: typing.Set[Field] = dataclasses.field(default_factory=set)
    abstracted: bool = False
    model: typing.Optional[typing.Type[Model]] = None

    def __hash__(self):
        return hash(
            (
                self.name,
                tuple(f for f in sorted(self.fields, key=lambda f: f.name)),
                self.primary_field,
                tuple(i for i in sorted(self.indexes, key=lambda f: f.name)),
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
        add_fields = set(self.fields) - set(other.fields)
        _add_fields = {f.name: f for f in add_fields}
        drop_fields = set(other.fields) - set(self.fields)
        _drop_fields = {f.name: f for f in drop_fields}
        change_type_fileds = []
        change_type_field_names = set(f.name for f in add_fields) & set(
            f.name for f in drop_fields
        )
        for f_name in change_type_field_names:
            field = _add_fields[f_name]
            add_fields.remove(field)
            drop_fields.remove(_drop_fields[field.name])
            change_type_fileds.append(field)

        return Migration(
            schema=self,
            old_schema=other,
            add_indexes=list(set(self.indexes) - set(other.indexes)),
            drop_indexes=list(set(other.indexes) - set(self.indexes)),
            add_fields=list(add_fields),
            drop_fields=list(drop_fields),
            change_type_fields=change_type_fileds,
        )

    def to_sql(self) -> str:
        assert self.primary_field

        keys = [f"PRIMARY KEY (`{self.primary_field.name}`)"]
        keys.extend([index.to_sql() for index in self.indexes])

        return (
            f"CREATE TABLE `{self.name}` (\n"
            + ",\n".join(itertools.chain((v.to_sql() for v in self.fields), keys))
            + f"\n) {self.POSTFIX}"
        )

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
        schema.primary_field = m._table_primary_key
        fields = {f.model_name: f for f in schema.fields}
        # index
        for i, index_keys in enumerate((m._table_index_keys, m._table_unique_keys)):
            for keys in index_keys:
                if keys:
                    _fileds = []
                    for key in keys:
                        if isinstance(key, Field):
                            _fileds.append(key)
                        elif isinstance(key, str) and key in fields:
                            _fileds.append(fields[key])
                        else:
                            raise SchemaException(f"Index: {keys} not supported")
                    schema.indexes.add(Index(fields=_fileds, unique=i == 1))
        return schema

    @classmethod
    async def from_db(
        cls: typing.Type[SCHEMA_TV], database: Database, m: typing.Type[Model]
    ) -> typing.Optional[SCHEMA_TV]:
        schema = cls(name=m.table_name, model=m)
        db_names = {f.name: f.model_name for f in m.schema.fields}
        try:
            for line in (await database.fetch_all(f"SHOW CREATE TABLE {m.table_name}"))[
                0
            ][1].split("\n")[1:-1]:
                if "PRIMARY KEY" in line:
                    db_name = cls.FIELD_NAME_PATTERN.findall(line)[0]
                    for f in schema.fields:
                        if db_name == f.name:
                            schema.primary_field = f
                            break
                elif "KEY" in line:
                    fields = {f.name: f for f in schema.fields}
                    index_fileds = []
                    _names = cls.FIELD_NAME_PATTERN.findall(line)
                    index_name = _names[0]
                    index_fileds = [fields[n] for n in _names[1:]]
                    schema.indexes.add(
                        Index(
                            fields=index_fileds,
                            unique="UNIQUE" in line,
                            name=index_name,
                        )
                    )
                else:
                    db_name = cls.FIELD_NAME_PATTERN.findall(line)[0]
                    if db_name in db_names:
                        name = db_names[db_name]
                    else:
                        name = ""
                    schema.fields.add(
                        Field(
                            name=db_name,
                            describe=line[2:].replace(",", ""),
                            model_name=name,
                        )
                    )
        except Exception as e:
            if "doesn't exist" in str(e):
                return None
            raise e

        return schema


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

    def to_sql(self) -> str:
        sqls = []
        if self.schema and not self.old_schema:
            sqls.append(self.schema.to_sql())
        elif self.old_schema and not self.schema:
            sqls.append(f"DROP TABLE `{self.old_schema.name}`")
        elif self.schema and self.old_schema:
            if self.old_schema.name != self.schema.name:
                sqls.append(
                    f"ALTER TABLE `{self.old_schema.name}` RENAME `{self.schema.name}`"
                )
            for f in self.add_fields:
                sqls.append(f"ALTER TABLE `{self.schema.name}` ADD COLUMN {f.to_sql()}")
                if not isinstance(f.default_value, f.NoDefault):
                    sqls[
                        -1
                    ] += f" DEFAULT {converters.escape_item(f.to_database(f.default_value), None)}"
                    sqls.append(
                        f"ALTER TABLE `{self.schema.name}` ALTER COLUMN `{f.name}` DROP DEFAULT"
                    )
            for f in self.drop_fields:
                sqls.append(f"ALTER TABLE `{self.schema.name}` DROP COLUMN `{f.name}`")
            for f in self.change_type_fields:
                sqls.append(
                    f"ALTER TABLE `{self.schema.name}` MODIFY `{f.name}` {f.type}"
                )
            for i in self.add_indexes:
                sqls.append(
                    f"CREATE {'UNIQUE ' if i.unique else ''}INDEX {i.name} on `{self.schema.name}` ({','.join('`' + f.name + '`' for f in i.fields)})"
                )
            for i in self.drop_indexes:
                if not set(i.fields) & set(self.drop_fields):
                    sqls.append(f"ALTER TABLE `{self.schema.name}` DROP INDEX {i.name}")
        if sqls:
            sqls[-1] += ";"

        return ";\n".join(sqls)


@dataclasses.dataclass
class Model:
    @enum.unique
    class Operation(enum.IntEnum):
        CREATE = 1
        READ = 2
        UPDATE = 3
        DELETE = 4

    id: int = field(field_cls=IntField, auto_increment=True, default=0)
    # for table schema
    _table_prefix: typing.ClassVar[str] = ""
    _table_primary_key: typing.ClassVar[typing.Any] = id
    _table_index_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = ((),)
    _table_unique_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = ((),)
    _table_abstracted: typing.ClassVar[
        bool
    ] = True  # do not impact subclass, default false for every class except defined as true
    _schema: typing.ClassVar[typing.Optional[Schema]] = None

    def __post_init__(self):
        for f in self.schema.fields:
            value = getattr(self, f.model_name)
            if isinstance(value, Field):
                setattr(self, f.model_name, f.default_value)

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

    def dump(self, fields: typing.Sequence[Field] = ()) -> typing.Dict[str, typing.Any]:
        """Dump dataclass to DB level dict"""
        data = {}
        field_names = {f.name for f in fields}
        for f in self.schema.fields:
            if field_names and f.name not in field_names:
                continue
            data[f.name] = f.to_database(getattr(self, f.model_name))

        return data

    async def validate(self):
        for f in self.schema.fields:
            value = getattr(self, f.model_name)
            # choices
            if f.enum:
                if isinstance(value, enum.Enum):
                    value = value.value
                if value not in set((c.value for c in f.enum)):
                    raise ValidateException(
                        f"{self.__class__.__name__}.{f.model_name} value: {value} not in choices: {f.enum}"
                    )
            # no default
            if isinstance(value, f.NoDefault):
                raise ValidateException(f"{self.table_name}.{f.model_name} required!")

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

    async def create(
        self: MODEL_TV,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        validate: bool = True,
    ):
        data = self.dump(fields=fields)
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
        assert self.primary
        await self.before_update(validate=validate)
        data = self.dump(fields=fields)
        rowcount = await self.__class__.update_many(
            self.schema.primary_field == self.primary,
            database=database,
            **data,
        )
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
        row_count = await self.__class__.delete_many(
            self.schema.primary_field == self.primary, database=database
        )
        await self.after_delete()
        return row_count > 0

    async def get_or_create(
        self: MODEL_TV,
        key_fields: typing.Sequence[Field],
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        validate: bool = True,
        for_udpate: bool = False,
    ) -> typing.Tuple[MODEL_TV, bool]:
        if not database:
            # using write db by default
            database = self.__class__.get_database(
                self.Operation.CREATE, self.table_name
            )
        conditons = []
        created = False
        for f in key_fields:
            conditons.append(f == getattr(self, f.model_name))
        ins = await self.__class__.get(
            *conditons, database=database, fields=fields, for_update=for_udpate
        )
        if not ins:
            try:
                ins = await self.create(
                    database=database, fields=fields, validate=validate
                )
                created = True
            except pymysql.IntegrityError as e:
                ins = await self.__class__.get(
                    *conditons, database=database, fields=fields, for_update=for_udpate
                )
                if not ins:
                    raise e
        assert ins
        return ins, created

    async def create_or_update(
        self: MODEL_TV,
        key_fields: typing.Sequence[Field],
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = (),
        validate: bool = True,
    ) -> typing.Tuple[MODEL_TV, bool, bool]:
        if not database:
            database = self.__class__.get_database(
                self.Operation.CREATE, self.table_name
            )
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
                for_udpate=True,
            )
            if not created:
                setattr(self, self.schema.primary_field.model_name, ins.primary)
                updated = await self.update(validate=validate, fields=fields)
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
        row="",
        is_and=True,
    ) -> Curd[MODEL_TV]:
        return Curd(model=cls, fields=fields, database=database).where(
            *conditions, is_and=is_and, row=row
        )

    @classmethod
    async def select(
        cls: typing.Type[MODEL_TV],
        *conditions: SQLExpression,
        fields: typing.Sequence[Field] = tuple(),
        order_by: typing.Optional[typing.Union[Field, SQLExpression]] = None,
        order_by_asc=False,
        database: typing.Optional[Database] = None,
        for_update=False,
        for_share=False,
        limit=0,
        offset=0,
    ) -> typing.List[MODEL_TV]:
        return (
            await Curd(  # type: ignore
                model=cls,
                fields=fields,
                _order_by=order_by,
                _order_by_asc=order_by_asc,
                database=database,
                _for_update=for_update,
                _for_share=for_share,
                _limit=limit,
                _offset=offset,
            )
            .where(*conditions)
            .fetch_all()
        )

    @classmethod
    async def get(
        cls: typing.Type[MODEL_TV],
        *conditions: SQLExpression,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = tuple(),
        order_by: typing.Optional[typing.Union[Field, SQLExpression]] = None,
        for_update=False,
        for_share=False,
        offset=0,
    ) -> typing.Optional[MODEL_TV]:
        return (
            await Curd(  # type: ignore
                model=cls,
                fields=fields,
                _order_by=order_by,
                database=database,
                _for_update=for_update,
                _for_share=for_share,
                _limit=1,
                _offset=offset,
            )
            .where(*conditions)
            .fetch_one()
        )

    @classmethod
    async def count(
        cls: typing.Type[MODEL_TV],
        *conditions: SQLExpression,
        fields: typing.Sequence[Field] = tuple(),
        database: typing.Optional[Database] = None,
    ) -> int:
        return (
            await Curd(model=cls, database=database, fields=fields)
            .where(*conditions)
            .fetch_count()
        )

    @classmethod
    async def update_many(
        cls: typing.Type[MODEL_TV],
        *conditions: SQLExpression,
        database: typing.Optional[Database] = None,
        **data: typing.Any,
    ) -> int:
        return (
            await Curd(model=cls, database=database).where(*conditions).update(**data)
        )

    @classmethod
    async def delete_many(
        cls: typing.Type[MODEL_TV],
        *conditions: SQLExpression,
        database: typing.Optional[Database] = None,
    ) -> int:
        return await Curd(model=cls, database=database).where(*conditions).delete()

    @classmethod
    async def upsert(
        cls,
        insert_data: typing.List[typing.Dict[str, typing.Any]],
        database: typing.Optional[Database] = None,
        update_fields: typing.Sequence[str] = (),
    ) -> typing.Tuple[bool, bool]:
        """
        Using insert on duplicate: https://dev.mysql.com/doc/refman/5.6/en/insert-on-duplicate.html
        """
        _, rowcount = await Insert(
            model=cls,
            database=database,
            insert_data=insert_data,
            update_fields=update_fields,
        ).exec()
        return rowcount == 1, rowcount == 2

    @classmethod
    async def bulk_create(
        cls: typing.Type[MODEL_TV],
        instances: typing.Sequence[MODEL_TV],
        database: typing.Optional[Database] = None,
        validate: bool = True,
    ) -> typing.Sequence[MODEL_TV]:
        assert cls.schema.primary_field
        for ins in instances:
            await ins.before_create(validate=validate)

        data = [ins.dump() for ins in instances]
        next_ins_id = (
            await Insert(model=cls, database=database, insert_data=data).exec()
        )[0]

        for ins in instances:
            if not ins.primary:
                setattr(ins, cls.schema.primary_field.model_name, next_ins_id)
            next_ins_id = ins.primary + 1

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
        row_count = await cls.delete_many(
            cls.schema.primary_field.contains(tuple(ins.primary for ins in instances)),
            database=database,
        )
        for ins in instances:
            await ins.after_delete()
        return row_count


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

    def _parse(self, value: typing.Any) -> str:
        if isinstance(value, Field):
            return value.name
        elif isinstance(value, SQLMarker):
            return value.sync(self).to_sql()
        elif self.field:
            return f":{self.mark(self.field.to_database(value))}"
        else:
            return f":{self.mark(value)}"

    def to_sql(self) -> str:
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

    def to_sql(self):
        assert self.field

        sql = f"`{self.field.name}`"
        for op, value in self.values:
            if op == self.Operator.IN:
                sql += f" {op.value} ({', '.join(self._parse(v) for v in value)})"
            elif isinstance(value, SQLExpression):
                sql = f"({sql}) {op.value} ({self._parse(value)})"
            else:
                sql += f" {op.value} {self._parse(value)}"
        return sql


@dataclasses.dataclass
class SQLCase(SQLMarker):
    cases: typing.List[typing.Tuple[SQLExpression, typing.Any]] = dataclasses.field(
        default_factory=list
    )
    default: typing.Any = None

    def case(self: CASE_TV, expression: SQLExpression, value: typing.Any) -> CASE_TV:
        self.cases.append((expression, value))
        return self

    def to_sql(self) -> str:
        assert self.cases

        sql = "CASE"
        for ex, v in self.cases:
            sql += f" WHEN {self._parse(ex)} THEN {self._parse(v)}"
        sql += f" ELSE {self._parse(self.default)} END"

        return sql


@dataclasses.dataclass
class BaseSQLBuilder(SQLMarker):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.READ

    model: typing.Optional[typing.Type[Model]] = None
    database: typing.Optional[Database] = None

    def get_database(self, operation: typing.Optional[Model.Operation] = None):
        assert self.model
        if not self.database:
            return self.model.get_database(
                operation or self.OPERATION, self.model.table_name
            )
        else:
            return self.database


@dataclasses.dataclass
class Curd(BaseSQLBuilder, typing.Generic[MODEL_TV]):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.READ

    model: typing.Optional[typing.Type[MODEL_TV]] = None
    fields: typing.Sequence[Field] = tuple()
    database: typing.Optional[Database] = None
    _where: typing.Optional[SQLExpression] = None
    _row_where: str = ""
    _limit: int = 0
    _offset: int = 0
    _order_by: typing.Optional[typing.Union[Field, SQLExpression]] = None
    _order_by_asc: bool = False
    _for_update: bool = False
    _for_share: bool = False

    async def fetch_all(self) -> typing.List[MODEL_TV]:
        assert self.model
        return self.model.load(
            await self.get_database(Model.Operation.READ).fetch_all(
                self.to_select_sql(), self._vars
            )
        )

    async def fetch_one(self) -> typing.Optional[Model]:
        assert self.model
        data = await self.get_database(Model.Operation.READ).fetch_one(
            self.to_select_sql(), self._vars
        )
        if data:
            return self.model.load([data])[0]
        else:
            return None

    async def fetch_count(self) -> int:
        data = await self.get_database(Model.Operation.READ).fetch_one(
            self.to_select_sql(count=True), self._vars
        )
        assert data
        return data[0]

    async def delete(self) -> int:
        return (
            await self.get_database(Model.Operation.DELETE).execute(
                self.to_delete_sql(), self._vars
            )
        )[1]

    async def update(self, **data) -> int:
        return (
            await self.get_database(Model.Operation.UPDATE).execute(
                self.to_update_sql(data), self._vars
            )
        )[1]

    def where(
        self: CURD_TV,
        *conditions: SQLExpression,
        row="",
        is_and=True,
    ) -> CURD_TV:
        if conditions:
            _where = reduce(lambda c1, c2: c1 & c2 if is_and else c1 | c2, conditions)
            if self._where:
                self._where = self._where & _where
            else:
                self._where = _where
        elif row:
            self._row_where = row

        return self

    def limit(self, n: int) -> Curd:
        self._limit = n
        return self

    def offset(self, n: int) -> Curd:
        self._offset = n
        return self

    def order_by(self, f: typing.Union[Field, SQLExpression], asc=True) -> Curd:
        self._order_by = f
        self._order_by_asc = asc
        return self

    def for_update(self) -> Curd:
        self._for_update = True
        return self

    def for_share(self) -> Curd:
        self._for_share = True
        return self

    def to_select_sql(self, count=False) -> str:
        assert self.model
        if not count:
            sql = f"SELECT {', '.join(f'`{f.name}`' for f in self.fields or self.model.schema.fields)} FROM `{self.model.table_name}`"
        else:
            sql = f"SELECT COUNT(*) FROM `{self.model.table_name}`"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql()}"
        elif self._row_where:
            sql += f" WHERE {self._row_where}"
        if not count:
            if self._order_by:
                if isinstance(self._order_by, Field):
                    sql += f" ORDER BY `{self._order_by.name}` {'ASC' if self._order_by_asc else 'DESC'}"
                elif isinstance(self._order_by, SQLExpression):
                    sql += f" ORDER BY {self._order_by.sync(self).to_sql()} {'ASC' if self._order_by_asc else 'DESC'}"
            if self._limit:
                sql += f" LIMIT {self._limit}"
            if self._offset:
                assert self._limit, "Offset need limit"
                sql += f" OFFSET {self._offset}"
            if self._for_update:
                sql += " FOR UPDATE"
            elif self._for_share:
                sql += " LOCK IN SHARE MODE"

        return sql + ";"

    def to_delete_sql(self):
        assert self.model
        sql = f"DELETE from `{self.model.table_name}`"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql()}"
        return sql + ";"

    def to_update_sql(self, data: typing.Dict[str, typing.Any]):
        assert self.model
        fields = {f.model_name: f for f in self.model.schema.fields}
        _sqls = []
        for k, v in data.items():
            if isinstance(v, SQLMarker):
                _sqls.append(f"`{k}` = {v.sync(self).to_sql()}")
            else:
                _sqls.append(f"`{k}` = :{self.mark(fields[k].to_database(v))}")
        sql = f"UPDATE `{self.model.table_name}` SET {', '.join(_sqls)}"
        if self._where:
            sql += f" WHERE {self._where.sync(self).to_sql()}"
        return sql + ";"


@dataclasses.dataclass
class Insert(BaseSQLBuilder):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.CREATE
    insert_data: typing.Sequence[typing.Dict[str, str]] = dataclasses.field(
        default_factory=list
    )
    update_fields: typing.Sequence[str] = dataclasses.field(default_factory=list)

    async def exec(self) -> typing.Tuple[int, int]:
        return await self.get_database().execute(self.to_sql(), self._vars)

    def to_sql(self):
        assert self.insert_data

        keys = list(self.insert_data[0].keys())
        vars = []
        for d in self.insert_data:
            _vars = []
            for k in keys:
                _vars.append(self.mark(d[k]))
            vars.append(_vars)
        sql = f"INSERT INTO `{self.model.table_name}` ({', '.join(map(lambda x: f'`{x}`', keys))}) VALUES"
        insert_value_sql = []
        for vs in vars:
            insert_value_sql.append(f"({', '.join(':' + v for v in vs)})")
        update_value_sql = []
        for d in self.update_fields:
            update_value_sql.append(f"{d} = VALUES({d})")
        sql = sql + ", ".join(insert_value_sql)
        if update_value_sql:
            sql += " ON DUPLICATE KEY UPDATE " + ", ".join(update_value_sql)
        return sql + ";"


@dataclasses.dataclass
class CaseUpdate(BaseSQLBuilder):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.UPDATE
    data: typing.List[typing.Dict[str, str]] = dataclasses.field(default_factory=list)

    async def exec(self) -> int:
        database = self.get_database()
        await database.execute(self.to_sql(), self._vars)
        return 1

    def to_sql(self):
        assert self.data
        assert self.model

        parse_data = defaultdict(dict)
        primary_values = []
        for d in self.data:
            for k, v in d.items():
                if k == self.model.schema.primary_field.name:
                    primary_values.append(v)
                else:
                    parse_data[k][d[self.model.schema.primary_field.name]] = v
        sql = f"UPDATE `{self.model.table_name}` SET"
        _sqls = []
        for k, vs in parse_data.items():
            case = SQLCase()
            for pv, v in vs.items():
                case.case(self.model.schema.primary_field == pv, v)
            _sqls.append(f" `{k}` = {case.sync(self).to_sql()}")
        sql += ", ".join(_sqls)
        sql += f" WHERE {(self.model.schema.primary_field.contains(primary_values)).sync(self).to_sql()};"

        return sql
