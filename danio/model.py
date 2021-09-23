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
from datetime import date, datetime, timedelta
from functools import reduce

from .exception import SchemaException, ValidateException
from .utils import class_property

if typing.TYPE_CHECKING:
    from .database import Database


MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")
SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")
MIGRATION_TV = typing.TypeVar("MIGRATION_TV", bound="Migration")
SQL_BUILDER_TV = typing.TypeVar("SQL_BUILDER_TV", bound="SQLBuilder")


@dataclasses.dataclass
class Field:
    class FieldDefault:
        pass

    COMMENT_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"COMMENT '(.*)'")
    NAME_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"^`([^ ,]*)`")
    TYPE: typing.ClassVar[str] = ""
    ESCAPE_TABLE: typing.ClassVar[typing.List[str]] = [chr(x) for x in range(128)]
    ESCAPE_TABLE[0] = "\\0"
    ESCAPE_TABLE[ord("\\")] = "\\\\"
    ESCAPE_TABLE[ord("\n")] = "\\n"
    ESCAPE_TABLE[ord("\r")] = "\\r"
    ESCAPE_TABLE[ord("\032")] = "\\Z"
    ESCAPE_TABLE[ord('"')] = '\\"'
    ESCAPE_TABLE[ord("'")] = "\\'"

    name: str = ""
    model_name: str = ""
    default: typing.Any = None  # for model layer
    describe: str = ""
    type: str = ""
    auto_increment: bool = False
    comment: str = ""
    enum: typing.Optional[typing.Type[enum.Enum]] = None

    def __post_init__(self):
        # from schema sql
        if self.describe:
            self.auto_incrment = "AUTO_INCREMENT" in self.describe
            self.type = self.describe.split(" ")[1]
            tmp = self.COMMENT_PATTERN.findall(self.describe)
            if tmp:
                self.comment = tmp[0]
            if not self.name:
                tmp = self.NAME_PATTERN.findall(self.describe)
                if tmp:
                    self.name = tmp[0]
        # from model field
        if not self.describe and not self.type and self.TYPE:
            self.type = self.TYPE
        if not self.describe and self.name and self.type:
            self.describe = f"`{self.name}` {self.type} NOT NULL {'AUTO_INCREMENT ' if self.auto_increment else ' '}COMMENT '{self.comment}'"
        # model spec
        if self.enum and not isinstance(self.default, self.enum):
            self.default = tuple(self.enum)[0]

    def __hash__(self):
        return hash((self.name, self.type))

    def __eq__(self, other: object) -> typing.Union[bool, Condition]:  # type: ignore
        if isinstance(other, Field):
            return self.__hash__() == other.__hash__()
        else:
            return Condition(field=self, value=other, operetor=Condition.Operator.EQ)

    def __gt__(self, other: object) -> Condition:
        return Condition(field=self, value=other, operetor=Condition.Operator.GT)

    def __lt__(self, other: object) -> Condition:
        return Condition(field=self, value=other, operetor=Condition.Operator.LT)

    def __ge__(self, other: object) -> Condition:
        return Condition(field=self, value=other, operetor=Condition.Operator.GE)

    def __le__(self, other: object) -> Condition:
        return Condition(field=self, value=other, operetor=Condition.Operator.LE)

    def __ne__(self, other: object) -> Condition:  # type: ignore
        return Condition(field=self, value=other, operetor=Condition.Operator.NE)

    def contains(self, values: typing.Iterable) -> Condition:
        return Condition(field=self, value=values, operetor=Condition.Operator.IN)

    def to_sql(self) -> str:
        if not self.describe:
            raise ValueError("Need name type or describe")

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
    TYPE: typing.ClassVar[str] = "smallint(3)"


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

    def like(self, value: typing.Any) -> Condition:
        return Condition(field=self, value=value, operetor=Condition.Operator.LK)


@dataclasses.dataclass(eq=False)
class TextField(CharField):
    TYPE: typing.ClassVar[str] = "text"

    default: str = ""


@dataclasses.dataclass(eq=False)
class ComplexField(Field):
    def to_database(self, value: typing.Any) -> str:
        if not isinstance(value, self.default.__class__):
            raise ValueError(
                f"{self.__class__.__name__} with wrong type: {type(value)}"
            )

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
        return f"{json.dumps(value)}"


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
    if default is not Field.FieldDefault:
        extras["default"] = default
    if "default" in extras:
        extras["default"] = copy.copy(extras["default"])

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
        if not isinstance(other, Index):
            raise NotImplementedError()
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
    primary_field: typing.Optional[Field] = None
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
        if not isinstance(other, Schema):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def __sub__(self, other: object) -> Migration:
        if other is None:
            return Migration(schema=self, old_schema=None)

        elif not isinstance(other, Schema):
            raise NotImplementedError()
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
        if not self.primary_field:
            raise SchemaException("No primary field!")

        keys = [f"PRIMARY KEY (`{self.primary_field.name}`)"]
        keys.extend([index.to_sql() for index in self.indexes])

        return (
            f"CREATE TABLE `{self.name}` (\n"
            + ",\n".join(itertools.chain((v.to_sql() for v in self.fields), keys))
            + f"\n) {self.POSTFIX}"
        )

    @classmethod
    def from_model(cls: typing.Type[SCHEMA_TV], m: typing.Type[Model]) -> SCHEMA_TV:
        schema = cls(name=m.table_name)
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
            else:
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
            sqls.append(f"DROP TABLE {self.old_schema.name}")
        elif self.schema and self.old_schema:
            if self.old_schema.name != self.schema.name:
                sqls.append(
                    f"ALTER TABLE {self.old_schema.name} RENAME {self.schema.name}"
                )
            for f in self.add_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} ADD COLUMN {f.to_sql()}")
            for f in self.drop_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} DROP COLUMN {f.name}")
            for f in self.change_type_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} MODIFY {f.name} {f.type}")
            for i in self.add_indexes:
                sqls.append(
                    f"CREATE {'UNIQUE' if i.unique else ''} INDEX {i.name} on {self.schema.name} ({','.join('`' + f.name + '`' for f in i.fields)})"
                )
            for i in self.drop_indexes:
                if not set(i.fields) & set(self.drop_fields):
                    sqls.append(f"ALTER TABLE {self.schema.name} DROP INDEX {i.name}")
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
                setattr(self, f.model_name, f.default)

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

    def dump(self) -> typing.Dict[str, typing.Any]:
        """Dump dataclass to DB level dict"""
        data = {}
        for f in self.schema.fields:
            data[f.name] = f.to_database(getattr(self, f.model_name))

        return data

    def validate(self):
        # choices
        for f in self.schema.fields:
            if not f.enum:
                continue
            value = getattr(self, f.model_name)
            if value not in set((c for c in f.enum)):
                raise ValidateException(
                    f"{self.__class__.__name__}.{f.model_name} value: {value} not in choices: {f.enum}"
                )

    async def before_save(self):
        self.validate()

    async def after_save(self):
        pass

    async def before_delete(self):
        pass

    async def after_delete(self):
        pass

    async def save(
        self,
        database: typing.Optional[Database] = None,
        force_insert=False,
    ):
        assert self.schema.primary_field

        await self.before_save()
        data = self.dump()
        dumped_primary_value = data.pop(self.schema.primary_field.name)
        if self.primary and not force_insert:
            await Update(self.__class__, data=data).where(
                self.schema.primary_field == self.primary  # type: ignore
            ).exec()
        else:
            if self.primary and force_insert:
                data[self.schema.primary_field.name] = dumped_primary_value
            elif not self.primary and force_insert:
                raise ValueError("Force insert with zero id")
            setattr(
                self,
                self.schema.primary_field.model_name,
                await Insert(self.__class__, database=database, data=[data]).exec(),
            )
        await self.after_save()

    async def delete(
        self,
        *conditions: typing.Union[Condition, ConditionGroup],
        database: typing.Optional[Database] = None,
    ) -> bool:
        await self.before_delete()
        deleted = (
            await self.__class__._delete(database=database).where(*conditions).exec()
        )
        await self.after_delete()
        return deleted

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
    def _select(
        cls,
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = tuple(),
    ) -> Select:
        return Select(cls, fields=fields, database=database)

    @classmethod
    def _delete(
        cls,
        database: typing.Optional[Database] = None,
    ) -> Delete:
        return Delete(cls, database=database)

    @classmethod
    async def select(
        cls: typing.Type[MODEL_TV],
        *conditions: typing.Union[Condition, ConditionGroup],
        fields: typing.Sequence[Field] = tuple(),
        order_by: typing.Optional[Field] = None,
        order_by_asc=False,
        database: typing.Optional[Database] = None,
        for_update=False,
        for_share=False,
        limit=0,
        offset=0,
    ) -> typing.List[MODEL_TV]:
        return (
            await Select(  # type: ignore
                cls,
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
        *conditions: typing.Union[Condition, ConditionGroup],
        database: typing.Optional[Database] = None,
        fields: typing.Sequence[Field] = tuple(),
        order_by: typing.Optional[Field] = None,
        for_update=False,
        for_share=False,
        offset=0,
    ) -> typing.Optional[MODEL_TV]:
        return (
            await Select(  # type: ignore
                cls,
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
        *conditions: typing.Union[Condition, ConditionGroup],
        database: typing.Optional[Database] = None,
    ) -> int:
        return await Select(cls, database=database).where(*conditions).fetch_count()

    @classmethod
    async def update(
        cls: typing.Type[MODEL_TV],
        *conditions: typing.Union[Condition, ConditionGroup],
        database: typing.Optional[Database] = None,
        **data: str,
    ) -> int:
        return await Update(cls, database=database, data=data).where(*conditions).exec()

    @classmethod
    async def bulk_create(
        cls: typing.Type[MODEL_TV],
        instances: typing.Iterator[MODEL_TV],
        database: typing.Optional[Database] = None,
    ) -> typing.Iterator[MODEL_TV]:
        assert cls.schema.primary_field
        for ins in instances:
            await ins.before_save()

        data = []
        for ins in instances:
            data.append(ins.dump())

        first_id = await Insert(cls, database=database, data=data).exec()
        for i, ins in enumerate(instances):
            if not ins.id:
                setattr(ins, cls.schema.primary_field.model_name, first_id + i)

        for ins in instances:
            await ins.after_save()
        return instances


# query builder
@dataclasses.dataclass
class Condition:
    class Operator(enum.Enum):
        EQ = "="
        NE = "!="
        GT = ">"
        GE = ">="
        LT = "<"
        LE = "<="
        LK = "LIKE"
        IN = "IN"

    field: Field
    value: typing.Any
    operetor: Operator
    _var_index: int = 0
    _vars: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    def __and__(self, other: typing.Union[Condition, ConditionGroup]) -> ConditionGroup:
        if isinstance(other, (Condition, ConditionGroup)):
            return ConditionGroup(conditions=[self, other], is_and=True)
        else:
            raise NotImplementedError()

    def __or__(self, other: typing.Union[Condition, ConditionGroup]) -> ConditionGroup:
        if isinstance(other, (Condition, ConditionGroup)):
            return ConditionGroup(conditions=[self, other], is_and=False)
        else:
            raise NotImplementedError()

    def mark(self, value: typing.Any) -> str:
        k = f"where{self._var_index}"
        self._vars[k] = value.value if isinstance(value, enum.Enum) else value
        self._var_index += 1
        return k

    def to_sql(self) -> str:
        if self.operetor == self.Operator.IN:
            return f"`{self.field.name}` {self.operetor.value} ({', '.join(':' + self.mark(v) for v in self.value)})"
        else:
            return f"`{self.field.name}` {self.operetor.value} :{self.mark(self.value)}"


@dataclasses.dataclass
class ConditionGroup:
    conditions: typing.List[typing.Union[Condition, ConditionGroup]]
    is_and: bool = False
    _var_index: int = 0
    _vars: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    def __and__(self, other: typing.Union[Condition, ConditionGroup]) -> ConditionGroup:
        if isinstance(other, (Condition, ConditionGroup)):
            return ConditionGroup(conditions=[self, other], is_and=True)
        else:
            raise NotImplementedError()

    def __or__(self, other: typing.Union[Condition, ConditionGroup]) -> ConditionGroup:
        if isinstance(other, (Condition, ConditionGroup)):
            return ConditionGroup(conditions=[self, other], is_and=False)
        else:
            raise NotImplementedError()

    def to_sql(self) -> str:
        operator = " AND " if self.is_and else " OR "
        sqls = []
        for cg in self.conditions:
            cg._var_index = self._var_index
            sqls.append(f"({cg.to_sql()})")
            self._var_index = cg._var_index
            self._vars.update(cg._vars)
        return operator.join(sqls)


@dataclasses.dataclass
class SQLBuilder:
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.READ

    model: typing.Type[Model]
    _where: typing.Optional[typing.Union[ConditionGroup, Condition]] = None
    database: typing.Optional[Database] = None
    _var_index: int = 0
    _vars: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if not self.database:
            self.database = self.model.get_database(
                self.OPERATION, self.model.table_name
            )

    def mark(self, value: typing.Any) -> str:
        k = f"var{self._var_index}"
        self._vars[k] = value
        self._var_index += 1
        return k

    def where(
        self: SQL_BUILDER_TV,
        *conditions: typing.Union[Condition, ConditionGroup],
        is_and=True,
    ) -> SQL_BUILDER_TV:
        if conditions:
            _where = reduce(lambda c1, c2: c1 & c2 if is_and else c1 | c2, conditions)
            if self._where:
                self._where = self._where & _where
            else:
                self._where = _where
        return self


@dataclasses.dataclass
class Select(SQLBuilder):
    fields: typing.Sequence[Field] = tuple()
    _limit: int = 0
    _offset: int = 0
    _order_by: typing.Optional[Field] = None
    _order_by_asc: bool = False
    _for_update: bool = False
    _for_share: bool = False

    async def fetch_all(self) -> typing.List[Model]:
        assert self.database
        return self.model.load(await self.database.fetch_all(self.to_sql(), self._vars))

    async def fetch_one(self) -> typing.Optional[Model]:
        assert self.database
        data = await self.database.fetch_one(self.to_sql(), self._vars)
        if data:
            return self.model.load([data])[0]
        else:
            return None

    async def fetch_count(self) -> int:
        assert self.database
        data = await self.database.fetch_one(self.to_sql(count=True), self._vars)
        if data:
            return data[0]
        else:
            return 0

    def limit(self, n: int) -> Select:
        self._limit = int(n)
        return self

    def offset(self, n: int) -> Select:
        self._offset = int(n)
        return self

    def order_by(self, f: Field, asc=True) -> Select:
        self._order_by = f
        self._order_by_asc = asc
        return self

    def for_update(self) -> Select:
        self._for_update = True
        return self

    def for_share(self) -> Select:
        self._for_share = True
        return self

    def to_sql(self, count=False) -> str:
        if not count:
            sql = f"SELECT {', '.join(f'`{f.name}`' for f in self.fields or self.model.schema.fields)} FROM `{self.model.table_name}`"
        else:
            sql = f"SELECT COUNT(*) FROM {self.model.table_name}"
        if self._where:
            sql += f" WHERE {self._where.to_sql()}"
            self._vars.update(self._where._vars)
        if not count:
            if self._order_by:
                sql += f" ORDER BY `{self._order_by.name}` {'ASC' if self._order_by_asc else 'DESC'}"
            if self._limit:
                sql += f" LIMIT {self._limit}"
            if self._offset:
                sql += f" OFFSET {self._offset}"
            if self._for_update:
                sql += " FOR UPDATE"
            if self._for_share:
                sql += " LOCK IN SHARE MODE"

        return sql + ";"


@dataclasses.dataclass
class Delete(SQLBuilder):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.DELETE

    async def exec(self) -> bool:
        assert self.database
        return bool(await self.database.execute(self.to_sql(), self._vars))

    def to_sql(self):
        sql = f"DELETE from `{self.model.table_name}`"
        if self._where:
            sql += f" WHERE {self._where.to_sql()}"
            self._vars.update(self._where._vars)
        return sql + ";"


@dataclasses.dataclass
class Insert(SQLBuilder):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.CREATE
    data: typing.List[typing.Dict[str, str]] = dataclasses.field(default_factory=list)

    async def exec(self) -> int:
        assert self.database
        return await self.database.execute(self.to_sql(), self._vars)

    def to_sql(self):
        assert self.data

        keys = list(self.data[0].keys())
        vars = []
        for d in self.data:
            _vars = []
            for k in keys:
                _vars.append(self.mark(d[k]))
            vars.append(_vars)
        sql = f"INSERT INTO `{self.model.table_name}` ({', '.join(map(lambda x: f'`{x}`', keys))}) VALUES"
        value_sql = []
        for vs in vars:
            value_sql.append(f"({', '.join(':' + v for v in vs)})")
        return sql + ", ".join(value_sql) + ";"


@dataclasses.dataclass
class Update(SQLBuilder):
    OPERATION: typing.ClassVar[Model.Operation] = Model.Operation.UPDATE
    data: typing.Dict[str, str] = dataclasses.field(default=dict)  # type: ignore

    async def exec(self) -> int:
        assert self.database
        return await self.database.execute(self.to_sql(), self._vars)

    def to_sql(self):
        assert self.data
        sql = f"UPDATE `{self.model.table_name}` SET {', '.join([f'`{k}` = :{self.mark(v)}' for k, v in self.data.items()])}"
        if self._where:
            sql += f" WHERE {self._where.to_sql()}"
            self._vars.update(self._where._vars)
        return sql + ";"
