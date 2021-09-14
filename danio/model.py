"""
Base ORM model with CRUD
"""
from __future__ import annotations

import ast
import contextlib
import dataclasses
import enum
import inspect
import itertools
import random
import re
import typing

from .exception import SchemaException
from .utils import class_property

if typing.TYPE_CHECKING:
    from .database import Database

MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")
SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")
MIGRATION_TV = typing.TypeVar("MIGRATION_TV", bound="Migration")


@dataclasses.dataclass
class Field:
    name: str
    describe: str
    model_name: str  # mapping model filed name
    type: str = ""

    def __post_init__(self):
        if not self.type and self.describe:
            self.type = self.describe.split(" ")[1]

    def __hash__(self):
        return hash((self.name, self.type))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def to_sql(self) -> str:
        return self.describe


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
    FIELD_DESCRIBE_PATTERN: typing.ClassVar[re.Pattern] = re.compile(
        r"\"database: (.*)\""
    )
    FIELD_DBNAME_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"^`([^ ,]*)`")
    DB_FIELD_NAME_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"`([^ ,]*)`")

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
    def _parse(
        cls: typing.Type[SCHEMA_TV], m: typing.Type[Model], schema: SCHEMA_TV
    ) -> SCHEMA_TV:
        schema.name = m.table_name
        schema.abstracted = False
        primary_key = ""
        unique_keys: typing.List[typing.List[str]] = []
        index_keys: typing.List[typing.List[str]] = []
        # get source code
        codes = inspect.getsourcelines(m)[0]
        _indentation_counts = 0
        for c in codes[0]:
            if c in (" ", "\t"):
                _indentation_counts += 1
            else:
                break
        for i, l in enumerate(codes):
            codes[i] = l[_indentation_counts:]
        # parse and get table field and key
        for a in ast.parse("".join(codes)).body[0].body:  # type: ignore
            if isinstance(a, ast.AnnAssign):
                # primary key
                if a.target.id == "__table_primary_key":  # type: ignore
                    primary_key = a.value.id  # type: ignore
                # index
                elif a.target.id in ["__table_unique_keys", "__table_index_keys"]:  # type: ignore
                    if not isinstance(a.value, ast.Tuple):
                        raise SchemaException(
                            f"{m.table_name}: KEYS type should be type.Tuple[type.Tuple[]]"
                        )
                    keys = []
                    for sub in a.value.elts:
                        _keys = []
                        if not isinstance(sub, ast.Tuple):
                            raise SchemaException(
                                f"{m.table_name}: KEYS type should be type.Tuple[type.Tuple[]]"
                            )
                        for e in sub.elts:
                            if isinstance(e, ast.Name):
                                _keys.append(e.id)
                            elif isinstance(e, ast.Constant):
                                _keys.append(e.value)
                            elif isinstance(e, ast.Attribute):
                                _keys.append(e.attr)
                            else:
                                raise SchemaException(
                                    f"{m.table_name} key type not support"
                                )
                        keys.append(_keys)

                    if any(itertools.chain(*keys)):
                        if "unique" in a.target.id:  # type: ignore
                            unique_keys = keys
                        else:
                            index_keys = keys
                # abstract
                elif a.target.id == "__table_abstracted":  # type: ignore
                    if not isinstance(a.value, ast.Constant):
                        raise SchemaException(
                            f"{m.table_name}: __table_abstracted should be constant"
                        )
                    schema.abstracted = bool(a.value.value)
                # field
                else:
                    ans = cls.FIELD_DESCRIBE_PATTERN.findall(
                        "".join(codes[a.lineno - 1 : a.end_lineno])
                    )
                    if ans:
                        try:
                            field_name = a.target.id  # type: ignore
                            describe: str = ans[0]
                            field_db_name = cls.FIELD_DBNAME_PATTERN.findall(describe)[
                                0
                            ]
                            if field_db_name == "{}":
                                field_db_name = field_name
                                describe = describe.replace(
                                    "`{}`", f"`{field_db_name}`"
                                )
                            field = Field(
                                name=field_db_name,
                                model_name=field_name,
                                describe=describe,
                            )
                            schema.fields.add(field)
                        except IndexError as e:
                            raise SchemaException(
                                f"{schema.name}: can't find field db name, line: {ans}"
                            ) from e
                    else:
                        with contextlib.suppress(ValueError):
                            for field in schema.fields:
                                if field.name == a.target.id:  # type: ignore
                                    schema.fields.remove(field)
                                    break

        try:
            model_fields = {f.name: f for f in schema.fields}
            if primary_key:
                schema.primary_field = model_fields[primary_key]
            if index_keys:
                for index in schema.indexes.copy():
                    if not index.unique:
                        schema.indexes.remove(index)
                for _keys in index_keys:
                    schema.indexes.add(
                        Index(fields=[model_fields[key] for key in _keys], unique=False)
                    )
            if unique_keys:
                for index in schema.indexes.copy():
                    if index.unique:
                        schema.indexes.remove(index)
                for _keys in unique_keys:
                    schema.indexes.add(
                        Index(fields=[model_fields[key] for key in _keys], unique=True)
                    )
        except KeyError as e:
            raise SchemaException(f"{schema.name}: missing field") from e

        return schema

    @classmethod
    def from_model(cls: typing.Type[SCHEMA_TV], m: typing.Type[Model]) -> SCHEMA_TV:
        schema = cls(name=m.table_name)

        for _m in m.mro()[::-1]:
            if issubclass(_m, Model):
                schema = cls._parse(_m, schema)  # type: ignore
        schema.model = m

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
                    db_name = cls.DB_FIELD_NAME_PATTERN.findall(line)[0]
                    for f in schema.fields:
                        if db_name == f.name:
                            schema.primary_field = f
                            break
                elif "KEY" in line:
                    fields = {f.name: f for f in schema.fields}
                    index_fileds = []
                    _names = cls.DB_FIELD_NAME_PATTERN.findall(line)
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
                    db_name = cls.DB_FIELD_NAME_PATTERN.findall(line)[0]
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

    def __neg__(self: MIGRATION_TV) -> MIGRATION_TV:
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

    id: int = 0  # "database: `id` int(11) NOT NULL AUTO_INCREMENT"
    # for table schema
    __table_prefix: typing.ClassVar[str] = ""
    __table_primary_key: typing.ClassVar[typing.Any] = id
    __table_index_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = ((),)
    __table_unique_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = ((),)
    __table_abstracted: typing.ClassVar[
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

    def dump(self) -> typing.Dict[str, typing.Any]:
        """Dump dataclass to DB level dict"""
        data = {}
        for f in self.schema.fields:
            data[f.name] = getattr(self, f.model_name)

        return data

    def validate(self):
        pass

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
        await self.before_save()
        data = self.dump()
        data.pop("id")
        if self.id and not force_insert:
            if not database:
                database = self.__class__.get_database(
                    self.Operation.CREATE, self.table_name
                )
            await database.update(self.table_name, (data,), ({"id": self.id},))
        else:
            if not database:
                database = self.__class__.get_database(
                    self.Operation.UPDATE, self.table_name
                )
            if self.id and force_insert:
                data["id"] = self.id
            elif not self.id and force_insert:
                raise ValueError("Force insert with zero id")
            self.id = await database.insert(self.table_name, (data,))
        await self.after_save()

    async def delete(self, database: typing.Optional[Database] = None):
        if not database:
            database = self.__class__.get_database(
                self.Operation.DELETE, self.table_name
            )
        await self.before_delete()
        await database.delete(self.table_name, id=self.id)
        await self.after_delete()

    @classmethod
    def get_table_name(cls) -> str:
        return cls.__table_prefix + cls.__name__.lower()

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
                data[f.model_name] = row[f.name]
            instances.append(cls(**data))
        return instances

    @classmethod
    async def select(
        cls: typing.Type[MODEL_TV],
        limit: typing.Optional[int] = None,
        order_by="id",
        database: typing.Optional[Database] = None,
        **conditions: typing.Any,
    ) -> typing.List[MODEL_TV]:
        if not database:
            database = cls.get_database(cls.Operation.READ, cls.get_table_name())
        return cls.load(
            await database.select(
                cls.get_table_name(),
                [f.name for f in cls.schema.fields],
                limit=limit,
                order_by=order_by,
                **conditions,
            )
        )

    @classmethod
    async def get(
        cls: typing.Type[MODEL_TV],
        database: typing.Optional[Database] = None,
        **conditions: typing.Any,
    ) -> typing.Optional[MODEL_TV]:
        instances = await cls.select(database=database, **conditions)
        return instances[0] if instances else None

    @classmethod
    async def count(
        cls: typing.Type[MODEL_TV],
        database: typing.Optional[Database] = None,
        **conditions: typing.Any,
    ) -> int:
        if not database:
            database = cls.get_database(cls.Operation.READ, cls.get_table_name())

        return (
            await database.select(cls.get_table_name(), ("COUNT(*)",), **conditions)
        )[0][0]

    @classmethod
    async def bulk_create(
        cls: typing.Type[MODEL_TV],
        instances: typing.Iterator[MODEL_TV],
        database: typing.Optional[Database] = None,
    ) -> typing.Iterator[MODEL_TV]:
        if not database:
            database = cls.get_database(cls.Operation.CREATE, cls.get_table_name())
        for ins in instances:
            await ins.before_save()

        data = []
        for ins in instances:
            data.append(ins.dump())
            if not ins.id:
                data[-1].pop("id")

        fist_id = await database.insert(cls.get_table_name(), data)
        for i, ins in enumerate(instances):
            if not ins.id:
                ins.id = fist_id + i

        for ins in instances:
            await ins.after_save()
        return instances

    @classmethod
    async def bulk_update(
        cls: typing.Type[MODEL_TV],
        instances: typing.Iterator[MODEL_TV],
        database: typing.Optional[Database] = None,
        validate=True,
    ):
        if not database:
            database = cls.get_database(cls.Operation.CREATE, cls.get_table_name())
        for ins in instances:
            await ins.before_save()

        data = []
        for i in instances:
            if not i.id:
                raise ValueError("Update with empty ID")
            data.append(i.dump())

        await database.update(
            cls.get_table_name(), data, [{"id": i.id} for i in instances]
        )

        for ins in instances:
            await ins.after_save()
