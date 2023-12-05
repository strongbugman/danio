"""
Base ORM model with CRUD
"""
from __future__ import annotations

import dataclasses
import enum
import logging
import re
import typing
import warnings
from contextvars import ContextVar

from databases.interfaces import Record

from . import exception, schema
from .database import Database
from .schema import (Field, Index, IntField, Operation, RelationField, Schema,
                     SQLExpression)
from .utils import class_property

MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")
SQLCHAIN_TV = typing.TypeVar("SQLCHAIN_TV", bound="SqlChain")


@typing.dataclass_transform()
def model(cls: typing.Type[MODEL_TV]) -> typing.Type[MODEL_TV]:
    cls = dataclasses.dataclass(cls)
    cls.schema = cls.get_schema()
    return cls


@model
class Model:
    DATABASE: typing.ClassVar[ContextVar[typing.Optional[Database]]] = ContextVar(
        "database"
    )
    schema: typing.ClassVar[Schema]

    ID: typing.ClassVar[Field]
    id: typing.Annotated[int, IntField(primary=True, auto_increment=True)] = 0
    # for table schema
    _table_prefix: typing.ClassVar[str] = ""
    _table_name_prefix: typing.ClassVar[str] = ""
    _table_name_snake_case: typing.ClassVar[bool] = True
    _table_index_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = tuple()
    _table_unique_keys: typing.ClassVar[
        typing.Tuple[typing.Tuple[typing.Any, ...], ...]
    ] = tuple()
    _table_abstracted: typing.ClassVar[
        bool
    ] = True  # do not impact subclass, default false for every child class except defined as true

    @class_property
    @classmethod
    def table_name(cls) -> str:
        return cls.get_table_name()

    @class_property
    @classmethod
    def table_index_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return cls.get_table_index_keys()

    @class_property
    @classmethod
    def table_unique_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return cls.get_table_unique_keys()

    @class_property
    @classmethod
    def table_abstracted(cls) -> bool:
        return cls.__dict__.get("_table_abstracted", False)

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
        await self.load_relations(auto=True)

    async def before_create(self, validate: bool = True):
        await self.before_save()
        if validate:
            await self.validate()

    async def after_create(self):
        await self.after_save()

    async def before_update(self, validate: bool = True):
        await self.before_save()
        if validate:
            await self.validate()

    async def after_update(self):
        await self.after_save()

    async def before_save(self):
        pass

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

    async def load_relations(self, auto=False):
        for relation_field in self.schema.relation_fields:
            if auto and not relation_field.auto:
                continue
            setattr(self, relation_field.model_name, await relation_field.load(self))

    def dump(
        self,
        fields: typing.Iterable[Field] = (),
        ignore_fields: typing.Iterable[Field] = (),
    ) -> typing.Dict[str, typing.Any]:
        """Dump model to dict with only database fields"""
        data = {}
        _ignore_fields = {f.name for f in ignore_fields}
        for f in fields or self.schema.fields:
            if f.name in _ignore_fields:
                continue
            data[f.name] = getattr(self, f.model_name)

        return data

    async def create(
        self,
        database: typing.Optional[Database] = None,
        fields: typing.Iterable[Field] = (),
        ignore_fields: typing.Iterable[Field] = (),
        validate: bool = True,
    ):
        database = (
            database if database else self.__class__.get_database(Operation.CREATE)
        )
        data = self.dump(fields=fields, ignore_fields=ignore_fields)
        if (
            self.schema.primary_field.name in data
            and not data[self.schema.primary_field.name]
        ):
            data.pop(self.schema.primary_field.name)
        await self.before_create(validate=validate)

        builder = schema.Insert(
            insert_data=[data],
            schema=self.__class__.schema,
        )
        last_id = (
            await database.execute(builder.to_sql(database.type), builder._vars)
        )[0]
        setattr(self, self.schema.primary_field.model_name, last_id)
        await self.after_create()
        return self

    async def update(
        self,
        database: typing.Optional[Database] = None,
        fields: typing.Iterable[Field] = (),
        ignore_fields: typing.Iterable[Field] = (),
        validate: bool = True,
    ) -> bool:
        """
        For PostgreSQL/SQLite, always return True
        """
        assert self.primary
        await self.before_update(validate=validate)
        data = self.dump(fields=fields, ignore_fields=ignore_fields)
        rowcount = await self.__class__.where(
            self.schema.primary_field == self.primary,
            database=database,
        ).update(**data)
        await self.after_update()
        return rowcount > 0

    async def save(
        self: MODEL_TV,
        database: typing.Optional[Database] = None,
        fields: typing.Iterable[Field] = (),
        ignore_fields: typing.Iterable[Field] = (),
        force_insert=False,
        validate: bool = True,
    ) -> MODEL_TV:
        if self.primary and not force_insert:
            await self.update(
                database=database,
                fields=fields,
                ignore_fields=ignore_fields,
                validate=validate,
            )
        else:
            await self.create(
                database=database,
                fields=fields,
                ignore_fields=ignore_fields,
                validate=validate,
            )
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
        fields: typing.Iterable[Field] = tuple(),
    ) -> MODEL_TV:
        new = await self.__class__.where(
            self.schema.primary_field == self.primary, database=database
        ).fetch_one(fields=fields)
        for f in dataclasses.fields(self):
            setattr(self, f.name, getattr(new, f.name))
        return self

    async def get_or_create(
        self: MODEL_TV,
        key_fields: typing.Iterable[Field],
        database: typing.Optional[Database] = None,
        fields: typing.Iterable[Field] = (),
        validate: bool = True,
        for_update: bool = False,
    ) -> typing.Tuple[MODEL_TV, bool]:
        if not database:
            # using write db by default
            database = self.__class__.get_database(Operation.CREATE)
        conditions = []
        created = False
        for f in key_fields:
            conditions.append(f == getattr(self, f.model_name))
        where = self.__class__.where(*conditions, database=database)
        if for_update:
            where = where.for_update()
        ins = await where.fetch_one(fields=fields)
        if not ins:
            try:
                ins = await self.create(
                    database=database, fields=fields, validate=validate
                )
                created = True
            except exception.IntegrityError as e:
                where = self.__class__.where(*conditions, database=database)
                if for_update:
                    where = where.for_update()
                ins = await where.fetch_one(fields=fields)
                if not ins:
                    raise e
        assert ins
        return ins, created

    async def create_or_update(
        self: MODEL_TV,
        key_fields: typing.Iterable[Field],
        database: typing.Optional[Database] = None,
        fields: typing.Iterable[Field] = (),
        update_fields: typing.Iterable[Field] = (),
        for_update: bool = True,
        validate: bool = True,
    ) -> typing.Tuple[MODEL_TV, bool, bool]:
        """Try get_or_create then update.

        For SQLite, updated will always be True.
        """
        if not database:
            database = self.__class__.get_database(Operation.CREATE)
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
        prefix = cls._table_name_prefix or cls._table_prefix
        if cls._table_name_snake_case:
            return prefix + re.sub(r"(?P<n>[A-Z])", r"_\g<n>", cls.__name__).lower()[1:]
        else:
            return prefix + cls.__name__.lower()

    @classmethod
    def get_table_index_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return cls._table_index_keys

    @classmethod
    def get_table_unique_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return cls._table_unique_keys

    @classmethod
    def get_database(cls, operation: Operation, *args, **kwargs) -> Database:
        """Get database instance, route database by operation"""
        db = cls.DATABASE.get()
        if not db:
            raise RuntimeError("No database provide")
        return db

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
        raw="",
        is_and=True,
    ) -> SqlChain[MODEL_TV]:
        return SqlChain(model=cls, database=database, schema=cls.schema).where(
            *conditions, is_and=is_and, raw=raw
        )

    @classmethod
    async def upsert(
        cls,
        insert_data: typing.List[typing.Dict[str, typing.Any]],
        database: typing.Optional[Database] = None,
        update_fields: typing.Iterable[str] = (),
        conflict_targets: typing.Iterable[str] = (),
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
        database = database if database else cls.get_database(Operation.CREATE)
        builder = schema.Insert(
            insert_data=insert_data,
            schema=cls.schema,
            update_fields=update_fields,
            conflict_targets=conflict_targets,
        )
        last_id, rowcount = await database.execute(
            builder.to_sql(database.type), builder._vars
        )
        if database.type == Database.Type.MYSQL:
            return rowcount == 1, rowcount == 2
        else:
            return last_id != 0, rowcount != 0

    @classmethod
    async def bulk_create(
        cls: typing.Type[MODEL_TV],
        instances: typing.Sequence[MODEL_TV],
        fields: typing.Iterable[Field] = (),
        database: typing.Optional[Database] = None,
        validate: bool = True,
    ) -> typing.Sequence[MODEL_TV]:
        assert cls.schema.primary_field
        if not database:
            database = cls.get_database(Operation.CREATE)
        if (
            database.type != database.type.POSTGRES
            and not cls.schema.primary_field.auto_increment
        ):
            raise exception.OperationException(
                f"{cls}'s primary_field must be auto incremented!"
            )
        if (
            database.type == database.type.POSTGRES
            and "serial" not in cls.schema.primary_field.type
        ):
            raise exception.OperationException(
                f"{cls}'s primary_field must be auto incremented!"
            )

        for ins in instances:
            await ins.before_create(validate=validate)
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

        builder = schema.Insert(insert_data=data, schema=cls.schema)
        next_ins_id = (
            await database.execute(builder.to_sql(database.type), builder._vars)
        )[0]

        if database.type == database.type.MYSQL:
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
        instances: typing.Iterable[MODEL_TV],
        fields: typing.Iterable[Field] = (),
        database: typing.Optional[Database] = None,
        validate: bool = True,
    ) -> typing.Iterable[MODEL_TV]:
        assert cls.schema.primary_field

        database = database if database else cls.get_database(Operation.UPDATE)
        for ins in instances:
            await ins.before_update(validate=validate)

        data = []
        for ins in instances:
            assert ins.primary, "Need primary"
            data.append(ins.dump(fields=fields))
            data[-1][cls.schema.primary_field.name] = ins.primary

        builder = schema.CaseUpdate(data=data, schema=cls.schema)
        await database.execute(builder.to_sql(database.type), builder._vars)

        for ins in instances:
            await ins.after_update()
        return instances

    @classmethod
    async def bulk_delete(
        cls,
        instances: typing.Iterable[MODEL_TV],
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

    @classmethod
    def get_schema(cls) -> Schema:
        schema = Schema(name=cls.table_name)
        schema.abstracted = cls.table_abstracted
        # fields
        for f in dataclasses.fields(cls):
            if isinstance(f.default, Field):  # from dataclass default
                f.default.model_name = f.name
                if not f.default.name:
                    f.default.name = f.name
                    f.default.__post_init__()
                schema.fields.append(f.default)
            if hint := typing.get_type_hints(cls, include_extras=True).get(
                f.name
            ):  # from Annotated
                for meta in getattr(hint, "__metadata__", ()):
                    if type(meta) is type and issubclass(meta, Field):
                        meta = meta()
                    if isinstance(meta, Field):
                        meta.model_name = f.name
                        if not meta.name:
                            meta.name = f.name
                            meta.__post_init__()
                        meta.default = f.default
                        schema.fields.append(meta)
                        setattr(cls, f.name, meta)
                        setattr(cls, f.name.upper(), meta)
                    elif isinstance(meta, RelationField):
                        meta.set_model_name(f.name)
                        schema.relation_fields.append(meta)
                        setattr(cls, f.name, meta)
                        setattr(cls, f.name.upper(), meta)
        fields = {f.model_name: f for f in schema.fields}
        # index
        for i, index_keys in enumerate((cls.table_index_keys, cls.table_unique_keys)):
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
                    schema.indexes.append(Index(fields=_fields, unique=i == 1))
        return schema

    @classmethod
    async def get_db_schema(cls, database: Database) -> typing.Optional[Schema]:
        schema = Schema(name=cls.table_name if cls else "table")
        model_names = {f.name: f.model_name for f in cls.schema.fields} if cls else {}
        if database.type == database.type.MYSQL:
            field_name_pattern = re.compile(r"`([^ ,]*)`")
            try:
                for line in (
                    await database.fetch_all(f"SHOW CREATE TABLE `{cls.table_name}`")
                )[0][1].split("\n")[1:-1]:
                    if "PRIMARY KEY" in line:
                        db_name = field_name_pattern.findall(line)[0]
                        for f in schema.fields:
                            if db_name == f.name:
                                f.primary = True
                                break
                    elif "FOREIGN KEY" in line:
                        logging.warning("FOREIGN KEY not support!")
                    elif "KEY" in line:
                        fields = {f.name: f for f in schema.fields}
                        index_fields = []
                        _names = field_name_pattern.findall(line)
                        index_name = _names[0]
                        index_fields = [fields[n] for n in _names[1:]]
                        schema.indexes.append(
                            Index(
                                fields=index_fields,
                                unique="UNIQUE" in line,
                                name=index_name,
                            )
                        )
                    else:
                        db_name = field_name_pattern.findall(line)[0]
                        name = model_names.get(db_name, "")
                        field_type = line.split("`")[-1].split(" ")[1]
                        schema.fields.append(
                            Schema.detect_field_type(database, field_type)(
                                name=db_name,
                                type=field_type,
                                model_name=name,
                                auto_increment="AUTO_INCREMENT" in line,
                                not_null="NOT NULL" in line,
                            )
                        )
            except Exception as e:
                if "doesn't exist" in str(e):
                    return None
                raise e
        elif database.type == database.type.SQLITE:
            field_name_pattern = re.compile(r"`([^ ,]*)`")
            for r in await database.fetch_all(
                f"SELECT * FROM sqlite_schema WHERE tbl_name = '{cls.table_name}';"
            ):
                if r[0] == "table":
                    for line in r[4].split("\n")[1:]:
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
                            field_type = line.split("`")[-1].split(" ")[1]
                            schema.fields.append(
                                Schema.detect_field_type(database, field_type)(
                                    name=db_name,
                                    type=field_type,
                                    model_name=name,
                                    primary=primary,
                                    auto_increment=auto_increment,
                                )
                            )
                elif r[0] == "index":
                    fields = {f.name: f for f in schema.fields}
                    _names = field_name_pattern.findall(r[4])
                    index_fields = [fields[n] for n in _names[2:]]
                    schema.indexes.append(
                        Index(
                            fields=index_fields,
                            unique="UNIQUE" in r[4],
                            name=r[1],
                        )
                    )
        else:
            for r in await database.fetch_all(
                f"SELECT * FROM information_schema.columns WHERE table_name = '{cls.table_name}';"
            ):
                d = dict(r)
                field_type = d["data_type"]
                if field_type == "character varying":
                    field_type = f"varchar({d['character_maximum_length']})"
                elif field_type == "character":
                    field_type = f"char({d['character_maximum_length']})"
                else:
                    field_type = field_type
                schema.fields.append(
                    Schema.detect_field_type(database, field_type)(
                        name=d["column_name"],
                        model_name=model_names.get(d["column_name"], ""),
                        type=field_type,
                    )
                )
            for r in await database.fetch_all(
                f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{cls.table_name}';"
            ):
                d = dict(r)
                fields = {f.name: f for f in schema.fields}
                _names = d["indexdef"].split("(")[-1].split(")")[0].split(", ")
                index_fields = [fields[n] for n in _names]
                if d["indexname"].endswith("_pkey"):
                    primary_field = index_fields[0]
                    primary_field.primary = True
                else:
                    schema.indexes.append(
                        Index(
                            fields=index_fields,
                            unique="UNIQUE" in d["indexdef"],
                            name=d["indexname"],
                        )
                    )

        return schema if schema.fields else None


@dataclasses.dataclass
class SqlChain(schema.Crud, typing.Generic[MODEL_TV]):
    model: typing.Optional[typing.Type[MODEL_TV]] = None
    database: typing.Optional[Database] = None

    async def fetch_all(
        self,
        fields: typing.Iterable[Field] = tuple(),
        ignore_fields: typing.Iterable[Field] = tuple(),
    ) -> typing.List[MODEL_TV]:
        assert self.model
        self.database = (
            self.database if self.database else self.model.get_database(Operation.READ)
        )

        instances = self.model.load(
            [
                dict(r)
                for r in await self.database.fetch_all(
                    self.to_select_sql(
                        type=self.database.type,
                        fields=fields,
                        ignore_fields=ignore_fields,
                    ),
                    self._vars,
                )
            ]
        )
        for ins in instances:
            await ins.after_read()

        return instances

    async def fetch_one(
        self,
        fields: typing.Iterable[Field] = tuple(),
        ignore_fields: typing.Iterable[Field] = tuple(),
    ) -> typing.Optional[MODEL_TV]:
        assert self.model
        self.database = (
            self.database if self.database else self.model.get_database(Operation.READ)
        )

        data = await self.database.fetch_one(
            self.to_select_sql(
                type=self.database.type, fields=fields, ignore_fields=ignore_fields
            ),
            self._vars,
        )
        if data:
            ins = self.model.load([dict(data)])[0]
            await ins.after_read()
            return ins
        else:
            return None

    async def must_fetch_one(
        self,
        fields: typing.Iterable[Field] = tuple(),
        ignore_fields: typing.Iterable[Field] = tuple(),
    ) -> MODEL_TV:
        instance = await self.fetch_one(fields=fields, ignore_fields=ignore_fields)
        if instance is None:
            raise exception.NotFound(f"{self.model} {self.to_select_sql()}")
        else:
            return instance

    async def fetch_row(
        self,
        fields: typing.Iterable[Field] = tuple(),
        ignore_fields: typing.Iterable[Field] = tuple(),
    ) -> typing.List[Record]:
        assert self.model
        self.database = (
            self.database if self.database else self.model.get_database(Operation.READ)
        )

        return await self.database.fetch_all(
            self.to_select_sql(
                type=self.database.type, fields=fields, ignore_fields=ignore_fields
            ),
            self._vars,
        )

    async def fetch_count(self) -> int:
        assert self.model
        self.database = (
            self.database if self.database else self.model.get_database(Operation.READ)
        )

        data = await self.database.fetch_one(
            self.to_select_sql(count=True, type=self.database.type), self._vars
        )
        assert data
        return data[0]

    async def delete(self) -> int:
        assert self.model
        self.database = (
            self.database
            if self.database
            else self.model.get_database(Operation.DELETE)
        )

        return (
            await self.database.execute(
                self.to_delete_sql(type=self.database.type), self._vars
            )
        )[1]

    async def update(self, **data) -> int:
        assert self.model
        self.database = (
            self.database
            if self.database
            else self.model.get_database(Operation.UPDATE)
        )

        return (
            await self.database.execute(
                self.to_update_sql(data, type=self.database.type), self._vars
            )
        )[1]


def id_to_one(
    related_model: typing.Type[Model], related_field: Field, auto: bool = False
) -> RelationField:
    return RelationField(
        lambda ins: related_model.where(related_field == ins.id).must_fetch_one(),
        auto=auto,
    )


def id_to_many(
    related_model: typing.Type[Model], related_field: Field, auto: bool = False
) -> RelationField:
    return RelationField(
        lambda ins: related_model.where(related_field == ins.id).fetch_all(), auto=auto
    )
