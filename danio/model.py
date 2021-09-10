"""
Base ORM model with CRUD
"""
from __future__ import annotations

import dataclasses
import enum
import typing

from .schema import Schema
from .utils import class_property

if typing.TYPE_CHECKING:
    from .database import Database

MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")


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
        **conditions: typing.Any
    ) -> typing.List[MODEL_TV]:
        if not database:
            database = cls.get_database(cls.Operation.READ, cls.get_table_name())
        return cls.load(
            await database.select(
                cls.get_table_name(),
                [f.name for f in cls.schema.fields],
                limit=limit,
                order_by=order_by,
                **conditions
            )
        )

    @classmethod
    async def get(
        cls: typing.Type[MODEL_TV],
        database: typing.Optional[Database] = None,
        **conditions: typing.Any
    ) -> typing.Optional[MODEL_TV]:
        instances = await cls.select(database=database, **conditions)
        return instances[0] if instances else None

    @classmethod
    async def count(
        cls: typing.Type[MODEL_TV],
        database: typing.Optional[Database] = None,
        **conditions: typing.Any
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
