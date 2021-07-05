"""
Base ORM model with CRUD
TODO: migration, page, error, signal
"""
import abc
import typing
import dataclasses
import time
import enum

from .database import Database


MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")


@dataclasses.dataclass
class Model(abc.ABC):
    @enum.unique
    class Operation(enum.IntEnum):
        CREATE = 1
        READ = 2
        UPDATE = 3
        DELETE = 4

    TABLE_NAME_PREFIX = ""

    id: int = 0
    created_at: typing.Optional[int] = None
    updated_at: typing.Optional[int] = None

    @property
    def table_name(self) -> str:
        return self.__class__.get_table_name()

    def dump(self) -> typing.Dict[str, typing.Any]:
        """Dump dataclass to DB level dict"""
        return dataclasses.asdict(self)

    async def save(
        self, database: typing.Optional[Database] = None, force_insert=False
    ):
        if not self.created_at:
            self.created_at = int(time.time())
        self.updated_at = int(time.time())
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

    async def delete(self, database: typing.Optional[Database] = None):
        if not database:
            database = self.__class__.get_database(
                self.Operation.DELETE, self.table_name
            )
        await database.delete(self.table_name, id=self.id)

    @classmethod
    def get_table_name(cls) -> str:
        return cls.TABLE_NAME_PREFIX + cls.__name__.lower()

    @classmethod
    @abc.abstractmethod
    def get_database(
        cls, operation: Operation, table: str, *args, **kwargs
    ) -> Database:
        """Get database instance, route database by operation"""

    @classmethod
    def get_fields(cls) -> typing.Tuple[str]:
        return tuple(str(f.name) for f in dataclasses.fields(cls))

    @classmethod
    def load(
        cls: typing.Type[MODEL_TV], rows: typing.List[typing.Mapping]
    ) -> typing.List[MODEL_TV]:
        """Load DB data to dataclass"""
        return [cls(**row) for row in rows]

    @classmethod
    async def get(
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
                cls.get_table_name(), cls.get_fields(), limit=limit, order_by=order_by, **conditions
            )
        )

    @classmethod
    async def count(
            cls: typing.Type[MODEL_TV],
            database: typing.Optional[Database] = None,
            **conditions: typing.Any
    ) -> int:
        if not database:
            database = cls.get_database(cls.Operation.READ, cls.get_table_name())

        return (await database.select(cls.get_table_name(), ("COUNT(*)",), **conditions))[0][0]

    @classmethod
    async def bulk_create(
        cls: typing.Type[MODEL_TV],
        instances: typing.Iterator[MODEL_TV],
        database: typing.Optional[Database] = None,
    ) -> typing.Iterator[MODEL_TV]:
        if not database:
            database = cls.get_database(cls.Operation.CREATE, cls.get_table_name())

        data = []
        for i in instances:
            if not i.created_at:
                i.created_at = int(time.time())
            i.updated_at = int(time.time())
            data.append(i.dump())
            if not i.id:
                data[-1].pop("id")

        fist_id = await database.insert(cls.get_table_name(), data)
        for i, ins in enumerate(instances):
            if not ins.id:
                ins.id = fist_id + i

        return instances

    @classmethod
    async def bulk_update(
        cls: typing.Type[MODEL_TV],
        instances: typing.Iterator[MODEL_TV],
        database: typing.Optional[Database] = None,
    ):
        if not database:
            database = cls.get_database(cls.Operation.CREATE, cls.get_table_name())

        data = []
        for i in instances:
            if not i.id:
                raise ValueError("Update with empty ID")
            i.updated_at = int(time.time())
            data.append(i.dump())

        await database.update(
            cls.get_table_name(), data, [{"id": i.id} for i in instances]
        )
