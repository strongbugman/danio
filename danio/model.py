"""
Base ORM model with CRUD
"""
import abc
import typing
import dataclasses
import time

from .database import Database


MODEL_TV = typing.TypeVar("MODEL_TV", bound="Model")


@dataclasses.dataclass
class Model(abc.ABC):
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

    async def save(self, database: typing.Optional[Database] = None, force_insert=False):
        if not database:
            database = self.__class__.get_database("save", self.table_name)
        if not self.created_at:
            self.created_at = int(time.time())
        self.updated_at = int(time.time())
        data = self.dump()
        data.pop("id")
        if self.id != 0 and not force_insert:
            await database.update(self.table_name, data, id=self.id)
        else:
            self.id = await database.insert(self.table_name, data)

    async def delete(self, database: typing.Optional[Database] = None):
        if not database:
            database = self.__class__.get_database("delete", self.table_name)
        await database.delete(self.table_name, id=self.id)

    @classmethod
    def get_table_name(cls) -> str:
        return cls.TABLE_NAME_PREFIX + cls.__name__.lower()

    @classmethod
    @abc.abstractmethod
    def get_database(cls, operation: str, table: str, *args, **kwargs) -> Database:
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
        database: typing.Optional[Database] = None,
        **conditions: typing.Any
    ) -> typing.List[MODEL_TV]:
        if not database:
            database = cls.get_database("get", cls.get_table_name())
        return cls.load(
            await database.select(
                cls.get_table_name(), cls.get_fields(), limit=limit, **conditions
            )
        )
