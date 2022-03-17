import enum
import typing

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore


from databases import Database as _Database
from databases.backends import mysql, sqlite

from . import exception


class MySQLConnection(mysql.MySQLConnection):
    async def execute(self, query: mysql.ClauseElement) -> typing.Any:
        try:
            assert self._connection is not None, "Connection is not acquired"
            query, args, context = self._compile(query)
            cursor = await self._connection.cursor()
            try:
                await cursor.execute(query, args)
                return (cursor.lastrowid, cursor.rowcount)
            finally:
                await cursor.close()
        except Exception as e:
            if pymysql and isinstance(e, pymysql.IntegrityError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class MySQLBackend(mysql.MySQLBackend):
    def connection(self) -> MySQLConnection:
        return MySQLConnection(self, self._dialect)


class SQLiteConnection(sqlite.SQLiteConnection):
    async def execute(self, query: sqlite.ClauseElement) -> typing.Any:
        try:
            assert self._connection is not None, "Connection is not acquired"
            query, args, context = self._compile(query)
            async with self._connection.cursor() as cursor:
                await cursor.execute(query, args)
                return cursor.lastrowid, cursor.rowcount
        except Exception as e:
            # TODO
            # if pymysql and isinstance(e, pymysql.IntegrityError):
            #     raise exception.IntegrityError(str(e)) from e
            # else:
            raise e


class SQLiteBackend(sqlite.SQLiteBackend):
    def connection(self) -> SQLiteConnection:
        return SQLiteConnection(self._pool, self._dialect)


class Database(_Database):
    class Type(enum.Enum):
        MYSQL = "mysql"
        SQLITE = "sqlite"

    SUPPORTED_BACKENDS = {
        "mysql": "danio.database:MySQLBackend",
        "sqlite": "danio.database:SQLiteBackend",
    }

    @property
    def type(self) -> Type:
        for t in self.Type:
            if t.value in str(self._backend).lower():
                return t
        raise exception.SchemaException(f"Can't determine {self._backend}'s type")
