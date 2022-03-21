import enum
import typing

try:
    import pymysql
except ImportError:
    pymysql = None  # type: ignore

try:
    import aiosqlite
except ImportError:
    aiosqlite = None  # type: ignore

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore


from databases import Database as _Database
from databases.backends import mysql, postgres, sqlite

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
            if aiosqlite and isinstance(e, aiosqlite.IntegrityError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class SQLiteBackend(sqlite.SQLiteBackend):
    def connection(self) -> SQLiteConnection:
        return SQLiteConnection(self._pool, self._dialect)


class PostgresConnection(postgres.PostgresConnection):
    async def execute(self, query: postgres.ClauseElement) -> typing.Any:
        try:
            assert self._connection is not None, "Connection is not acquired"
            query, args, result_columns = self._compile(query)
            data, _status, _d = await self._connection._execute(
                query, args, 0, None, return_status=True
            )
            status = _status.decode()
            last_id = row_count = 0
            if data:
                last_id = data[-1][0]
            if "INSERT" in status or "UPDATE" in status or "DELETE" in status:
                row_count = int(status.split(" ")[-1])
            return last_id, row_count
        except Exception as e:
            if asyncpg and isinstance(e, asyncpg.IntegrityConstraintViolationError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class PostgresBackend(postgres.PostgresBackend):
    def connection(self) -> PostgresConnection:
        return PostgresConnection(self, self._dialect)


class Database(_Database):
    class Type(enum.Enum):
        MYSQL = "mysql"
        POSTGRES = "postgres"
        SQLITE = "sqlite"

    SUPPORTED_BACKENDS = {
        "mysql": "danio.database:MySQLBackend",
        "sqlite": "danio.database:SQLiteBackend",
        "postgres": "danio.database:PostgresBackend",
    }

    @property
    def type(self) -> Type:
        for t in self.Type:
            if t.value in str(self._backend).lower():
                return t
        raise exception.SchemaException(f"Can't determine {self._backend}'s type")

    @classmethod
    def get_quote(cls, type: Type) -> str:
        if type == cls.Type.POSTGRES:
            return '"'
        else:
            return "`"
