import typing

import aiosqlite
from databases.backends import sqlite
from sqlalchemy.sql.elements import ClauseElement

from . import exception


class SQLiteConnection(sqlite.SQLiteConnection):
    async def execute(self, query: ClauseElement) -> typing.Any:
        try:
            assert self._connection is not None, "Connection is not acquired"
            _query, args, *_ = self._compile(query)
            async with self._connection.cursor() as cursor:
                await cursor.execute(_query, args)
                return cursor.lastrowid, cursor.rowcount
        except Exception as e:
            if isinstance(e, aiosqlite.IntegrityError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class SQLiteBackend(sqlite.SQLiteBackend):
    def connection(self) -> SQLiteConnection:
        return SQLiteConnection(self._pool, self._dialect)
