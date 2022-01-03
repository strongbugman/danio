import typing

from databases import Database as _Database
from databases.backends.mysql import ClauseElement
from databases.backends.mysql import MySQLBackend as _MySQLBackend
from databases.backends.mysql import MySQLConnection as _MySQLConnection


class MySQLConnection(_MySQLConnection):
    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            return (cursor.lastrowid, cursor.rowcount)
        finally:
            await cursor.close()


class MySQLBackend(_MySQLBackend):
    def connection(self) -> MySQLConnection:
        return MySQLConnection(self, self._dialect)


class Database(_Database):
    SUPPORTED_BACKENDS = {"mysql": "danio.database:MySQLBackend"}
