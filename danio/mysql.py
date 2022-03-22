import typing

import aiomysql
from databases.backends import mysql

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
            if isinstance(e, aiomysql.IntegrityError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class MySQLBackend(mysql.MySQLBackend):
    def connection(self) -> MySQLConnection:
        return MySQLConnection(self, self._dialect)
