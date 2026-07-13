import typing

import psycopg2
from databases.backends import aiopg
from sqlalchemy.sql import ClauseElement

from . import exception


class PostgresConnection(aiopg.AiopgConnection):
    async def execute(self, query: ClauseElement) -> typing.Any:
        try:
            assert self._connection is not None, "Connection is not acquired"
            _query, args, _context = self._compile(query)
            cursor = await self._connection.cursor()
            try:
                await cursor.execute(_query, args)
                if "RETURNING id" in _query:
                    lastrowid = (await cursor.fetchall())[-1][0]
                else:
                    lastrowid = 0
                return lastrowid, cursor.rowcount
            finally:
                cursor.close()
        except Exception as e:
            if isinstance(e, psycopg2.IntegrityError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class PostgresBackend(aiopg.AiopgBackend):
    def connection(self) -> PostgresConnection:
        return PostgresConnection(self, self._dialect)
