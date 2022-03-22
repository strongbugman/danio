import typing

import asyncpg
from databases.backends import postgres

from . import exception


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
            if "RETURNING id" in query:
                last_id = data[-1][0]
            if "INSERT" in status or "UPDATE" in status or "DELETE" in status:
                row_count = int(status.split(" ")[-1])
            return last_id, row_count
        except Exception as e:
            if isinstance(e, asyncpg.IntegrityConstraintViolationError):
                raise exception.IntegrityError(str(e)) from e
            else:
                raise e


class PostgresBackend(postgres.PostgresBackend):
    def connection(self) -> PostgresConnection:
        return PostgresConnection(self, self._dialect)
