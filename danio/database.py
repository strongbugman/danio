"""
Mange DB connection pool and  provider base SQL operations
"""
import typing

from databases import Database as _Database


class Database(_Database):

    async def insert(self, table: str, data: typing.Dict[str, typing.Any]) -> int:
        return await self.execute(
            f"INSERT INTO `{table}` ({', '.join(data.keys())}) VALUES ({', '.join((':' + k for k in data.keys()))});",
            values=data,
        )

    async def update(
        self,
        table: str,
        data: typing.Dict[str, typing.Any],
        **conditions: typing.Any,
    ):
        sql = f"UPDATE `{table}` SET {', '.join(f'{k}=:{k}' for k in data.keys())}"
        if conditions:
            sql += f" WHERE {' AND '.join(f'{k}=:__{k}' for k in conditions.keys())}"
        sql += ";"

        await self.execute(
            sql,
            values={
                **data,
                **{f"__{k}": v for k, v in conditions.items()},
            },
        )

    async def select(
        self,
        table: str,
        keys: typing.Sequence[str],
        limit: typing.Optional[int] = None,
        order_by="id",
        **conditions: typing.Any,
    ) -> typing.List[typing.Mapping]:
        sql = f"SELECT {', '.join(keys)} from `{table}`"
        if conditions:
            sql += f" WHERE {' AND '.join(f'{k} = :{k}' for k in conditions)} ORDER BY {order_by}"
        if limit:
            sql += f"LIMIT {limit}"
        sql += ";"

        return await self.fetch_all(
            sql,
            values=conditions,
        )

    async def delete(
        self,
        table: str,
        **conditions: typing.Any,
    ) -> bool:
        sql = f"DELETE from `{table}`"
        if conditions:
            sql += f" WHERE {' AND '.join(f'{k}=:{k}' for k in conditions.keys())}"
        sql += ";"

        return bool(await self.execute(sql, values=conditions))
