"""
Provider base SQL operations
"""
import typing

from databases import Database as _Database


class Database(_Database):
    async def insert(
        self, table: str, data: typing.Sequence[typing.Dict[str, typing.Any]]
    ) -> int:
        params = {}
        for i, d in enumerate(data):
            for k, v in d.items():
                params[f"{k}_{i}"] = v

        keys = ", ".join(data[0].keys())
        values = ", ".join(
            (
                f"({', '.join((f':{k}_{i}' for k in d.keys()))})"
                for i, d in enumerate(data)
            )
        )
        return await self.execute(
            f"INSERT INTO `{table}` ({keys}) VALUES {values}",
            values=params,
        )

    async def update(
        self,
        table: str,
        data: typing.Sequence[typing.Dict[str, typing.Any]],
        conditions: typing.Sequence[typing.Dict[str, typing.Any]],
    ):
        sql = f"UPDATE `{table}` SET {', '.join(f'{k}=:{k}' for k in data[0].keys())}"
        if conditions:
            sql += f" WHERE {' AND '.join(f'{k}=:__{k}' for k in conditions[0].keys())}"
        sql += ";"

        await self.execute_many(
            sql,
            values=[
                {
                    **d,
                    **{f"__{k}": v for k, v in c.items()},
                }
                for d, c in zip(data, conditions)
            ],
        )

    async def select(
        self,
        table: str,
        keys: typing.Sequence[str],
        limit: typing.Optional[int] = None,
        order_by: typing.Optional[str] = None,
        **conditions: typing.Any,
    ) -> typing.List[typing.Mapping]:
        sql = f"SELECT {', '.join(keys)} from `{table}`"
        if conditions:
            sql += f" WHERE {' AND '.join(f'{k} = :{k}' for k in conditions)}"
        if order_by:
            sql += f" ORDER BY {order_by} "
        if limit:
            sql += f" LIMIT {limit}"
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
