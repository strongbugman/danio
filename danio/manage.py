import os
import typing
from datetime import datetime

from . import utils
from .database import Database
from .model import Model
from .schema import Schema


def get_models(paths: typing.List[str]) -> typing.List[typing.Type[Model]]:
    models = []
    for m in utils.find_classes(Model, paths):
        if not m.schema.abstracted:
            models.append(m)

    return models


async def make_migration(
    db: Database, models: typing.Sequence[typing.Type[Model]], dir: str
) -> str:
    """Make migration sql, compare model schema and database schema"""
    sqls = []
    sqls.append(
        f"CREATE DATABASE IF NOT EXISTS `{db.url.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    sqls.append(f"USE `{db.url.database}`;")
    for m in models:
        sqls.append((m.schema - await Schema.from_db(db, m)).to_sql())
    # write to file
    sql = "\n".join(sqls)
    with open(
        os.path.join(dir, f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.sql"), "w"
    ) as f:
        f.write(sql)

    return sql
