import logging
import os
import typing
from datetime import datetime

from . import utils
from .database import Database
from .model import Model, Schema


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
    down_sqls = []
    min_len = 0
    if db.type == Database.Type.MYSQL:
        min_len = 1
        sqls.append(f"USE `{db.url.database}`;")
    down_sqls.extend(sqls)
    for m in models:
        db_schema = None if not db.is_connected else await Schema.from_db(db, m)
        migration = m.schema - db_schema
        migration_sql = migration.to_sql(type=db.type)
        if migration_sql:
            sqls.append(migration_sql)
            down_sqls.append((~migration).to_sql(type=db.type))
    if len(sqls) == min_len:
        logging.info("No migration detected")
        return ""
    # write to file
    sql = "\n".join(sqls)
    down_sql = "\n".join(down_sqls)
    with open(
        os.path.join(dir, f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}_up.sql"), "w"
    ) as f:
        f.write(sql)
        logging.info(f"New migration sql file: {f.name}")
    with open(
        os.path.join(dir, f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}_down.sql"),
        "w",
    ) as f:
        f.write(down_sql)
        logging.info(f"New migration sql file: {f.name}")

    return sql
