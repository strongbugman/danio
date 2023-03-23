import inspect
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


async def write_model_hints(
    db: Database, Model: typing.Type[Model], type_hint="typing.ClassVar[danio.Field]"
):
    hints_flag = f"{'-' * 20}Danio Hints{'-' * 20}"
    # analyze
    lines, no = inspect.getsourcelines(Model)
    with open(inspect.getsourcefile(Model), "r") as file:  # type: ignore
        all_lines = file.readlines()

    start_index = 0
    for i, l in enumerate(lines):
        if l.strip().startswith("class "):
            start_index = no + i
            break

    old_start_index = 0
    old_end_index = 0
    for i, l in enumerate(lines):
        if hints_flag in l:
            if not old_start_index:
                old_start_index = no + i - 1
            else:
                old_end_index = no + i - 1

    indentsize = 0
    for i in range(start_index, len(all_lines)):
        if all_lines[i]:
            indentsize = inspect.indentsize(all_lines[i])
            break
    hints_title = f"{' ' * indentsize}# {hints_flag}\n"
    # write
    ths = [hints_title]
    ths.append(f"{' ' * indentsize}# TABLE NAME: {Model.table_name}\n")
    migrated = False
    db_schema = await Schema.from_db(db, Model)
    if db_schema and db_schema == Model.schema:
        migrated = True
        Model.schema.sync_index_name(db_schema)
    ths.append(
        f"{' ' * indentsize}# TABLE IS {'NOT ' if not migrated else ''}MIGRATED!\n"
    )
    for f in Model.schema.fields:
        ths.append(
            f"{' ' * indentsize}{f.model_name.upper()}: {type_hint}  # {f.to_sql(db.type)}\n"
        )
    for idx in Model.schema.indexes:
        ths.append(
            f"{' ' * indentsize}# TABLE {'UNIQUE ' if idx.unique else ''}INDEX: {idx.name if migrated else ''}({','.join(f.name for f in idx.fields)})\n"
        )
    ths.append(hints_title)
    if old_start_index and old_end_index:
        all_lines = all_lines[:old_start_index] + all_lines[old_end_index + 1 :]
    all_lines = all_lines[:start_index] + ths + all_lines[start_index:]
    with open(inspect.getsourcefile(Model), "w") as file:  # type: ignore
        file.writelines(all_lines)


async def init(db: Database, paths: typing.List[str]) -> None:
    for M in get_models(paths):
        await write_model_hints(db, M)
