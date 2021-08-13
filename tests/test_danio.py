import asyncio
import dataclasses
import datetime
import typing

import pymysql
import pytest

from danio import Database, Model, Schema, ValidateException

db = Database(
    "mysql://root:app@localhost:3306/",
    maxsize=1,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
)
read_db = Database(
    "mysql://root:app@localhost:3306/",
    maxsize=1,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
)
db_name = "test_danio"


@dataclasses.dataclass
class User(Model):
    name: str = ""  # "database: `name` varchar(255) NOT NULL COMMENT 'User name'"
    created_at: datetime.datetime = datetime.datetime.utcfromtimestamp(
        0
    )  # "database: `created_at` datetime NOT NULL COMMENT 'when created'"
    updated_at: datetime.datetime = datetime.datetime.utcfromtimestamp(
        0
    )  # "database: `updated_at` datetime NOT NULL COMMENT 'when updated'"

    async def before_save(self):
        self.updated_at = datetime.datetime.utcnow()
        if self.created_at.ctime() == "Thu Jan  1 00:00:00 1970":
            self.created_at = self.updated_at
        await super().before_save()

    def validate(self):
        if not self.name:
            raise ValidateException("Empty name!")

    @classmethod
    def get_database(
        cls, operation: Model.Operation, table: str, *args, **kwargs
    ) -> Database:
        if operation == Model.Operation.READ:
            return read_db
        else:
            return db


@pytest.fixture(autouse=True)
async def database():
    await db.connect()
    await read_db.connect()
    try:
        await db.execute(
            f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
        )
        await db.execute(f"USE `{db_name}`;")
        await db.execute(Schema.parse(User).to_sql())
        await read_db.execute(f"USE `{db_name}`;")
        yield db
    finally:
        await db.execute(f"DROP DATABASE {db_name};")
        await db.disconnect()
        await read_db.disconnect()


@pytest.mark.asyncio
async def test_database():
    results = await db.fetch_all("SHOW DATABASES;")
    assert results


@pytest.mark.asyncio
async def test_model():
    # create
    u = User(name="test_user")
    await asyncio.sleep(0.1)
    await u.save()
    assert u.updated_at >= u.created_at
    assert u.id > 0
    # read
    u = await User.get(id=u.id)
    assert u
    # read with limit
    u = await User.get(id=u.id)
    assert u
    # read with order by
    u = (await User.select(limit=1, order_by="name"))[0]
    assert u.id
    # count
    assert (await User.count()) == 1
    # update
    u.name = "admin_user"
    await u.save()
    assert u.name == "admin_user"
    # read
    u = (await User.select(id=u.id))[0]
    assert u.name == "admin_user"
    # delete
    await u.delete()
    assert not await User.select(id=u.id)
    # create with id
    u = User(id=101, name="test_user")
    await u.save(force_insert=True)
    u = (await User.select(id=u.id))[0]
    assert u.name == "test_user"


@pytest.mark.asyncio
async def test_bulk_operations():
    # create
    users = await User.bulk_create([User(name=f"user_{i}") for i in range(10)])
    for i, u in enumerate(users):
        assert u.id == i + 1
    # --
    users = await User.bulk_create([User(name=f"user_{i}") for i in range(10)])
    for i, u in enumerate(users):
        assert u.id == i + 1 + 10
    # with conflict
    users = [User(name=f"user_{i}") for i in range(10)]
    users[-1].id = 2
    with pytest.raises(pymysql.err.InternalError):
        await User.bulk_create(users)
    assert await User.count() == 20
    # with special id
    users = await User.bulk_create(
        [User(id=100 + i, name=f"user_{i}") for i in range(10)]
    )
    for i, u in enumerate(users):
        assert u.id == i + 100
    # update
    users = await User.select()
    for u in users:
        u.name = "update_name"
    await User.bulk_update(users)
    for u in users:
        assert u.name == "update_name"


@pytest.mark.asyncio
async def test_schema():
    @dataclasses.dataclass
    class UserProfile(User):
        user_id: int = 0  # "database: `user_id` int(10) NOT NULL COMMENT 'User ID'"
        level: int = 1  # "database: `level` int(10) NOT NULL COMMENT 'User level'"
        coins: int = 0  # "database: `coins` int(10) NOT NULL COMMENT 'User coins'"

        __table_unique_keys: typing.ClassVar = ((user_id,),)
        __table_index_keys: typing.ClassVar = (
            (
                User.created_at,
                User.updated_at,
            ),
            ("level",),
        )

    await db.execute(Schema.parse(UserProfile).to_sql())
    assert not (await UserProfile.select())
    assert (
        len(await db.fetch_all(f"SHOW INDEX FROM {UserProfile.get_table_name()}")) == 5
    )
    # generate all
    assert Schema.generate_all(["danio", "tests"])
    # abstract class

    @dataclasses.dataclass
    class BaseUserBackpack(User):
        user_id: int = 0  # "database: `user_id` int(10) NOT NULL COMMENT 'User ID'"
        weight: int = (
            0  # "database: `weight` int(10) NOT NULL COMMENT 'backpack weight'"
        )

        __table_abstracted: typing.ClassVar[bool] = True

    assert Schema.parse(BaseUserBackpack).abstracted
    assert Schema.parse(BaseUserBackpack).to_sql()
    # disable fields

    @dataclasses.dataclass
    class UserBackpack(BaseUserBackpack):
        id: int = 0  # using pk rather than id
        pk: int = 0  # "database: `pk` int(11) NOT NULL AUTO_INCREMENT"

        __table_primary_key: typing.ClassVar[str] = pk

    # db name
    @dataclasses.dataclass
    class UserBackpack2(BaseUserBackpack):
        user_id: int = 0  # "database: `user_id2` int(10) NOT NULL COMMENT 'User ID'"
        weight: int = (
            0  # "database: `{name}` int(10) NOT NULL COMMENT 'backpack weight'"
        )

    sql = Schema.parse(UserBackpack2).to_sql()
    assert "user_id2" in sql
    assert "weight" in sql
    await db.execute(Schema.parse(UserBackpack2).to_sql())
