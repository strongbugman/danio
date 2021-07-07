import dataclasses

import pymysql
import pytest

from danio import Database, Model


@pytest.fixture()
async def database():
    e = Database(
        "mysql://root:app@localhost:3306/",
        maxsize=6,
        charset="utf8mb4",
        use_unicode=True,
        connect_timeout=60,
    )
    await e.connect()
    try:
        yield e
    finally:
        await e.disconnect()


@pytest.fixture()
async def read_database():
    e = Database(
        "mysql://root:app@localhost:3306/",
        maxsize=2,
        charset="utf8mb4",
        use_unicode=True,
        connect_timeout=60,
    )
    await e.connect()
    try:
        yield e
    finally:
        await e.disconnect()


@pytest.fixture(autouse=True)
async def db(database):
    db_name = "test_app"
    await database.execute(
        f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
    )
    await database.execute(f"USE `{db_name}`;")
    try:
        yield
    finally:
        await database.execute(f"DROP DATABASE {db_name};")


@pytest.fixture(autouse=True)
async def table(db, database):
    schema = """
    CREATE TABLE `user` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `updated_at` int(11) NOT NULL,
      `created_at` int(11) NOT NULL,
      `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    await database.execute(schema)


@pytest.mark.asyncio
async def test_database(database):
    results = await database.fetch_all("SHOW DATABASES;")
    assert results


@pytest.mark.asyncio
async def test_model(database):
    @dataclasses.dataclass
    class User(Model):
        name: str = ""

        @classmethod
        def get_database(cls, *_, **__) -> Database:
            return database

    # create
    u = User(name="test_user")
    await u.save()
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
async def test_shard_model(database, read_database):
    await read_database.execute("USE `test_app`;")

    @dataclasses.dataclass
    class User(Model):
        name: str = ""

        @classmethod
        def get_database(cls, operation: Model.Operation, *_, **__) -> Database:
            if operation == cls.Operation.READ:
                return read_database
            else:
                return database

    # create
    u = User(name="test_user")
    await u.save()
    assert u.id > 0
    # read
    u = (await User.select(id=u.id))[0]
    assert u.id
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


@pytest.mark.asyncio
async def test_bulk_operations(database):
    @dataclasses.dataclass
    class User(Model):
        name: str = ""

        @classmethod
        def get_database(cls, *_, **__) -> Database:
            return database

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
