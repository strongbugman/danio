import dataclasses

import pytest

from danio import Database, Model


@pytest.fixture()
async def database():
    e = Database("mysql://root:app@localhost:3306/", maxsize=6, charset="utf8mb4", use_unicode=True, connect_timeout=60)
    await e.connect()
    try:
        yield e
    finally:
        await e.disconnect()


@pytest.fixture()
async def read_database():
    e = Database("mysql://root:app@localhost:3306/", maxsize=2, charset="utf8mb4", use_unicode=True, connect_timeout=60)
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
    u = (await User.get(id=u.id))[0]
    assert u.id
    # update
    u.name = "admin_user"
    await u.save()
    assert u.name == "admin_user"
    # read
    u = (await User.get(id=u.id))[0]
    assert u.name == "admin_user"
    # delete
    await u.delete()
    assert not await User.get(id=u.id)


@pytest.mark.asyncio
async def test_shard_model(database, read_database):
    await read_database.execute("USE `test_app`;")

    @dataclasses.dataclass
    class User(Model):
        name: str = ""

        @classmethod
        def get_database(cls, operation, *_, **__) -> Database:
            if operation == "get":
                return read_database
            else:
                return database
    # create
    u = User(name="test_user")
    await u.save()
    assert u.id > 0
    # read
    u = (await User.get(id=u.id))[0]
    assert u.id
    # update
    u.name = "admin_user"
    await u.save()
    assert u.name == "admin_user"
    # read
    u = (await User.get(id=u.id))[0]
    assert u.name == "admin_user"
    # delete
    await u.delete()
    assert not await User.get(id=u.id)
