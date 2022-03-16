import dataclasses
import glob
import os

import pytest

import danio

db = danio.Database(
    "sqlite://./tests/test.db",
)


@dataclasses.dataclass
class User(danio.Model):
    id: int = danio.field(
        danio.IntField, primary=True, auto_increment=True, default=0, type="INTEGER"
    )
    name: str = danio.field(danio.CharField, type="CHAR(255)")
    age: int = danio.field(danio.IntField, type="INTEGER")

    @classmethod
    def get_database(
        cls, operation: danio.Operation, table: str, *args, **kwargs
    ) -> danio.Database:
        return db


@pytest.fixture(autouse=True)
async def database():
    await db.connect()
    if not os.path.exists(os.path.join("tests", "migrations")):
        os.mkdir(os.path.join("tests", "migrations"))
    try:
        await db.execute(danio.Schema.from_model(User).to_sql(database="sqlite"))
        yield db
    finally:
        for f in glob.glob("./tests/migrations/*.sql"):
            os.remove(f)
        for f in glob.glob("./tests/*.db"):
            os.remove(f)


@pytest.mark.asyncio
async def test_sql():
    # create
    u = User(name="test_user")
    await u.save()
    assert u.id == 1
    # create with id
    u = User(id=10, name="test_user")
    await u.save(force_insert=True)
    assert u.id == 10
    # read
    assert await User.where(User.id == u.id).fetch_one()
    assert await User.where(raw=f"id = {u.id}").fetch_one()
    # read with limit
    assert await User.where(User.id == u.id).limit(1).fetch_all()
    # read with order by
    assert await User.where().limit(1).order_by(User.name, asc=False).fetch_one()
    assert await User.where().limit(1).order_by(User.name).fetch_one()
    # read with page
    for _ in range(10):
        await User(name="test_users").save()
    assert await User.where().offset(10).limit(1).fetch_one()
    assert await User.where().limit(1).offset(10).fetch_one()
    assert not await User.where().offset(20).limit(1).fetch_one()
    # count
    assert await User.where().fetch_count() == 12
    assert await User.where(User.id == -1).fetch_count() == 0
    # row data
    assert await User.where().fetch_row()
    # update
    u = await User.where(User.id == u.id).fetch_one()
    u.name = "updated"
    await u.save()
    u = await User.where(User.id == u.id).fetch_one()
    assert u.name == "updated"
    # delete
    await User.where().delete()
    assert not await User.where().fetch_count()
    # upset
    created, updated = await User.upsert(
        [
            dict(id=100, name="user", age=18),
        ],
        update_fields=["name"],
    )
    assert created
    assert updated

    created, updated = await User.upsert(
        [
            dict(id=100, name="updated", age=18),
        ],
        update_fields=["name"],
    )
    assert not created
    assert updated
    assert (await User.where(User.id == 100).fetch_one()).name == "updated"
