import dataclasses
import enum
import glob
import os
import typing

import pytest

import danio

db = danio.Database(
    "sqlite://./tests/test.db",
)


@dataclasses.dataclass
class User(danio.Model):
    class Gender(enum.Enum):
        MALE = 0
        FEMALE = 1
        OTHER = 2

    id: int = danio.field(
        danio.IntField, primary=True, auto_increment=True, default=0, type="INTEGER"
    )
    name: str = danio.field(danio.CharField, type="CHAR(255)")
    age: int = danio.field(danio.IntField)
    gender: Gender = danio.field(
        danio.IntField, enum=Gender, default=Gender.MALE, not_null=False
    )
    _table_index_keys = ((name,),)

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
        async with db.connection() as connection:
            async with connection._connection._connection.cursor() as cursor:
                await cursor.executescript(User.schema.to_sql(type=db.type))
        yield db
    finally:
        for f in glob.glob("./tests/migrations/*.sql"):
            os.remove(f)
        for f in glob.glob("./tests/*.db"):
            os.remove(f)
        await db.disconnect()


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
    # use index
    if (await db.fetch_all("select sqlite_version();"))[0][0] < "3.38":
        # test with 3.38+
        return
    await User.where().use_index([list(User.schema.indexes)[0].name]).fetch_all()
    await User.where().force_index([list(User.schema.indexes)[0].name]).fetch_all()
    await User.where().ignore_index([]).fetch_all()
    # upset
    created, updated = await User.upsert(
        [
            dict(id=100, name="user", age=18, gender=1),
        ],
        update_fields=["name"],
        conflict_targets=("id",),
    )
    assert created
    assert updated

    created, updated = await User.upsert(
        [
            dict(id=100, name="updated", age=18, gender=1),
        ],
        update_fields=["name"],
    )
    assert not created
    assert updated
    assert (await User.where(User.id == 100).fetch_one()).name == "updated"


@pytest.mark.asyncio
async def test_complicated_update():
    # +1
    u = await User(name="rails").save()
    await User.where(User.id == u.id).update(age=User.age + 1)
    assert (await User.where(User.id == u.id).fetch_one()).age == u.age + 1
    # *1
    await User.where(User.id == u.id).update(age=User.age * 1)
    assert (await User.where(User.id == u.id).fetch_one()).age == u.age + 1
    # /1
    await User.where(User.id == u.id).update(age=User.age / 1)
    assert (await User.where(User.id == u.id).fetch_one()).age == u.age + 1
    # -1
    await User.where(User.id == u.id).update(age=User.age - 1)
    assert (await User.where(User.id == u.id).fetch_one()).age == u.age
    # +self
    u.age = 1
    await u.save()
    await User.where(User.id == u.id).update(age=User.age + User.age)
    assert (await User.where(User.id == u.id).fetch_one()).age == u.age * 2
    # -self
    await User.where(User.id == u.id).update(age=User.age - User.age)
    assert (await User.where(User.id == u.id).fetch_one()).age == 0
    # combine
    await User.where(User.id == u.id).update(age=User.age - 1 + 10)
    assert (await User.where(User.id == u.id).fetch_one()).age == 9
    assert await User.where((User.id + 1) > u.id).fetch_one()
    assert await User.where((User.id + 0) >= u.id).fetch_one()
    assert await User.where((User.id - 1) < u.id).fetch_one()
    assert await User.where((User.id - 0) <= u.id).fetch_one()
    assert await User.where((User.id - 1) != u.id).fetch_one()
    # multi express
    await User.where(User.id == u.id).update(age=User.age + 1 + (User.age / 9))
    assert (await User.where(User.id == u.id).fetch_one()).age == 11
    await User.where(User.id == u.id).update(age=User.age + 1 - (User.age / 11))
    assert (await User.where(User.id == u.id).fetch_one()).age == 11
    await User.where(User.id == u.id).update(age=(User.age + 1) * (User.age / 11))
    assert (await User.where(User.id == u.id).fetch_one()).age == 12
    await User.where(User.id == u.id).update(age=(User.age + 1) / (User.age / 12) - 2)
    assert (await User.where(User.id == u.id).fetch_one()).age == 11
    # case
    await User.where(User.id == u.id).update(
        age=User.age.case(User.age > 10, 1).case(User.age < 10, 10)
    )
    assert (await User.where(User.id == u.id).fetch_one()).age == 1
    # case default
    await User.where(User.id == u.id).update(
        age=User.age.case(User.age > 10, 1, default=18).case(User.age <= 0, 10)
    )
    assert (await User.where(User.id == u.id).fetch_one()).age == 18


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
    # with special id
    users = [User(name=f"user_100_{i}") for i in range(10)]
    users[1].id = 34
    with pytest.raises(danio.exception.OperationException):
        await User.bulk_create(users)
    users = [User(id=30 + i, name=f"user_100_{30 + i}") for i in range(10)]
    await User.bulk_create(users)
    for i in range(30, 40):
        user = await User.where(User.id == i).fetch_one()
        assert user.name == f"user_100_{i}"
    # update
    users = await User.where().fetch_all()
    for user in users:
        user.name += f"_updated_{user.id}"
    await User.bulk_update(users)
    for user in await User.where().fetch_all():
        assert user.name.endswith(f"_updated_{user.id}")
    # update with special fields
    users = await User.where().fetch_all()
    for user in users:
        user.name += f"_updated_{user.id}"
        user.gender = User.Gender.OTHER
    await User.bulk_update(users, fields=(User.name,))
    for user in await User.where().fetch_all():
        assert user.name.endswith(f"_updated_{user.id}")
        assert user.gender == User.gender.default
    # delete
    await User.bulk_delete(users)
    assert not await User.where().fetch_all()


@pytest.mark.asyncio
async def test_combo_operations():
    @dataclasses.dataclass
    class UserProfile(danio.Model):
        id: int = danio.field(
            danio.IntField, primary=True, auto_increment=True, default=0, type="INTEGER"
        )
        user_id: int = danio.field(danio.IntField, type="INTEGER")
        level: int = danio.field(danio.IntField, type="INTEGER")

        _table_unique_keys = ((user_id,),)

        @classmethod
        def get_database(
            cls, operation: danio.Operation, table: str, *args, **kwargs
        ) -> danio.Database:
            return db

    async with db.connection() as connection:
        async with connection._connection._connection.cursor() as cursor:
            await cursor.executescript(UserProfile.schema.to_sql(type=db.type))
    # get or create
    up, created = await UserProfile(user_id=1, level=10).get_or_create(
        key_fields=(UserProfile.user_id,)
    )
    assert up.id
    assert created
    # --
    up, created = await UserProfile(user_id=1, level=11).get_or_create(
        key_fields=(UserProfile.user_id,)
    )
    assert up.id
    assert not created
    assert up.level == 10
    # create or update
    up, created, updated = await UserProfile(user_id=2, level=10).create_or_update(
        key_fields=(UserProfile.user_id,), for_update=False
    )
    assert up.id
    assert created
    assert not updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.user_id,), for_update=False
    )
    assert up.id
    assert not created
    assert updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.user_id,), for_update=False
    )
    assert up.id
    assert not created
    assert updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=12).create_or_update(
        key_fields=(UserProfile.user_id,),
        update_fields=(UserProfile.user_id,),
        for_update=False,
    )
    assert up.id
    assert not created
    assert updated


@pytest.mark.asyncio
async def test_schema():
    @dataclasses.dataclass
    class UserProfile(User):
        user_id: int = danio.field(field_cls=danio.IntField, type="INTEGER")
        level: int = danio.field(field_cls=danio.IntField, type="INTEGER")
        coins: int = danio.field(field_cls=danio.IntField, type="INTEGER")

        _table_unique_keys: typing.ClassVar = ((user_id,),)
        _table_index_keys: typing.ClassVar = (
            (
                User.gender,
                User.age,
            ),
            ("level",),
        )

    m = danio.Schema.from_model(UserProfile) - UserProfile.schema
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    async with db.connection() as connection:
        async with connection._connection._connection.cursor() as cursor:
            await cursor.executescript(UserProfile.schema.to_sql(type=db.type))
    assert not (await UserProfile.where().fetch_all())
    assert (
        len(await db.fetch_all(f"PRAGMA INDEX_LIST('{UserProfile.table_name}');")) == 3
    )


@pytest.mark.asyncio
async def test_migrate():
    if (await db.fetch_all("select sqlite_version();"))[0][0] < "3.35":
        # Migrate support with 3.35+
        return

    @dataclasses.dataclass
    class UserProfile(User):
        user_id: int = danio.field(field_cls=danio.IntField, type="INTEGER")
        level: int = danio.field(field_cls=danio.IntField, default=1, type="INTEGER")
        coins: int = danio.field(field_cls=danio.IntField, type="INTEGER")

        _table_unique_keys: typing.ClassVar = ((user_id,),)
        _table_index_keys: typing.ClassVar = (
            (
                User.gender,
                User.age,
            ),
            ("level",),
        )

    for idx in UserProfile.schema.indexes:
        if idx.fields[0].name == "level":
            idx.name = "level_11_idx"

    async with db.connection() as connection:
        async with connection._connection._connection.cursor() as cursor:
            await cursor.executescript(UserProfile.schema.to_sql(type=db.type))
            await cursor.executescript(
                "ALTER TABLE userprofile ADD COLUMN `group_id` INTEGER NOT NULL DEFAULT 0;"
                "CREATE  INDEX `userprofile_group_id_6969_idx`  on `userprofile` (`group_id`);"
                "CREATE  INDEX `userprofile_user_id_6969_idx`  on `userprofile` (`user_id`);"
                "DROP INDEX level_11_idx;"
                "ALTER TABLE userprofile DROP COLUMN level;"
            )
    # make migration
    old_schema = await danio.Schema.from_db(db, UserProfile)
    migration: danio.Migration = UserProfile.schema - old_schema
    assert len(migration.add_fields) == 1
    assert migration.add_fields[0].name == "level"
    assert len(migration.drop_fields) == 1
    assert migration.drop_fields[0].name == "group_id"
    assert len(migration.add_indexes) == 1
    assert migration.add_indexes[0].fields[0].name == "level"
    assert len(migration.drop_indexes) == 2
    assert migration.drop_indexes[0].fields[0].name in ("group_id", "user_id")
    assert migration.drop_indexes[1].fields[0].name in ("group_id", "user_id")
    # migrate
    async with db.connection() as connection:
        async with connection._connection._connection.cursor() as cursor:
            await cursor.executescript(migration.to_sql(type=db.type))
    m = UserProfile.schema - await danio.Schema.from_db(db, UserProfile)
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    # down migrate
    async with db.connection() as connection:
        async with connection._connection._connection.cursor() as cursor:
            await cursor.executescript((~migration).to_sql(type=db.type))
    m = old_schema - await danio.Schema.from_db(db, UserProfile)
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    # drop table
    await db.execute((~(UserProfile.schema - None)).to_sql())
