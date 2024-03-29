import asyncio
import dataclasses
import datetime
import enum
import glob
import os
import random
import typing

import pytest
import pytest_asyncio

import danio

db_name = "test_danio"
db = danio.Database(
    f"postgres://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{db_name}",
    min_size=1,
    max_size=3,
    max_inactive_connection_lifetime=60,
)
read_db = danio.Database(
    f"postgres://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{db_name}",
    min_size=1,
    max_size=3,
)
db2 = danio.Database(
    f"aiopg://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{db_name}",
    min_size=1,
    max_size=3,
)
read_db2 = danio.Database(
    f"aiopg://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{db_name}",
    min_size=1,
    max_size=3,
)


user_count = 0


@danio.model
class User(danio.Model):
    # --------------------Danio Hints--------------------
    # TABLE NAME: user
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]  # "id" serial PRIMARY KEY NOT NULL
    NAME: typing.ClassVar[danio.Field]  # "name" varchar(255)  NOT NULL
    AGE: typing.ClassVar[danio.Field]  # "age" int  NOT NULL
    CREATED_AT: typing.ClassVar[
        danio.Field
    ]  # "created_at" timestamp without time zone  NOT NULL
    UPDATED_AT: typing.ClassVar[
        danio.Field
    ]  # "updated_at" timestamp without time zone  NOT NULL
    GENDER: typing.ClassVar[danio.Field]  # "gender" int  NOT NULL
    # --------------------Danio Hints--------------------

    class Gender(enum.Enum):
        MALE = 0
        FEMALE = 1
        OTHER = 2

    id: typing.Annotated[int, danio.IntField(primary=True, type="serial")] = 0
    name: typing.Annotated[str, danio.CharField(comment="User name")] = ""
    age: typing.Annotated[int, danio.IntField] = 0
    created_at: typing.Annotated[
        datetime.datetime,
        danio.DateTimeField(type="timestamp without time zone", comment="when created"),
    ] = dataclasses.field(default_factory=datetime.datetime.now)
    updated_at: typing.Annotated[
        datetime.datetime,
        danio.DateTimeField(type="timestamp without time zone", comment="when updated"),
    ] = dataclasses.field(default_factory=datetime.datetime.now)
    gender: typing.Annotated[Gender, danio.IntField(enum=Gender)] = Gender.MALE

    async def after_create(self):
        global user_count
        user_count += 1
        await super().after_create()

    async def before_update(self, **kwargs):
        self.updated_at = datetime.datetime.now()
        await super().before_update(**kwargs)

    async def validate(self):
        await super().validate()
        if not self.name:
            raise danio.ValidateException("Empty name!")

    @classmethod
    def get_database(
        cls, operation: danio.Operation, *args, **kwargs
    ) -> danio.Database:
        if operation == danio.Operation.READ:
            return read_db if random.randint(1, 10) > 5 else read_db2
        else:
            return db if random.randint(1, 10) > 5 else db2


@pytest_asyncio.fixture(autouse=True)
async def database():
    _db = danio.Database(
        f"postgres://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/",
        min_size=1,
        max_size=3,
        max_inactive_connection_lifetime=60,
    )
    await _db.connect()
    if not os.path.exists(os.path.join("tests", "migrations")):
        os.mkdir(os.path.join("tests", "migrations"))
    try:
        await _db.execute(
            f"CREATE DATABASE {db_name};",
        )
        await db.connect()
        await read_db.connect()
        await read_db2.connect()
        await db2.connect()
        for sql in User.get_schema().to_sql(type=db.type).split(";"):
            if sql:
                await db.execute(sql + ";")
        await danio.manage.init(db, ["tests.test_postgres"])
        yield db
    finally:
        await db.disconnect()
        await read_db.disconnect()
        await read_db2.disconnect()
        await db2.disconnect()
        await _db.execute(f"DROP DATABASE {db_name};")
        await _db.disconnect()
        for f in glob.glob("./tests/migrations/*.sql"):
            os.remove(f)


@pytest.mark.asyncio
async def test_database():
    results = await db.fetch_all("SELECT datname FROM pg_database;")
    assert results


@pytest.mark.asyncio
async def test_sql():
    # create
    u = User(name="test_user")
    await asyncio.sleep(0.1)
    await u.save()
    assert u.updated_at
    assert u.created_at
    assert u.id > 0
    assert u.gender is u.Gender.MALE
    assert u.table_name == User.table_name
    assert user_count == 1
    with pytest.raises(danio.ValidateException):
        await User().save()
    # read
    assert await User.where(User.ID == u.id).fetch_one()
    assert await User.where(raw=f"id = {u.id}").fetch_one()
    # read with limit
    assert await User.where(User.ID == u.id).limit(1).fetch_all()
    # read with order by
    assert await User.where().limit(1).order_by(User.name, asc=False).fetch_one()
    assert (
        await User.where().limit(1).order_by(User.name, User.id, asc=False).fetch_one()
    )
    assert (
        await User.where()
        .limit(1)
        .order_by(User.NAME, User.ID - 1, asc=False)
        .fetch_one()
    )
    assert (
        await User.where()
        .limit(1)
        .order_by(User.age + User.gender, asc=False)
        .fetch_one()
    )
    # read with page
    for _ in range(10):
        await User(name="test_users").save()
    assert await User.where().offset(10).limit(1).fetch_one()
    assert not await User.where().offset(11).limit(1).fetch_one()
    assert await User.where().limit(1).offset(10).fetch_one()
    assert not await User.where().limit(1).offset(11).fetch_one()
    # count
    assert await User.where().fetch_count() == 11
    assert await User.where(User.id == -1).fetch_count() == 0
    assert user_count == 11
    # row data
    assert await User.where().fetch_row()
    # save with special fields only
    u = await User.where().fetch_one()
    u.name = "tester"
    u.gender = u.Gender.OTHER
    u = await u.save(fields=[User.name])
    nu = await User.where(User.id == u.id).fetch_one()
    assert nu.name == "tester"
    assert nu.gender == User.Gender.MALE
    assert user_count == 11
    # save with wrong field
    u = await User.where().fetch_one()
    u.gender = 10
    with pytest.raises(danio.ValidateException):
        await u.save()
    # update
    u = await User.where().fetch_one()
    u.name = "admin_user"
    await u.save()
    assert u.name == "admin_user"
    await User.where(User.id == u.id).update(name=User.name.to_database("admin_user2"))
    assert (await User.where(User.id == u.id).fetch_one()).name == "admin_user2"
    # read
    u = (await User.where(User.id == u.id).fetch_all())[0]
    assert u.name == "admin_user2"
    # read only special field
    u = await User.where().fetch_one(fields=(User.name,))
    assert not u.id
    assert u.name
    # refetch
    u = await User.where().fetch_one()
    await User.where(User.id == u.id).update(name="user1")
    await u.refetch()
    assert u.name == "user1"
    # delete
    assert await u.delete()
    assert not await User.where(User.id == u.id).fetch_all()
    u = await User.where().fetch_one()
    await User.where(User.id == u.id).delete()
    assert not await User.where(User.id == u.id).fetch_all()
    # create with id
    u = User(id=101, name="test_user")
    await u.save(force_insert=True)
    u = (await User.where(User.id == u.id).fetch_all())[0]
    assert u.name == "test_user"
    # multi where condition
    assert await User.where(
        ((User.ID != 1) | (User.NAME != "")) & (User.GENDER == User.Gender.MALE)
    ).fetch_all()
    assert await User.where(User.ID != 1, User.NAME != "", is_and=False).fetch_all()
    assert (
        not await User.where(User.ID != 1, User.NAME != "", is_and=False)
        .where(User.GENDER == User.Gender.FEMALE)
        .fetch_all()
    )
    assert await User.where(User.NAME.like("test_%")).fetch_all()
    assert await User.where(
        User.GENDER.contains([g.value for g in User.Gender])
    ).fetch_all()
    # combine condition
    u = await User.where().fetch_one()
    u.age = 2
    await u.save()
    assert await User.where((User.age + 1) == 3).fetch_all()
    # delete many
    await User.where(User.id >= 1).delete()
    assert not await User.where().fetch_all()
    # transaction
    db = User.get_database(danio.Operation.UPDATE, User.table_name)
    async with db.transaction():
        for u in await User.where(database=db).fetch_all():
            u.name += "_updated"
            u.save(fields=[User.name], database=db)
    # exclusive lock
    async with db.transaction():
        for u in await User.where(database=db).for_update().fetch_all():
            u.name += "_updated"
            u.save(fields=[User.name], database=db)
    # share lock
    async with db.transaction():
        for u in await User.where(database=db).for_share().fetch_all():
            u.name += "_updated"
            u.save(fields=[User.name], database=db)
    # upsert

    @danio.model
    class UserProfile(danio.Model):
        id: typing.Annotated[int, danio.IntField(primary=True, type="serial")] = 0
        user_id: typing.Annotated[int, danio.IntField] = 0
        level: typing.Annotated[int, danio.IntField] = 0

        _table_unique_keys = (("user_id",),)

        @classmethod
        def get_database(
            cls, operation: danio.Operation, *args, **kwargs
        ) -> danio.Database:
            if operation == danio.Operation.READ:
                return read_db
            else:
                return db

    for sql in UserProfile.schema.to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
    created, updated = await UserProfile.upsert(
        [
            dict(user_id=1, level=10),
        ],
        update_fields=["level"],
        conflict_targets=("user_id",),
    )
    assert created
    assert updated
    # --
    created, updated = await UserProfile.upsert(
        [
            dict(user_id=1, level=11),
        ],
        update_fields=["level"],
        conflict_targets=("user_id",),
    )
    assert created
    assert updated


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
    @danio.model
    class UserProfile(danio.Model):
        id: typing.Annotated[int, danio.IntField(primary=True, type="serial")] = 0
        user_id: typing.Annotated[int, danio.IntField] = 0
        level: typing.Annotated[int, danio.IntField] = 0

        _table_unique_keys = (("user_id",),)

        @classmethod
        def get_database(
            cls, operation: danio.Operation, *args, **kwargs
        ) -> danio.Database:
            if operation == danio.Operation.READ:
                return read_db
            else:
                return db

    for sql in UserProfile.schema.to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
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
        key_fields=(UserProfile.user_id,)
    )
    assert up.id
    assert created
    assert not updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.user_id,)
    )
    assert up.id
    assert not created
    assert updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.user_id,)
    )
    assert up.id
    assert not created
    assert updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=12).create_or_update(
        key_fields=(UserProfile.user_id,), update_fields=(UserProfile.user_id,)
    )
    assert up.id
    assert not created
    assert updated


@pytest.mark.asyncio
async def test_schema():
    @danio.model
    class UserProfile(User):
        id: typing.Annotated[int, danio.IntField(primary=True, type="serial")] = 0
        user_id: typing.Annotated[int, danio.IntField] = 0
        level: typing.Annotated[int, danio.IntField] = 0
        coins: typing.Annotated[int, danio.IntField] = 0

        _table_unique_keys: typing.ClassVar = (("user_id",),)
        _table_index_keys: typing.ClassVar = (
            (
                User.created_at,
                User.updated_at,
            ),
            ("level",),
        )

    m = UserProfile.get_schema() - UserProfile.schema
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    for sql in UserProfile.schema.to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
    assert not (await UserProfile.where().fetch_all())
    assert (
        len(
            await db.fetch_all(
                f"SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{UserProfile.get_table_name()}';"
            )
        )
        == 4
    )
    # abstract class

    @danio.model
    class BaseUserBackpack(User):
        user_id: typing.Annotated[int, danio.IntField] = 0
        weight: typing.Annotated[int, danio.IntField] = 0

        _table_abstracted: typing.ClassVar[bool] = True

    assert BaseUserBackpack.schema.abstracted
    assert BaseUserBackpack.schema.to_sql(type=db.type)
    # disable fields

    @danio.model
    class UserBackpack(BaseUserBackpack):
        id: int = 0
        pk: typing.Annotated[int, danio.IntField(type="serial", primary=True)] = 0

    # db name
    @danio.model
    class UserBackpack2(BaseUserBackpack):
        id: typing.Annotated[int, danio.IntField(type="serial", primary=True)] = 0
        user_id: typing.Annotated[int, danio.IntField(name="user_id2")] = 0
        weight: typing.Annotated[int, danio.IntField] = 0

    sql = UserBackpack2.schema.to_sql(type=db.type)
    assert "user_id2" in sql
    assert "weight" in sql
    for sql in UserBackpack2.schema.to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
    # from db
    m = UserBackpack2.schema - await UserBackpack2.get_db_schema(db)
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.drop_indexes
    assert not m.add_indexes
    assert await UserProfile.get_db_schema(db)
    assert not await UserBackpack.get_db_schema(db)
    # wrong index

    with pytest.raises(danio.SchemaException):

        @danio.model
        class UserBackpack3(BaseUserBackpack):
            id: typing.Annotated[int, danio.IntField(type="serial", primary=True)] = 0
            user_id: typing.Annotated[int, danio.IntField(name="user_id2")] = 0
            weight: typing.Annotated[int, danio.IntField] = 0

            _table_index_keys = (("wrong_id"),)


@pytest.mark.asyncio
async def test_migrate():
    @danio.model
    class UserProfile(User):
        id: typing.Annotated[int, danio.IntField(type="serial", primary=True)] = 0
        user_id: typing.Annotated[int, danio.IntField()] = 0
        coins: typing.Annotated[int, danio.IntField()] = 0
        level: typing.Annotated[int, danio.IntField()] = 0

        _table_unique_keys: typing.ClassVar = (("user_id",),)
        _table_index_keys: typing.ClassVar = (
            (
                User.created_at,
                User.updated_at,
            ),
            ("level",),
        )

    sqls = (UserProfile.schema - None).to_sql(type=db.type)
    sqls += (
        'ALTER TABLE user_profile ADD COLUMN "group_id" int NOT NULL;'
        "ALTER TABLE user_profile DROP COLUMN level;"
        "ALTER TABLE user_profile ALTER COLUMN user_id TYPE bigint;"
        'CREATE  INDEX "group_id_6969_idx"  on user_profile ("group_id");'
        'CREATE  INDEX "user_id_6969_idx"  on user_profile ("user_id");'
    )
    for sql in sqls.split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
    # make migration
    old_schema = await UserProfile.get_db_schema(db)
    assert old_schema
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
    assert len(migration.change_type_fields) == 1
    # migrate
    for sql in migration.to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
    m = UserProfile.schema - await UserProfile.get_db_schema(db)
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    # down migrate
    for sql in (~migration).to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")
    m = old_schema - (await UserProfile.get_db_schema(db))
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    # drop table
    for sql in (~(UserProfile.schema - None)).to_sql(type=db.type).split(";"):
        if sql:
            await db.execute(sql + ";" if sql[-1] != ";" else "")


@pytest.mark.asyncio
async def test_manage():
    @danio.model
    class UserProfile(User):
        # --------------------Danio Hints--------------------
        # TABLE NAME: user_profile
        # TABLE IS NOT MIGRATED!
        ID: typing.ClassVar[danio.Field]  # "id" serial PRIMARY KEY NOT NULL
        NAME: typing.ClassVar[danio.Field]  # "name" varchar(255)  NOT NULL
        AGE: typing.ClassVar[danio.Field]  # "age" int  NOT NULL
        CREATED_AT: typing.ClassVar[
            danio.Field
        ]  # "created_at" timestamp without time zone  NOT NULL
        UPDATED_AT: typing.ClassVar[
            danio.Field
        ]  # "updated_at" timestamp without time zone  NOT NULL
        GENDER: typing.ClassVar[danio.Field]  # "gender" int  NOT NULL
        USER_ID: typing.ClassVar[danio.Field]  # "user_id" int  NOT NULL
        COINS: typing.ClassVar[danio.Field]  # "coins" int  NOT NULL
        LEVEL: typing.ClassVar[danio.Field]  # "level" int  NOT NULL
        # --------------------Danio Hints--------------------
        id: typing.Annotated[int, danio.IntField(type="serial", primary=True)] = 0
        user_id: typing.Annotated[int, danio.IntField()] = 0
        coins: typing.Annotated[int, danio.IntField()] = 0
        level: typing.Annotated[int, danio.IntField()] = 1

    # generate all
    assert not await danio.manage.make_migration(db, [User], "./tests/migrations")
    assert await danio.manage.make_migration(db, [UserProfile], "./tests/migrations")
    # get models
    await danio.manage.write_model_hints(db, UserProfile)
    for m in danio.manage.get_models(["tests.test_postgres"]):
        await danio.manage.write_model_hints(db, m)
        await danio.manage.show_model_define(db, m.schema.name)
