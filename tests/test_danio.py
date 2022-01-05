import asyncio
import dataclasses
import datetime
import decimal
import enum
import glob
import os
import typing

import pytest

import danio
from danio import Database, SchemaException, ValidateException, manage, model

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


user_count = 0


@dataclasses.dataclass
class User(model.Model):
    class Gender(enum.Enum):
        MALE = 0
        FEMALE = 1
        OTHER = 2

    name: str = model.field(
        field_cls=model.CharField,
        comment="User name",
        default=model.CharField.NoDefault,
    )
    age: int = model.field(field_cls=model.IntField)
    created_at: datetime.datetime = model.field(
        field_cls=model.DateTimeField,
        comment="when created",
    )
    updated_at: datetime.datetime = model.field(
        field_cls=model.DateTimeField,
        comment="when created",
    )
    gender: Gender = model.field(field_cls=model.IntField, enum=Gender)

    async def before_create(self, **kwargs):
        global user_count
        user_count += 1
        await super().before_create(**kwargs)

    async def before_save(self, **kwargs):
        self.updated_at = datetime.datetime.utcnow()
        if self.created_at.ctime() == "Thu Jan  1 00:00:00 1970":
            self.created_at = self.updated_at
        await super().before_save(**kwargs)

    async def validate(self):
        await super().validate()
        if not self.name:
            raise ValidateException("Empty name!")

    @classmethod
    def get_database(
        cls, operation: model.Model.Operation, table: str, *args, **kwargs
    ) -> Database:
        if operation == model.Model.Operation.READ:
            return read_db
        else:
            return db


@pytest.fixture(autouse=True)
async def database():
    await db.connect()
    await read_db.connect()
    if not os.path.exists(os.path.join("tests", "migrations")):
        os.mkdir(os.path.join("tests", "migrations"))
    try:
        await db.execute(
            f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
        )
        await db.execute(f"USE `{db_name}`;")
        await db.execute(model.Schema.from_model(User).to_sql())
        await read_db.execute(f"USE `{db_name}`;")
        yield db
    finally:
        await db.execute(f"DROP DATABASE {db_name};")
        await db.disconnect()
        await read_db.disconnect()
        for f in glob.glob("./tests/migrations/*.sql"):
            os.remove(f)


@pytest.mark.asyncio
async def test_database():
    results = await db.fetch_all("SHOW DATABASES;")
    assert results


@pytest.mark.asyncio
async def test_sql():
    # create
    u = User(name="test_user")
    await asyncio.sleep(0.1)
    await u.save()
    assert u.updated_at >= u.created_at
    assert u.id > 0
    assert u.gender is u.Gender.MALE
    assert u.table_name == User.table_name
    assert user_count == 1
    with pytest.raises(ValidateException):
        await User().save()
    # read
    assert await User.get(User.id == u.id)
    assert await User.where(User.id == u.id).fetch_one()
    assert await User.where(row=f"id = {u.id}").fetch_one()
    # read with limit
    assert await User.select(User.id == u.id, limit=1)
    assert await User.where(User.id == u.id).limit(1).fetch_all()
    # read with order by
    assert (await User.select(limit=1, order_by=User.name, order_by_asc=False))[0]
    assert await User.where().limit(1).order_by(User.name, asc=False).fetch_one()
    assert (
        await User.where()
        .limit(1)
        .order_by(User.age + User.gender, asc=False)
        .fetch_one()
    )
    # read with page
    for _ in range(10):
        await User(name="test_users").save()
    assert await User.get(offset=10)
    assert not await User.get(offset=11)
    assert await User.where().limit(1).offset(10).fetch_one()
    assert not await User.where().limit(1).offset(11).fetch_one()
    # count
    assert (await User.count()) == 11
    assert await User.where().fetch_count() == 11
    assert await User.where(User.id == -1).fetch_count() == 0
    assert user_count == 11
    # save with special fields only
    u = await User.get()
    u.name = "tester"
    u.gender = u.Gender.OTHER
    u = await u.save(fields=[User.name])
    nu = await User.get(User.id == u.id)
    assert nu.name == "tester"
    assert nu.gender == User.Gender.MALE
    assert user_count == 11
    # save with wrong field
    u = await User.get()
    u.gender = 10
    with pytest.raises(ValidateException):
        await u.save()
    # update
    u = await User.get()
    u.name = "admin_user"
    await u.save()
    assert u.name == "admin_user"
    await User.update_many(User.id == u.id, name=User.name.to_database("admin_user2"))
    assert (await User.get()).name == "admin_user2"
    # read
    u = (await User.select(User.id == u.id))[0]
    assert u.name == "admin_user2"
    # delete
    await u.delete()
    assert not await User.select(User.id == u.id)
    u = await User.get()
    await User.where(User.id == u.id).delete()
    assert not await User.select(User.id == u.id)
    # create with id
    u = User(id=101, name="test_user")
    await u.save(force_insert=True)
    u = (await User.select(User.id == u.id))[0]
    assert u.name == "test_user"
    # multi where condition
    assert await User.select(
        ((User.id != 1) | (User.name != "")) & (User.gender == User.Gender.MALE)
    )
    assert await User.where(User.id != 1, User.name != "", is_and=False).fetch_all()
    assert (
        not await User.where(User.id != 1, User.name != "", is_and=False)
        .where(User.gender == User.Gender.FEMALE)
        .fetch_all()
    )
    assert await User.select(User.name.like("test_%"))
    assert await User.select(User.gender.contains([g.value for g in User.Gender]))
    assert (await User.select(fields=[User.id]))[0].name == User.name
    # combine condition
    u = await User.get()
    u.age = 2
    await u.save()
    assert await User.select((User.age + 1) == 3)
    # delete many
    await User.delete_many(User.id >= 1)
    assert not await User.select()
    # transation
    db = User.get_database(User.Operation.UPDATE, User.table_name)
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

    @dataclasses.dataclass
    class UserProfile(danio.Model):
        user_id: int = model.field(model.IntField)
        level: int = model.field(model.IntField)

        _table_unique_keys = ((user_id,),)

        @classmethod
        def get_database(
            cls, operation: model.Model.Operation, table: str, *args, **kwargs
        ) -> Database:
            if operation == model.Model.Operation.READ:
                return read_db
            else:
                return db

    await db.execute(UserProfile.schema.to_sql())
    created, updated = await UserProfile.upsert(
        [
            dict(user_id=1, level=10),
        ],
        update_fields=["level"],
    )
    assert created
    assert not updated
    # --
    created, updated = await UserProfile.upsert(
        [
            dict(user_id=1, level=11),
        ],
        update_fields=["level"],
    )
    assert not created
    assert updated
    # --
    created, updated = await UserProfile.upsert(
        [
            dict(user_id=1, level=11),
        ],
        update_fields=["level"],
    )
    assert not created
    assert not updated


@pytest.mark.asyncio
async def test_complicated_update():
    # +1
    u = await User(name="rails").save()
    await User.where(User.id == u.id).update(age=User.age + 1)
    assert (await User.get(User.id == u.id)).age == u.age + 1
    # *1
    await User.where(User.id == u.id).update(age=User.age * 1)
    assert (await User.get(User.id == u.id)).age == u.age + 1
    # /1
    await User.where(User.id == u.id).update(age=User.age / 1)
    assert (await User.get(User.id == u.id)).age == u.age + 1
    # -1
    await User.where(User.id == u.id).update(age=User.age - 1)
    assert (await User.get(User.id == u.id)).age == u.age
    # +self
    u.age = 1
    await u.save()
    await User.where(User.id == u.id).update(age=User.age + User.age)
    assert (await User.get(User.id == u.id)).age == u.age * 2
    # -self
    await User.where(User.id == u.id).update(age=User.age - User.age)
    assert (await User.get(User.id == u.id)).age == 0
    # combine
    await User.where(User.id == u.id).update(age=User.age - 1 + 10)
    assert (await User.get(User.id == u.id)).age == 9
    assert await User.where((User.id + 1) > u.id).fetch_one()
    assert await User.where((User.id + 0) >= u.id).fetch_one()
    assert await User.where((User.id - 1) < u.id).fetch_one()
    assert await User.where((User.id - 0) <= u.id).fetch_one()
    assert await User.where((User.id - 1) != u.id).fetch_one()
    # multi express
    await User.where(User.id == u.id).update(age=User.age + 1 + (User.age / 9))
    assert (await User.get(User.id == u.id)).age == 11
    await User.where(User.id == u.id).update(age=User.age + 1 - (User.age / 11))
    assert (await User.get(User.id == u.id)).age == 11
    await User.where(User.id == u.id).update(age=(User.age + 1) * (User.age / 11))
    assert (await User.get(User.id == u.id)).age == 12
    await User.where(User.id == u.id).update(age=(User.age + 1) / (User.age / 12) - 2)
    assert (await User.get(User.id == u.id)).age == 11
    # case
    await User.where(User.id == u.id).update(
        age=User.age.case(User.age > 10, 1).case(User.age < 10, 10)
    )
    assert (await User.get(User.id == u.id)).age == 1
    # case default
    await User.where(User.id == u.id).update(
        age=User.age.case(User.age > 10, 1, default=18).case(User.age <= 0, 10)
    )
    assert (await User.get(User.id == u.id)).age == 18


@pytest.mark.asyncio
async def test_field():
    @dataclasses.dataclass
    class Table(model.Model):
        fsint: int = model.field(field_cls=model.SmallIntField)
        fint: int = model.field(field_cls=model.IntField)
        fbint: int = model.field(field_cls=model.BigIntField)
        ftint: int = model.field(field_cls=model.TinyIntField)
        fbool: int = model.field(field_cls=model.BoolField)
        ffloat: int = model.field(field_cls=model.FLoatField)
        fdecimal: decimal.Decimal = model.field(field_cls=model.DecimalField)
        fchar: str = model.field(field_cls=model.CharField)
        ftext: str = model.field(field_cls=model.TextField)
        ftime: datetime.timedelta = model.field(field_cls=model.TimeField)
        fdate: datetime.date = model.field(field_cls=model.DateField)
        fdatetime: datetime.datetime = model.field(field_cls=model.DateTimeField)
        fjson1: typing.List[int] = model.field(field_cls=model.JsonField, default=[])
        fjson2: typing.Dict[str, int] = model.field(
            field_cls=model.JsonField, default=dict
        )

        @classmethod
        def get_database(cls, *args, **kwargs) -> Database:
            return db

    await db.execute(Table.schema.to_sql())
    # create
    t = Table()
    assert t.fsint == 0
    assert t.fbint == 0
    assert t.fint == 0
    assert t.ftint == 0
    assert not t.fbool
    assert t.ffloat == 0
    assert t.fchar == ""
    assert t.ftext == ""
    assert t.ftime == datetime.timedelta(0)
    assert t.fdate == datetime.date.fromtimestamp(0)
    assert t.fdatetime == datetime.datetime.fromtimestamp(0)
    assert t.fjson1 == []
    assert t.fjson2 == {}
    await t.save()
    # read
    t = await Table.get()
    assert t.fint == 0
    assert t.ftint == 0
    assert not t.fbool
    assert t.ffloat == 0
    assert t.fdecimal == decimal.Decimal()
    assert t.fchar == ""
    assert t.ftext == ""
    assert t.ftime == datetime.timedelta(0)
    assert t.fdate == datetime.date.fromtimestamp(0)
    assert t.fdatetime == datetime.datetime.fromtimestamp(0)
    assert t.fjson1 == []
    assert t.fjson2 == {}
    # update
    t.fint = 1
    t.fsint = 1
    t.fbint = 1
    t.ftint = 1
    t.fbool = True
    t.ffloat = 2.123456
    t.fdecimal = decimal.Decimal("2.123456")
    t.fchar = "hello"
    t.ftext = "long story"
    t.ftime = datetime.timedelta(hours=11, seconds=11)
    t.fdate = datetime.date.fromtimestamp(24 * 60 * 60)
    t.fdatetime = datetime.datetime.fromtimestamp(24 * 60 * 60)
    t.fjson1.extend([1, 2, 3])
    t.fjson2.update(x=3, y=4, z=5)
    await t.save()
    # read
    t = await Table.get()
    assert t.fint == 1
    assert t.fsint == 1
    assert t.fbint == 1
    assert t.ftint == 1
    assert t.fbool
    assert t.ffloat == 2.12346
    assert t.fdecimal == decimal.Decimal("2.12")
    assert t.fchar == "hello"
    assert t.ftext == "long story"
    assert str(t.ftime) == "11:00:11"
    assert t.fdate == datetime.date.fromtimestamp(24 * 60 * 60)
    assert t.fdatetime == datetime.datetime.fromtimestamp(24 * 60 * 60)
    assert t.fjson1 == [1, 2, 3]
    assert t.fjson2 == {"x": 3, "y": 4, "z": 5}


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
    await User.bulk_create(users)
    assert users[1].id == 34
    assert users[9].id == 42
    # update
    users = await User.select()
    for user in users:
        user.name += f"_updated_{user.id}"
    await User.bulk_update(users)
    for user in await User.select():
        assert user.name.endswith(f"_updated_{user.id}")
    # update with special fields
    users = await User.select()
    for user in users:
        user.name += f"_updated_{user.id}"
        user.gender = User.Gender.OTHER
    await User.bulk_update(users, fields=(User.name,))
    for user in await User.select():
        assert user.name.endswith(f"_updated_{user.id}")
        assert user.gender == User.gender.default
    # delete
    await User.bulk_delete(users)
    assert not await User.select()


@pytest.mark.asyncio
async def test_combo_operations():
    @dataclasses.dataclass
    class UserProfile(danio.Model):
        user_id: int = model.field(model.IntField)
        level: int = model.field(model.IntField)

        _table_unique_keys = ((user_id,),)

        @classmethod
        def get_database(
            cls, operation: model.Model.Operation, table: str, *args, **kwargs
        ) -> Database:
            if operation == model.Model.Operation.READ:
                return read_db
            else:
                return db

    await db.execute(UserProfile.schema.to_sql())
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
    # create or udpate
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
    assert not updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.user_id,),
        fields=(UserProfile.user_id, UserProfile.level),
    )
    assert up.id
    assert not created
    assert not updated


@pytest.mark.asyncio
async def test_schema():
    @dataclasses.dataclass
    class UserProfile(User):
        user_id: int = model.field(field_cls=model.IntField, comment="user id")
        level: int = model.field(field_cls=model.IntField, comment="user level")
        coins: int = model.field(field_cls=model.IntField, comment="user coins")

        _table_unique_keys: typing.ClassVar = ((user_id,),)
        _table_index_keys: typing.ClassVar = (
            (
                User.created_at,
                User.updated_at,
            ),
            ("level",),
        )

    assert model.Schema.from_model(UserProfile) == UserProfile.schema
    await db.execute(UserProfile.schema.to_sql())
    assert not (await UserProfile.select())
    assert (
        len(await db.fetch_all(f"SHOW INDEX FROM {UserProfile.get_table_name()}")) == 5
    )
    # abstract class

    @dataclasses.dataclass
    class BaseUserBackpack(User):
        user_id: int = model.field(field_cls=model.IntField)
        weight: int = model.field(field_cls=model.IntField)

        _table_abstracted: typing.ClassVar[bool] = True

    assert BaseUserBackpack.schema.abstracted
    assert BaseUserBackpack.schema.to_sql()
    # disable fields

    @dataclasses.dataclass
    class UserBackpack(BaseUserBackpack):
        id: int = 0
        pk: int = model.field(field_cls=model.IntField, auto_increment=True)

        _table_primary_key: typing.ClassVar[model.Field] = pk

    # db name
    @dataclasses.dataclass
    class UserBackpack2(BaseUserBackpack):
        user_id: int = model.field(field_cls=model.IntField, name="user_id2")
        weight: int = model.field(field_cls=model.IntField)

    sql = UserBackpack2.schema.to_sql()
    assert "user_id2" in sql
    assert "weight" in sql
    await db.execute(UserBackpack2.schema.to_sql())
    # from db
    assert UserBackpack2.schema == await model.Schema.from_db(db, UserBackpack2)
    assert await model.Schema.from_db(db, UserProfile)
    assert not await model.Schema.from_db(db, UserBackpack)
    # wrong index

    @dataclasses.dataclass
    class UserBackpack3(BaseUserBackpack):
        user_id: int = model.field(field_cls=model.IntField, name="user_id2")
        weight: int = model.field(field_cls=model.IntField)

        _table_index_keys = (("wrong_id"),)

    with pytest.raises(SchemaException):
        model.Schema.from_model(UserBackpack3)


@pytest.mark.asyncio
async def test_migrate():
    @dataclasses.dataclass
    class UserProfile(User):
        user_id: int = model.field(field_cls=model.IntField)
        level: int = model.field(field_cls=model.IntField, default=1)
        coins: int = model.field(field_cls=model.IntField)

        _table_unique_keys: typing.ClassVar = ((user_id,),)
        _table_index_keys: typing.ClassVar = (
            (
                User.created_at,
                User.updated_at,
            ),
            ("level",),
        )

    await db.execute((UserProfile.schema - None).to_sql())
    await db.execute(
        "ALTER TABLE userprofile ADD COLUMN `group_id` int(10) NOT NULL COMMENT 'User group';"
        "ALTER TABLE userprofile DROP COLUMN level;"
        "ALTER TABLE userprofile MODIFY user_id bigint(10);"
        "CREATE  INDEX `group_id_6969_idx`  on userprofile (`group_id`);"
        "CREATE  INDEX `user_id_6969_idx`  on userprofile (`user_id`);"
    )
    # make migration
    old_schema = await model.Schema.from_db(db, UserProfile)
    migration: model.Migration = UserProfile.schema - old_schema
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
    await db.execute(migration.to_sql())
    assert UserProfile.schema == await model.Schema.from_db(db, UserProfile)
    # down migrate
    await db.execute((~migration).to_sql())
    assert old_schema == await model.Schema.from_db(db, UserProfile)
    # drop table
    await db.execute((~(UserProfile.schema - None)).to_sql())


@pytest.mark.asyncio
async def test_manage():
    @dataclasses.dataclass
    class UserProfile(User):
        user_id: int = model.field(field_cls=model.IntField)
        level: int = model.field(field_cls=model.IntField, default=1)
        coins: int = model.field(field_cls=model.IntField)

    # generate all
    assert not await manage.make_migration(db, [User], "./tests/migrations")
    assert await manage.make_migration(db, [UserProfile], "./tests/migrations")
    # get models
    assert manage.get_models(["tests"])
