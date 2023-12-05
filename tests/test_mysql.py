import asyncio
import dataclasses
import datetime
import decimal
import enum
import glob
import os
import typing

import pytest
import pytest_asyncio

import danio

db = danio.Database(
    f"mysql://root:{os.getenv('MYSQL_PASSWORD', 'letmein')}@{os.getenv('MYSQL_HOST', 'mysql')}:3306/",
    maxsize=2,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
    echo=True,
)
read_db = danio.Database(
    f"mysql://root:{os.getenv('MYSQL_PASSWORD', 'letmein')}@{os.getenv('MYSQL_HOST', 'mysql')}:3306/",
    maxsize=2,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
    echo=True,
)
db_name = "test_danio"


@danio.model
class BaseModel(danio.Model):
    _table_abstracted = True

    @classmethod
    def get_database(cls, operation: danio.Operation, *_, **__) -> danio.Database:
        if operation == danio.Operation.READ:
            return read_db
        else:
            return db


user_count = 0


@danio.model
class UserProfile(BaseModel):
    # --------------------Danio Hints--------------------
    # TABLE NAME: user_profile
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]  # `id` int NOT NULL AUTO_INCREMENT COMMENT ''
    USER_ID: typing.ClassVar[danio.Field]  # `user_id` int NOT NULL  COMMENT ''
    LEVEL: typing.ClassVar[danio.Field]  # `level` int NOT NULL  COMMENT ''
    # TABLE UNIQUE INDEX: user_id_662_uiq(user_id)
    # --------------------Danio Hints--------------------
    user_id: typing.Annotated[int, danio.IntField] = 0
    level: typing.Annotated[int, danio.IntField] = 0

    @classmethod
    def get_table_unique_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return ((cls.USER_ID,),)

    @classmethod
    def get_database(
        cls, operation: danio.Operation, *args, **kwargs
    ) -> danio.Database:
        if operation == danio.Operation.READ:
            return read_db
        else:
            return db


@danio.model
class Pet(BaseModel):
    # --------------------Danio Hints--------------------
    # TABLE NAME: pet
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]  # `id` int NOT NULL AUTO_INCREMENT COMMENT ''
    USER_ID: typing.ClassVar[danio.Field]  # `user_id` int NOT NULL  COMMENT ''
    NAME: typing.ClassVar[danio.Field]  # `name` varchar(255) NOT NULL  COMMENT ''
    # TABLE INDEX: user_id_8460_idx(user_id)
    # --------------------Danio Hints--------------------
    user_id: typing.Annotated[int, danio.IntField] = 0
    name: typing.Annotated[str, danio.CharField()] = ""

    @classmethod
    def get_table_index_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return ((cls.USER_ID,),)


@danio.model
class Group(BaseModel):
    # --------------------Danio Hints--------------------
    # TABLE NAME: group
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]  # `id` int NOT NULL AUTO_INCREMENT COMMENT ''
    NAME: typing.ClassVar[danio.Field]  # `name` varchar(255) NOT NULL  COMMENT ''
    # --------------------Danio Hints--------------------
    name: typing.Annotated[str, danio.CharField()] = ""


@danio.model
class UserGroup(BaseModel):
    # --------------------Danio Hints--------------------
    # TABLE NAME: user_group
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]  # `id` int NOT NULL AUTO_INCREMENT COMMENT ''
    USER_ID: typing.ClassVar[danio.Field]  # `user_id` bigint NOT NULL  COMMENT ''
    GROUP_ID: typing.ClassVar[danio.Field]  # `group_id` bigint NOT NULL  COMMENT ''
    # TABLE UNIQUE INDEX: group_id_user_i_8446_uiq(group_id,user_id)
    # --------------------Danio Hints--------------------
    user_id: typing.Annotated[int, danio.BigIntField()] = 0
    group_id: typing.Annotated[int, danio.BigIntField()] = 0

    @classmethod
    def get_table_unique_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return ((cls.GROUP_ID, cls.USER_ID),)


@danio.model
class User(BaseModel):
    # --------------------Danio Hints--------------------
    # TABLE NAME: user
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]  # `id` int NOT NULL AUTO_INCREMENT COMMENT ''
    NAME: typing.ClassVar[
        danio.Field
    ]  # `name` varchar(255) NOT NULL  COMMENT 'User name'
    AGE: typing.ClassVar[danio.Field]  # `age` int NOT NULL  COMMENT ''
    CREATED_AT: typing.ClassVar[
        danio.Field
    ]  # `created_at` datetime NOT NULL  COMMENT 'when created'
    UPDATED_AT: typing.ClassVar[
        danio.Field
    ]  # `updated_at` datetime NOT NULL  COMMENT 'when updated'
    GENDER: typing.ClassVar[danio.Field]  # `gender` int NOT NULL  COMMENT ''
    # TABLE INDEX: created_at_4846_idx(created_at)
    # TABLE INDEX: updated_at_3648_idx(updated_at)
    # --------------------Danio Hints--------------------

    class Gender(enum.Enum):
        MALE = 0
        FEMALE = 1
        OTHER = 2

    name: typing.Annotated[str, danio.CharField(comment="User name")] = ""
    age: typing.Annotated[int, danio.IntField()] = 0
    created_at: typing.Annotated[
        datetime.datetime, danio.DateTimeField(comment="when created")
    ] = dataclasses.field(default_factory=datetime.datetime.now)
    updated_at: typing.Annotated[
        datetime.datetime, danio.DateTimeField(comment="when updated")
    ] = dataclasses.field(default_factory=datetime.datetime.now)
    gender: typing.Annotated[Gender, danio.IntField(enum=Gender)] = Gender.MALE
    # relationship
    user_profile: typing.Annotated[
        UserProfile,
        danio.id_to_one(UserProfile, UserProfile.USER_ID),
    ] = dataclasses.field(default_factory=UserProfile)
    pets: typing.Annotated[
        typing.List[Pet],
        danio.id_to_many(Pet, Pet.USER_ID, auto=True),
    ] = dataclasses.field(default_factory=list)
    groups: typing.Annotated[
        typing.List[Group],
        danio.RelationField["User"](
            lambda user: Group.where(
                Group.ID.contains(
                    UserGroup.where(UserGroup.USER_ID == user.id).select(
                        UserGroup.GROUP_ID
                    )
                )
            ).fetch_all()
        ),
    ] = dataclasses.field(default_factory=list)

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
    def get_table_index_keys(cls) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
        return ((cls.CREATED_AT,), (cls.UPDATED_AT,))


@pytest_asyncio.fixture(autouse=True)
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
        await db.execute(User.schema.to_sql())
        await db.execute(UserProfile.schema.to_sql())
        await db.execute(Pet.schema.to_sql())
        await db.execute(Group.schema.to_sql())
        await db.execute(UserGroup.schema.to_sql())
        await read_db.execute(f"USE `{db_name}`;")
        await danio.manage.init(db, ["tests.test_mysql"])
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
    assert await User.where().limit(1).order_by(User.NAME, asc=False).fetch_one()
    assert (
        await User.where().limit(1).order_by(User.NAME, User.ID, asc=False).fetch_one()
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
        .order_by(User.AGE + User.GENDER, asc=False)
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
    assert await User.where(User.ID == -1).fetch_count() == 0
    assert user_count == 11
    # row data
    assert await User.where().fetch_row()
    # save with special fields only
    u = await User.where().fetch_one()
    assert u
    u.name = "tester"
    u.gender = u.Gender.OTHER
    u = await u.save(fields=[User.NAME])
    nu = await User.where(User.ID == u.id).fetch_one()
    assert nu
    assert nu.name == "tester"
    assert nu.gender == User.Gender.MALE
    assert user_count == 11
    # save exclude special fields
    u = await User.where().fetch_one()
    assert u
    u.name = "tester"
    u.gender = u.Gender.OTHER
    u = await u.save(ignore_fields=[User.GENDER])
    nu = await User.where(User.ID == u.id).fetch_one()
    assert nu
    assert nu.name == "tester"
    assert nu.gender == User.Gender.MALE
    assert user_count == 11
    # save with wrong field
    u = await User.where().fetch_one()
    assert u
    u.gender = 10  # type: ignore
    with pytest.raises(danio.ValidateException):
        await u.save()
    # update
    u = await User.where().fetch_one()
    assert u
    u.name = "admin_user"
    await u.save()
    assert u.name == "admin_user"
    await User.where(User.ID == u.id).update(name=User.NAME.to_database("admin_user2"))
    assert (await User.where().must_fetch_one()).name == "admin_user2"
    # read
    u = (await User.where(User.ID == u.id).fetch_all())[0]
    assert u.name == "admin_user2"
    # read only special field
    u = await User.where().must_fetch_one(fields=(User.NAME,))
    assert not u.id
    assert u.name
    # read exclude special field
    u = await User.where().must_fetch_one(ignore_fields=(User.ID,))
    assert not u.id
    assert u.name
    # refetch
    u = await User.where().must_fetch_one()
    await User.where(User.ID == u.ID).update(name="user1")
    await u.refetch()
    assert u.name == "user1"
    # delete
    await u.delete()
    assert not await User.where(User.ID == u.id).fetch_all()
    u = await User.where().must_fetch_one()
    await User.where(User.ID == u.id).delete()
    assert not await User.where(User.ID == u.id).fetch_all()
    # create with id
    u = User(id=101, name="test_user")
    await u.save(force_insert=True)
    u = (await User.where(User.ID == u.id).fetch_all())[0]
    assert u.name == "test_user"
    # multi where condition
    assert await User.where(
        ((User.ID != 1) | (User.NAME != "")) & (User.gender == User.Gender.MALE)
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
    assert (await User.where().fetch_all(fields=[User.ID]))[0].name == User.name
    # combine condition
    u = await User.where().must_fetch_one()
    u.age = 2
    await u.save()
    assert await User.where((User.AGE + 1) == 3).fetch_all()
    # delete many
    await User.where(User.ID >= 1).delete()
    assert not await User.where().fetch_all()
    # transaction
    db = User.get_database(danio.Operation.UPDATE, User.table_name)
    async with db.transaction():
        for u in await User.where(database=db).fetch_all():
            u.name += "_updated"
            await u.save(fields=[User.NAME], database=db)
    # exclusive lock
    async with db.transaction():
        for u in await User.where(database=db).for_update().fetch_all():
            u.name += "_updated"
            await u.save(fields=[User.NAME], database=db)
    # share lock
    async with db.transaction():
        for u in await User.where(database=db).for_share().fetch_all():
            u.name += "_updated"
            await u.save(fields=[User.NAME], database=db)
    # use index
    await User.where().use_index([list(User.schema.indexes)[0].name]).fetch_all()
    await (
        User.where()
        .use_index(
            [list(User.schema.indexes)[0].name, list(User.schema.indexes)[0].name]
        )
        .fetch_all()
    )
    await User.where().ignore_index([list(User.schema.indexes)[0].name]).fetch_all()
    await User.where().force_index([list(User.schema.indexes)[0].name]).fetch_all()
    await (
        User.where()
        .force_index([list(User.schema.indexes)[0].name], _for="FOR ORDER BY")
        .order_by(User.CREATED_AT)
        .fetch_all()
    )
    # upsert

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
    await User.where(User.ID == u.id).update(age=User.AGE + 1)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == u.age + 1
    # *1
    await User.where(User.ID == u.id).update(age=User.AGE * 1)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == u.age + 1
    # /1
    await User.where(User.ID == u.id).update(age=User.AGE / 1)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == u.age + 1
    # -1
    await User.where(User.ID == u.id).update(age=User.AGE - 1)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == u.age
    # +self
    u.age = 1
    await u.save()
    await User.where(User.ID == u.id).update(age=User.AGE + User.AGE)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == u.age * 2
    # -self
    await User.where(User.ID == u.id).update(age=User.AGE - User.AGE)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 0
    # combine
    await User.where(User.ID == u.id).update(age=User.AGE - 1 + 10)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 9
    assert await User.where((User.ID + 1) > u.id).must_fetch_one()
    assert await User.where((User.ID + 0) >= u.id).must_fetch_one()
    assert await User.where((User.ID - 1) < u.id).must_fetch_one()
    assert await User.where((User.ID - 0) <= u.id).must_fetch_one()
    assert await User.where((User.ID - 1) != u.id).must_fetch_one()
    # multi express
    await User.where(User.ID == u.id).update(age=User.AGE + 1 + (User.AGE / 9))
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 11
    await User.where(User.ID == u.id).update(age=User.AGE + 1 - (User.AGE / 11))
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 11
    await User.where(User.ID == u.id).update(age=(User.AGE + 1) * (User.AGE / 11))
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 12
    await User.where(User.ID == u.id).update(age=(User.AGE + 1) / (User.AGE / 12) - 2)
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 11
    # case
    await User.where(User.ID == u.id).update(
        age=User.AGE.case(User.AGE > 10, 1).case(User.AGE < 10, 10)
    )
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 1
    # case default
    await User.where(User.ID == u.id).update(
        age=User.AGE.case(User.AGE > 10, 1, default=18).case(User.AGE <= 0, 10)
    )
    assert (await User.where(User.ID == u.id).must_fetch_one()).age == 18


@pytest.mark.asyncio
async def test_field():
    @danio.model
    class Table(danio.Model):
        fnull: typing.Annotated[
            typing.Optional[str], danio.CharField(not_null=False)
        ] = None
        fsint: typing.Annotated[int, danio.SmallIntField] = 0
        fint: typing.Annotated[int, danio.IntField] = 0
        fbint: typing.Annotated[int, danio.BigIntField] = 0
        ftint: typing.Annotated[int, danio.TinyIntField] = 0
        fbool: typing.Annotated[bool, danio.BoolField] = False
        ffloat: typing.Annotated[float, danio.FloatField] = 0
        fdecimal: typing.Annotated[
            decimal.Decimal, danio.DecimalField
        ] = decimal.Decimal(0)
        fchar: typing.Annotated[str, danio.CharField] = ""
        fbytes: typing.Annotated[bytes, danio.BlobField] = b""
        ftext: typing.Annotated[str, danio.TextField] = ""
        ftime: typing.Annotated[
            datetime.timedelta, danio.TimeField
        ] = datetime.timedelta()
        fdate: typing.Annotated[datetime.date, danio.DateField] = dataclasses.field(
            default_factory=lambda: datetime.datetime.now().date()
        )
        fdatetime: typing.Annotated[
            datetime.datetime, danio.DateTimeField
        ] = dataclasses.field(default_factory=datetime.datetime.now)
        fjson1: typing.Annotated[typing.List[int], danio.JsonField] = dataclasses.field(
            default_factory=list
        )
        fjson2: typing.Annotated[
            typing.Dict[str, int], danio.JsonField
        ] = dataclasses.field(default_factory=dict)
        fjson3: typing.Annotated[
            typing.Dict[str, int], danio.JsonField(type="json")
        ] = dataclasses.field(default_factory=dict)

        @classmethod
        def get_database(cls, *args, **kwargs) -> danio.Database:
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
    assert t.fbytes == b""
    assert t.ftime == datetime.timedelta(0)
    assert t.fdate
    assert t.fdatetime
    assert t.fjson1 == []
    assert t.fjson2 == {}
    assert t.fjson3 == {}
    assert t.fnull is None
    await t.save()
    # read
    t = await Table.where().fetch_one()
    assert t
    assert t.fint == 0
    assert t.ftint == 0
    assert not t.fbool
    assert t.ffloat == 0
    assert t.fdecimal == decimal.Decimal()
    assert t.fchar == ""
    assert t.ftext == ""
    assert t.fbytes == b""
    assert t.ftime == datetime.timedelta(0)
    assert t.fdate
    assert t.fdatetime
    assert t.fjson1 == []
    assert t.fjson2 == {}
    assert t.fjson3 == {}
    assert t.fnull is None
    # update
    t.fint = 1
    t.fsint = 1
    t.fbint = 1
    t.ftint = 1
    t.fbool = True
    t.ffloat = 2.123456
    t.fdecimal = decimal.Decimal("2.12")
    t.fchar = "hello"
    t.ftext = "long story"
    t.fbytes = b"long long bytes"
    t.ftime = datetime.timedelta(hours=11, seconds=11)
    t.fdate = datetime.date.fromtimestamp(24 * 60 * 60)
    t.fdatetime = datetime.datetime.fromtimestamp(24 * 60 * 60)
    t.fjson1.extend([1, 2, 3])
    t.fjson2.update(x=3, y=4, z=5)
    t.fjson3.update(x=3, y=4, z=5)
    t.fnull = "hello"
    await t.save()
    # read
    t = await Table.where().fetch_one()
    assert t
    assert t.fint == 1
    assert t.fsint == 1
    assert t.fbint == 1
    assert t.ftint == 1
    assert t.fbool
    assert t.ffloat == 2.12346
    assert t.fdecimal == decimal.Decimal("2.12")
    assert t.fchar == "hello"
    assert t.ftext == "long story"
    assert t.fbytes == b"long long bytes"
    assert str(t.ftime) == "11:00:11"
    assert t.fdate == datetime.date.fromtimestamp(24 * 60 * 60)
    assert t.fdatetime == datetime.datetime.fromtimestamp(24 * 60 * 60)
    assert t.fjson1 == [1, 2, 3]
    assert t.fjson2 == {"x": 3, "y": 4, "z": 5}
    assert t.fjson3 == {"x": 3, "y": 4, "z": 5}
    assert t.fnull == "hello"


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
    await User.bulk_update(users, fields=(User.NAME,))
    for user in await User.where().fetch_all():
        assert user.name.endswith(f"_updated_{user.id}")
        assert user.gender == User.GENDER.default
    # delete
    await User.bulk_delete(users)
    assert not await User.where().fetch_all()


@pytest.mark.asyncio
async def test_combo_operations():
    # get or create
    up, created = await UserProfile(user_id=1, level=10).get_or_create(
        key_fields=(UserProfile.USER_ID,)
    )
    assert up.id
    assert created
    # --
    up, created = await UserProfile(user_id=1, level=11).get_or_create(
        key_fields=(UserProfile.USER_ID,)
    )
    assert up.id
    assert not created
    assert up.level == 10
    # create or update
    up, created, updated = await UserProfile(user_id=2, level=10).create_or_update(
        key_fields=(UserProfile.USER_ID,)
    )
    assert up.id
    assert created
    assert not updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.USER_ID,)
    )
    assert up.id
    assert not created
    assert updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=11).create_or_update(
        key_fields=(UserProfile.USER_ID,)
    )
    assert up.id
    assert not created
    assert not updated
    # --
    up, created, updated = await UserProfile(user_id=2, level=12).create_or_update(
        key_fields=(UserProfile.USER_ID,), update_fields=(UserProfile.USER_ID,)
    )
    assert up.id
    assert not created
    assert not updated


@pytest.mark.asyncio
async def test_relation():
    await User(name="test").save()
    u = await User().where().must_fetch_one()
    g = await Group().save()
    assert not any((u.user_profile.id > 0, u.pets, u.groups))
    await UserProfile(user_id=u.id).save()
    await Pet(user_id=u.id).save()
    await UserGroup(user_id=u.id, group_id=g.id).save()
    await u.load_relations()
    assert u.user_profile.id
    assert u.pets
    assert u.groups


@pytest.mark.asyncio
async def test_schema():
    @danio.model
    class UserProfile(User):
        user_id: typing.Annotated[int, danio.IntField(comment="user id")] = 0
        level: typing.Annotated[int, danio.IntField(comment="user level")] = 0
        coins: typing.Annotated[int, danio.IntField(comment="user coins")] = 0

        _table_unique_keys: typing.ClassVar = (("user_id",),)

        @classmethod
        def get_table_name(cls) -> str:
            return "test_user_profile"

        @classmethod
        def get_table_index_keys(
            cls,
        ) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
            return (
                (
                    User.created_at,
                    User.updated_at,
                ),
                ("level",),
            )

    # assert danio.Schema.from_model(UserProfile) == UserProfile.schema
    await db.execute(UserProfile.schema.to_sql())
    assert not (await UserProfile.where().fetch_all())
    assert (
        len(await db.fetch_all(f"SHOW INDEX FROM {UserProfile.get_table_name()}")) == 5
    )
    # abstract class

    @danio.model
    class BaseUserBackpack(User):
        user_id: typing.Annotated[int, danio.IntField] = 0
        weight: typing.Annotated[int, danio.IntField] = 0

        _table_abstracted: typing.ClassVar[bool] = True

    assert BaseUserBackpack.schema.abstracted
    assert BaseUserBackpack.schema.to_sql()
    # disable fields

    @danio.model
    class UserBackpack(BaseUserBackpack):
        id: int = 0
        pk: typing.Annotated[int, danio.IntField(auto_increment=True, primary=True)] = 0

    # db name
    @danio.model
    class UserBackpack2(BaseUserBackpack):
        user_id: typing.Annotated[int, danio.IntField(name="user_id2")] = 0
        weight: typing.Annotated[int, danio.IntField] = 0

    sql = UserBackpack2.schema.to_sql()
    assert "user_id2" in sql
    assert "weight" in sql
    await db.execute(UserBackpack2.schema.to_sql())
    # from db
    assert UserBackpack2.schema == await UserBackpack2.get_db_schema(db)
    assert await UserProfile.get_db_schema(db)
    assert not await UserBackpack.get_db_schema(db)
    # wrong index

    with pytest.raises(danio.SchemaException):

        @danio.model
        class UserBackpack3(BaseUserBackpack):
            user_id: typing.Annotated[int, danio.IntField(name="user_id2")] = 0
            weight: typing.Annotated[int, danio.IntField] = 0

            @classmethod
            def get_table_index_keys(
                cls,
            ) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
                return (("wrong id",),)


@pytest.mark.asyncio
async def test_migrate():
    @danio.model
    class UserProfile2(User):
        user_id: typing.Annotated[int, danio.IntField(comment="user id")] = 0
        level: typing.Annotated[int, danio.IntField(comment="user level")] = 1
        coins: typing.Annotated[int, danio.IntField(comment="user coins")] = 0

        _table_name_snake_case: typing.ClassVar[bool] = True
        _table_unique_keys: typing.ClassVar = (("user_id",),)

        @classmethod
        def get_table_index_keys(
            cls,
        ) -> typing.Tuple[typing.Tuple[typing.Any, ...], ...]:
            return (
                (
                    User.created_at,
                    User.updated_at,
                ),
                ("level",),
            )

    await db.execute((UserProfile2.schema - None).to_sql())
    await db.execute(
        "ALTER TABLE user_profile2 ADD COLUMN `group_id` int(10) NOT NULL COMMENT 'User group';"
        "ALTER TABLE user_profile2 DROP COLUMN level;"
        "ALTER TABLE user_profile2 MODIFY user_id bigint(10);"
        "CREATE  INDEX `group_id_6969_idx`  on user_profile2 (`group_id`);"
        "CREATE  INDEX `user_id_6969_idx`  on user_profile2 (`user_id`);"
    )
    # make migration
    old_schema = await UserProfile2.get_db_schema(db)
    migration: danio.Migration = UserProfile2.schema - old_schema
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
    await db.execute(migration.to_sql())
    m = (await UserProfile2.get_db_schema(db)) - UserProfile2.schema  # type: ignore
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    # down migrate
    await db.execute((~migration).to_sql())
    m = await UserProfile2.get_db_schema(db) - old_schema  # type: ignore
    assert not m.add_fields
    assert not m.drop_fields
    assert not m.change_type_fields
    assert not m.add_indexes
    assert not m.drop_indexes
    # drop table
    await db.execute((~(UserProfile2.schema - None)).to_sql())


@pytest.mark.asyncio
async def test_manage():
    @danio.model
    class UserProfile2(User):
        # --------------------Danio Hints--------------------
        # TABLE NAME: user_profile2
        # TABLE IS NOT MIGRATED!
        ID: typing.ClassVar[danio.Field]  # `id` int NOT NULL AUTO_INCREMENT COMMENT ''
        NAME: typing.ClassVar[
            danio.Field
        ]  # `name` varchar(255) NOT NULL  COMMENT 'User name'
        AGE: typing.ClassVar[danio.Field]  # `age` int NOT NULL  COMMENT ''
        CREATED_AT: typing.ClassVar[
            danio.Field
        ]  # `created_at` datetime NOT NULL  COMMENT 'when created'
        UPDATED_AT: typing.ClassVar[
            danio.Field
        ]  # `updated_at` datetime NOT NULL  COMMENT 'when updated'
        GENDER: typing.ClassVar[danio.Field]  # `gender` int NOT NULL  COMMENT ''
        USER_ID: typing.ClassVar[
            danio.Field
        ]  # `user_id` int NOT NULL  COMMENT 'user id'
        LEVEL: typing.ClassVar[
            danio.Field
        ]  # `level` int NOT NULL  COMMENT 'user level'
        COINS: typing.ClassVar[
            danio.Field
        ]  # `coins` int NOT NULL  COMMENT 'user coins'
        # TABLE INDEX: (created_at)
        # TABLE INDEX: (updated_at)
        # --------------------Danio Hints--------------------
        user_id: typing.Annotated[int, danio.IntField(comment="user id")] = 0
        level: typing.Annotated[int, danio.IntField(comment="user level")] = 1
        coins: typing.Annotated[int, danio.IntField(comment="user coins")] = 0

    # generate all
    assert not await danio.manage.make_migration(db, [User], "./tests/migrations")
    assert await danio.manage.make_migration(db, [UserProfile2], "./tests/migrations")
    # with out db connection
    assert await danio.manage.make_migration(
        danio.Database("mysql://no_connected:3306/"),
        [UserProfile2],
        "./tests/migrations",
    )
    # get models
    await danio.manage.write_model_hints(db, UserProfile2)
    for m in danio.manage.get_models(["tests.test_mysql"]):
        await danio.manage.write_model_hints(db, m)
        await danio.manage.show_model_define(db, m.schema.name)
