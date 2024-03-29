# Danio

<p>
<a href="https://github.com/strongbugman/danio/actions">
    <img src="https://github.com/strongbugman/danio/workflows/UnitTest/badge.svg" alt="UnitTest">
</a>
<a href="https://pypi.org/project/danio/">
    <img src="https://badge.fury.io/py/danio.svg" alt="Package version">
</a>
<a href="https://codecov.io/gh/strongbugman/danio">
    <img src="https://codecov.io/gh/strongbugman/danio/branch/main/graph/badge.svg" alt="Code coverage">
</a>
</p>


Danio is a ORM for python asyncio world.It is designed to make getting easy and clearly.It builds on python's dataclass and encode's [databases](https://github.com/encode/databases)

## Features

* keep OOM in mind, custom your Field and Model behavior easily
* type hints any where, no more need to memorize words your field names any more
* base CRUD operation, transactions, lock and so on
* signals like before save, after save and so on
* complex operation like bulk create, upsert, create or update and so on
* assist model schema migration
* support MySQL/PostgreSQL/SQLite
* hints generation

## install

`pip install danio`

## Documents

[Danio Document](https://strongbugman.github.io/danio/)

## Glance

```python
db = danio.Database(
    "mysql://root:letmein@server:3306/test",
    maxsize=3,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
)

@dataclasses.dataclass
class User(danio.Model):
    # auto generated by danio:
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


    async def before_create(self, validate=True):
        await super().before_create(validate=True)

    async def before_update(self, validate=True):
        self.updated_at = datetime.datetime.now()
        await super().before_update(validate=True)

    async def validate(self):
        await super().validate()
        if not self.name:
            raise danio.ValidateException("Empty name!")

    @classmethod
    def get_database(
        cls, operation: danio.Operation, table: str, *args, **kwargs
    ) -> danio.Database:
        return db

# base CRUD
user = await User(name="batman").save()
user = await User.where(User.NAME == "batman").fetch_one()
user.gender = User.Gender.MALE
await user.save()
await user.delete()
# sql chain
await User.where(User.NAME != "").limit(10).fetch_all()
# multi where condition
await User.where(User.ID != 1, User.NAME != "").fetch_all()
await User.where(User.ID != 1).where(User.NAME != "").fetch_all()
await User.where(User.ID <= 10, User.ID >= 20, is_and=False).fetch_all()
# complicated expression
await User.where(User.ID == 1).update(age=(User.AGE + 1) / (User.AGE / 12) - 2)
await User.where((User.AGE + 1) == 3).fetch_all()
# complicated sql operation
await User.where(User.ID == u.id).update(
    age=User.AGE.case(User.AGE > 10, 1, default=18).case(User.AGE <= 0, 10)
)
created, updated = await UserProfile.upsert(
    [
        dict(id=1, name="upsert"),
    ],
    update_fields=["name"],
)
# bulk operation
await User.bulk_create([User(name=f"user_{i}") for i in range(10)])
await User.bulk_update(await User.fetch_all())
await User.bulk_delete(await User.fetch_all())
# shortcut
user, created = await User(id=1, name="created?").get_or_create(
    key_fields=(User.ID,)
)
user, created, updated = await User(id=2, name="updated?").create_or_update(
    key_fields=(User.ID,)
)
```