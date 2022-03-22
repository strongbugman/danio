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
    class Gender(enum.Enum):
        MALE = 0
        FEMALE = 1
        OTHER = 2

    name: str = danio.field(
        danio.CharField,
        comment="User name",
        default=danio.CharField.NoDefault,
    )
    age: int = danio.field(danio.IntField)
    created_at: datetime.datetime = danio.field(
        danio.DateTimeField,
        comment="when created",
    )
    updated_at: datetime.datetime = danio.field(
        danio.DateTimeField,
        comment="when created",
    )
    gender: Gender = danio.field(danio.IntField, enum=Gender, default=Gender.FEMALE)

    async def before_create(self, **kwargs):
        # user_count += 1
        await super().before_create(**kwargs)

    async def before_update(self, **kwargs):
        self.updated_at = datetime.datetime.now()

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
user = await User.where(User.name == "batman").fetch_one()
user.gender = User.Gender.MALE
await user.save()
await user.delete()
# sql chain
await User.where(User.name != "").limit(10).fetch_all()
# multi where condition
await User.where(User.id != 1, User.name != "").fetch_all()
await User.where(User.id != 1).where(User.name != "").fetch_all()
await User.where(User.id <= 10, User.id >= 20, is_and=False).fetch_all()
# complicated expression
await User.where(User.id == 1).update(age=(User.age + 1) / (User.age / 12) - 2)
await User.where((User.age + 1) == 3).fetch_all()
# complicated sql operation
await User.where(User.id == u.id).update(
    age=User.age.case(User.age > 10, 1, default=18).case(User.age <= 0, 10)
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
    key_fields=(User.id,)
)
user, created, updated = await User(id=2, name="updated?").create_or_update(
    key_fields=(User.id,)
)
```