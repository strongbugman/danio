# 🐠 Danio

[![UnitTest Status](https://github.com/strongbugman/danio/workflows/UnitTest/badge.svg)](https://github.com/strongbugman/danio/actions)
[![PyPI version](https://badge.fury.io/py/danio.svg)](https://pypi.org/project/danio/)
[![Codecov coverage](https://codecov.io/gh/strongbugman/danio/branch/main/graph/badge.svg)](https://codecov.io/gh/strongbugman/danio)
[![License](https://img.shields.io/github/license/strongbugman/danio)](https://github.com/strongbugman/danio/blob/main/LICENSE)
[![Python Support](https://img.shields.io/pypi/pyversions/danio.svg)](https://pypi.org/project/danio/)

**Danio** is an elegant, lightweight, and asynchronous ORM for Python. It bridges Python's standard `dataclasses` with encode's robust `databases` library, bringing a highly expressive, type-safe, and asynchronous interface to your database operations.

Designed with modern developers in mind, Danio aims to make database interaction simple, clean, and robust against common pitfalls like Out Of Memory (OOM) errors and runtime type mismatches.

---

## ✨ Features

*   **⚡ Async First**: Built from the ground up for `asyncio` using standard async/await paradigms.
*   **📦 Dataclasses Core**: Leverages Python's native `dataclasses` for clean, lightweight model schemas.
*   **🛡️ Type-Safety**: 100% type hints everywhere. Auto-generated stubs (`Danio Hints`) ensure flawless IDE autocompletion.
*   **🧠 Prevent Out-Of-Memory**: Designed with memory limits in mind, allowing easy customization of Fields and Model serialization.
*   **🚀 Comprehensive CRUD**: Simple query-chaining, transaction support, connection-pooling, and table locks.
*   **🔔 Lifecycle Signals**: Built-in hooks like `before_create`, `after_create`, `before_update`, etc.
*   **🔋 Advanced DB Operations**: Bulk creation, bulk deletion, atomic updates, and seamless `UPSERT` / `get_or_create`.
*   **🚧 Schema Migrations**: Practical helper scripts to assist with standard SQL table schema migrations.
*   **🔌 Multi-Engine Support**: Out-of-the-box support for **MySQL**, **Postgres**, and **SQLite**.

---

## ⚙️ Installation

Install via pip:
```bash
pip install danio
```

*Note: Depending on your database of choice, you should install corresponding async drivers (e.g., `aiomysql`, `asyncpg`, or `aiosqlite`).*

---

## 📖 Documentation

Full documentation, guides, and tutorials can be found at:
👉 **[Danio Documentation Site](https://strongbugman.github.io/danio/)**

---

## ⚡ Glance

```python
import dataclasses
import enum
import typing
import datetime
import danio

# 1. Establish Database Connection
db = danio.Database(
    "mysql://root:***@server:3306/test",
    maxsize=3,
    charset="utf8mb4",
    use_unicode=True,
    connect_timeout=60,
)

# 2. Define Model using Python Type-Hints and PEP 593 Annotated Fields
@danio.model
class User(danio.Model):
    # Auto-generated and maintained by danio:
    # --------------------Danio Hints--------------------
    # TABLE NAME: user
    # TABLE IS MIGRATED!
    ID: typing.ClassVar[danio.Field]          # "id" serial PRIMARY KEY NOT NULL
    NAME: typing.ClassVar[danio.Field]        # "name" varchar(255)  NOT NULL
    AGE: typing.ClassVar[danio.Field]         # "age" int  NOT NULL
    CREATED_AT: typing.ClassVar[danio.Field]  # "created_at" timestamp  NOT NULL
    UPDATED_AT: typing.ClassVar[danio.Field]  # "updated_at" timestamp  NOT NULL
    GENDER: typing.ClassVar[danio.Field]      # "gender" int  NOT NULL
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
        danio.DateTimeField(type="timestamp without time zone", comment="Created time"),
    ] = dataclasses.field(default_factory=datetime.datetime.now)
    updated_at: typing.Annotated[
        datetime.datetime,
        danio.DateTimeField(type="timestamp without time zone", comment="Updated time"),
    ] = dataclasses.field(default_factory=datetime.datetime.now)
    gender: typing.Annotated[Gender, danio.IntField(enum=Gender)] = Gender.MALE

    # Lifecycle Hooks
    async def before_update(self, validate=True):
        self.updated_at = datetime.datetime.now()
        await super().before_update(validate=True)

    async def validate(self):
        await super().validate()
        if not self.name:
            raise danio.ValidateException("Name cannot be empty!")

    @classmethod
    def get_database(cls, operation: danio.Operation, table: str, *args, **kwargs) -> danio.Database:
        return db

# 3. Perform Asynchronous CRUD Operations
async def main():
    # ---- Create ----
    user = await User(name="batman", age=30).save()
    print(f"Created user ID: {user.id}")

    # ---- Read (Type-safe Queries) ----
    user = await User.where(User.NAME == "batman").fetch_one()
    
    # ---- Update ----
    user.gender = User.Gender.MALE
    await user.save()

    # ---- Advanced Query Chaining ----
    active_users = await User.where(User.AGE > 18).limit(10).order_by(User.NAME).fetch_all()

    # ---- Complex Expression Updates ----
    # Increment all matching users' ages atomically
    await User.where(User.ID == 1).update(age=(User.AGE + 1))

    # ---- Bulk Operations ----
    users_to_create = [User(name=f"user_{i}", age=20) for i in range(10)]
    await User.bulk_create(users_to_create)

    # ---- Upsert (Create or Update) ----
    user, created, updated = await User(id=1, name="updated_name").create_or_update(
        key_fields=(User.ID,)
    )
```

---

## 🛠️ Development and Formatting

We use **[Ruff](https://github.com/astral-sh/ruff)** for extremely fast linting and formatting.

To format and check your code quality before submitting a PR:
```bash
# Check code style and run static checks
make lint

# Automatically format and fix lint errors
make format

# Run full tests across your local SQLite/Postgres/MySQL setup
make test
```

---

## 📄 License

Danio is open-sourced software licensed under the [BSD 3-Clause License](LICENSE).
