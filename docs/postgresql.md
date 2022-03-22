# PostgreSQL

Danio support PostgreSQL with a little code change.

## Database Define

Use `asyncpg` driver:
```python
db = danio.Database(
    f"postgres://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{db_name}",
    min_size=1,
    max_size=3,
    max_inactive_connection_lifetime=60,
)
```

Use `aiopg` driver:
```python
db2 = danio.Database(
    f"aiopg://postgres:{os.getenv('POSTGRES_PASSWORD', 'letmein')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{db_name}",
    min_size=1,
    max_size=3,
)
```

## Auto Increment Field

Danio use PostgreSQL's `serial` type as a auto increment field, so we should change Model's primary field define:
```python
@dataclasses.dataclass
class BasePostgreSQLModel(danio.Model):
    id: int = danio.field(danio.IntField, primary=True, type="serial", comment="primary key")
```

## Field Type

For PostgreSQL we may need change danio's field type define, like `timestamp`:
```python
updated_at: datetime.datetime = danio.field(
    danio.DateTimeField,
    type="timestamp without time zone",
    comment="when created",
)
```

## upsert

PostgreSQL need explicit `conflict_targets` for `upsert` method
