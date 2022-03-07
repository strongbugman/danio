# Combo queries

## Get or Create

```python
async def get_or_create(
    self: MODEL_TV,
    key_fields: typing.Sequence[Field],
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = (),
    validate: bool = True,
    for_update: bool = False,
) -> typing.Tuple[MODEL_TV, bool]
```

This method will search and get a instance from database by `key_fields` (primary key or unique key fields) value, or create a new one if no data match. eg:
```python
async def get_one():
    config, created = await Config(id=1, f="v").get_or_create((Config.id,))
```
And the instance will active signals(`*_read` or `*_update`)

## Create or Update

```python
async def create_or_update(
    self: MODEL_TV,
    key_fields: typing.Sequence[Field],
    database: typing.Optional[Database] = None,
    fields: typing.Sequence[Field] = (),
    update_fields: typing.Sequence[Field] = (),
    validate: bool = True,
) -> typing.Tuple[MODEL_TV, bool, bool]
```

This method will call `get_or_create` then update to database if database has matched data, all operation will wrap in one transaction, eg:
```python
async def subscribe(cls, user_id: int, blogger_id: int):
    ins, created, updated = await Sub(user_id=user_id, blogger_id=blogger_id, status=Sub.Status.SUBSCRIBED).create_or_update(
        (Sub.user_id, Sub.blogger_id),
        update_fields=(Sub.status,)
    )
    if created or updated:
        await redis.incr(f"BLOGGER_SUBSCRIBED_COUNT_{blogger_id}")
```

And the instance will active signals too.
