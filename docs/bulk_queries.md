# Bulk Queries

Optimize database IO performance when operate multi instance or table row data.

## Bulk Create

```python
@classmethod
async def bulk_create(
    cls: typing.Type[MODEL_TV],
    instances: typing.Sequence[MODEL_TV],
    fields: typing.Sequence[Field] = (),
    database: typing.Optional[Database] = None,
    validate: bool = True,
) -> typing.Sequence[MODEL_TV]
```

This method will insert multi instance to Database in one sql:
```sql
INSERT TABLE <table name> VALUES (<values>), ...;
```
And all instances will active model signals(`before_save` and `after_save`)

eg:
```python
users = [User(name=f"user_{i}") for i in range(10)]
await User.bulk_create(users)
```

For better database performance，if there are too many instances, consider grouping them and calling `bulk_create` in sequence.

## Bulk Update

```python
@classmethod
async def bulk_update(
    cls: typing.Type[MODEL_TV],
    instances: typing.Sequence[MODEL_TV],
    fields: typing.Sequence[Field] = (),
    database: typing.Optional[Database] = None,
    validate: bool = True,
) -> typing.Sequence[MODEL_TV]
```

This method will update all instance by sql `case` statement, example sql:
```sql
UPDATE `user` 
SET       `name` = CASE 
                  WHEN `id` = <id1> THEN  <name1>
                  WHEN `id` = <id2> THEN  <name2>
                  WHEN `id` = <id3> THEN  <name3>
                end, 
WHERE  `id` IN ( <id1>, <id2>, <id3>); 
```
And all instances will active model signals too.

eg:
```python
users = await User.where().fetch_all()
for u in users:
    u.name += "_updated"
await User.bulk_update(users, fields=(User.name, ))
```

Consider grouping instances and calling `bulk_update` in sequence if instances size is too large.

## Bulk delete

```python
@classmethod
async def bulk_delete(
    cls,
    instances: typing.Sequence[MODEL_TV],
    database: typing.Optional[Database] = None,
) -> int
```
This method will delete all instance in database and return deleted count, example sql:
```sql
DELETE FROM <table> WHERE `id` IN (<id1>, <id2>)
```
And all instances will active model signals too.


## Upsert

```python
@classmethod
async def upsert(
    cls,
    insert_data: typing.List[typing.Dict[str, typing.Any]],
    database: typing.Optional[Database] = None,
    update_fields: typing.Sequence[str] = (),
) -> typing.Tuple[bool, bool]
```

This method using [insert on duplicate](https://dev.mysql.com/doc/refman/5.6/en/insert-on-duplicate.html), sql example:
```sql
INSERT INTO <table> (<f1>,<f2>) VALUES (<v1>,<v2>)
  ON DUPLICATE KEY UPDATE <f2>=VALUES(<f2>);
```
And this method just execute a raw sql, eg:

```python
created, updated = await User.upsert(
    [
        dict(id=1, name="updated"),
    ],
    update_fields=["name"],
)
```
