# Signal

Danio support signal during instance's life cycle, all sql operation interact with model instance (receive or return instance as param) will call those signal method:

* after_read - called after the instance has been read from database

    `async def after_read(self) -> None`

* before_create - called before the instance has been created to database
* after_create - called after the instance has been created to database
* before_update
* after_update
* before_delete
* after_delete

Special signal:

* after_init - called after the instance has been init (call by `after_init` actually)

    `def after_init(self) -> None`

eg:
```python
count = 0
@dataclasses.dataclass
class Pet(danio.Model):
    name: str = danio.field(danio.CharField)
    age: int = danio.field(danio.IntField)
    code: str = ""

    def after_init(self):
        super().after_init()
        self.code = f"{self.name}_{self.age}"
    
    async def after_create(self):
        await super().after_create()
        count += 1
    
    async def after_delete(self):
        await super().after_delete()
        count -= 1
```