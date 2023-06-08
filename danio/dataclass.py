import typing as tp
import typeguard
import abc
import json


class Field:
    __slots__ = ("name", "type_hint", "default")
    __copy__fields__: tp.Tuple[str, ...] = ("default",)

    class NoDefault:
        pass

    class Default:
        pass

    DEFAULT: tp.ClassVar[tp.Any] = NoDefault

    def __init__(
        self, name: str = "", type_hint: tp.Type = object, default: tp.Any = NoDefault
    ) -> None:
        self.name = name
        self.type_hint = type_hint
        self.default = default if default is not self.Default else self.DEFAULT

    def __repr__(self) -> str:
        return f"Field(name: {self.field_name} type: {self.type_hint})"

    @property
    def field_name(self) -> str:
        return self.name

    @property
    def default_value(self) -> tp.Any:
        if self.default is self.NoDefault:
            return self.default

        if callable(self.default):
            return self.default()
        else:
            return self.default

    def check(self, value: tp.Any):
        typeguard.check_type(value, self.type_hint)


def field(default: tp.Callable) -> tp.Any:
    return Field(default=default)


@tp.dataclass_transform(field_specifiers=(Field,))
class Meta(abc.ABCMeta):
    def __new__(__mcls, __name, __bases, __namespace, **kwargs):
        # markup __fields__
        fields = {}
        for base in __bases:
            base_fields = getattr(base, "__fields__", {})
            fields.update(base_fields)
        current_fields = {
            n: Field(n, t, __namespace.get("n", Field.NoDefault))
            for n, t in __namespace.get("__annotations__", {}).items()
            if "ClassVar" not in str(t)
        }
        fields.update(current_fields)
        __namespace["__fields__"] = fields
        __namespace["__dataclass_fields__"] = fields
        __namespace["__match_args__"] = tuple(f.field_name for f in fields.values())
        # set field default
        for f in fields.values():
            if f.field_name in __namespace:
                default_or_field = __namespace.pop(f.field_name)
                if isinstance(default_or_field, Field):
                    for _f in Field.__copy__fields__:
                        setattr(fields[f.field_name], _f, getattr(default_or_field, _f))
                else:
                    fields[f.field_name].default = default_or_field
        # markup __slots__
        __namespace["__slots__"] = tuple(fields.keys())
        if not __bases:
            __namespace["__slots__"] += ("__dict__",)
        return super().__new__(__mcls, __name, __bases, __namespace, **kwargs)


class BaseData(metaclass=Meta):
    __fields__: tp.ClassVar[tp.Dict[str, Field]]

    def __init__(self, *args, **kwargs):
        if len(args) + len(kwargs) > len(self.__fields__):
            raise TypeError(
                f"{self.__class__.__name__}.__init__() takes from {len(self.__fields__)} arguments at max but {len(args) + len(kwargs)} were given"
            )

        # makeup params
        data = {}
        # args parsing
        fields = list(self.__fields__.values())
        for i, arg in enumerate(args):
            data[fields[i].field_name] = arg
        # kwargs parsing
        kw_fields = {f.field_name: f for f in fields[len(data) :]}
        for n, v in kwargs.items():
            if n in data:
                raise TypeError(
                    f"{self.__class__.__name__}.__init__() got multiple values for argument '{n}'"
                )
            if n not in kw_fields:
                raise TypeError(
                    f"{self.__class__.__name__}.__init__() got an unexpected keyword argument '{n}'"
                )
            data[n] = v
        # default value
        for f in self.__fields__.values():
            if f.field_name not in data:
                default = f.default_value
                if default is Field.NoDefault:
                    raise TypeError(
                        f"{self.__class__.__name__}.__init__() missing argument {f.field_name}"
                    )
                else:
                    data[f.field_name] = default
        # type check
        for f in self.__fields__.values():
            f.check(data[f.field_name])

        for k, v in data.items():
            setattr(self, k, v)

        self.after_init()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.to_dict()})"

    def after_init(self):
        pass

    def validate(self):
        pass

    def to_dict(self) -> tp.Dict[str, tp.Any]:
        data = {}
        for n in self.__fields__.keys():
            data[n] = getattr(self, n)
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
