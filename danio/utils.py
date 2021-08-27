import inspect
import typing
from importlib import import_module
from pkgutil import iter_modules


class class_property:
    def __init__(self, fget: typing.Callable):
        self.fget = fget

    def __get__(
        self, obj: typing.Any, cls: typing.Optional[typing.Type] = None
    ) -> typing.Any:
        if cls is None:
            cls = obj.__class__
        return self.fget.__get__(obj, cls)()  # type: ignore


TV = typing.TypeVar("TV")


def find_classes(
    cls: typing.Type[TV], paths: typing.List[str]
) -> typing.Set[typing.Type[TV]]:
    """Parse all orm table by package path"""
    modules = []
    models: typing.Set[typing.Type[TV]] = set()
    # get all modules from packages and subpackages
    for path in paths:
        module: typing.Any = import_module(path)
        modules.append(module)
        if hasattr(module, "__path__"):
            package_paths = []
            for _, name, ispkg in iter_modules(module.__path__):
                next_path = path + "." + name
                if ispkg:
                    package_paths.append(next_path)
                else:
                    modules.append(import_module(next_path))
            if len(package_paths) > 0:
                models.union(find_classes(cls, package_paths))
    # get and sift ant class obj from modules
    for module in modules:
        for name, obj in inspect.getmembers(module):
            if isinstance(obj, type) and issubclass(obj, cls) and obj not in models:
                models.add(obj)

    return models
