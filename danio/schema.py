import ast
import dataclasses
import inspect
import itertools
import random
import re
import typing
from importlib import import_module
from pkgutil import iter_modules

from .exception import SchemaException
from .model import Model


class Schema:
    POSTFIX = "ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    FIELD_PATTERN = re.compile(r"\"database: (.*)\"")

    @classmethod
    def _generate(
        cls, m: typing.Type[Model]
    ) -> typing.Tuple[
        typing.Dict[str, str],
        str,
        typing.List[typing.List[str]],
        typing.List[typing.List[str]],
        bool,
    ]:
        fields: typing.Dict[str, str] = {}
        primary_key = ""
        unique_keys: typing.List[typing.List[str]] = []
        index_keys: typing.List[typing.List[str]] = []
        abstracted = False
        # get source code
        codes = inspect.getsourcelines(m)[0]
        _indentation_counts = 0
        for c in codes[0]:
            if c in (" ", "\t"):
                _indentation_counts += 1
            else:
                break
        for i, l in enumerate(codes):
            codes[i] = l[_indentation_counts:]
        # parse and get table field and key
        for a in ast.parse("".join(codes)).body[0].body:  # type: ignore
            if isinstance(a, ast.AnnAssign):
                if a.target.id == "__table_primary_key":  # type: ignore
                    primary_key = a.value.id  # type: ignore
                elif a.target.id in ["__table_unique_keys", "__table_index_keys"]:  # type: ignore
                    if not isinstance(a.value, ast.Tuple):
                        raise SchemaException(
                            f"{m.get_table_name()}: KEYS type should be type.Tuple[type.Tuple[]]"
                        )
                    keys = []
                    for sub in a.value.elts:
                        _keys = []
                        if not isinstance(sub, ast.Tuple):
                            raise SchemaException(
                                f"{m.get_table_name()}: KEYS type should be type.Tuple[type.Tuple[]]"
                            )
                        for e in sub.elts:
                            if isinstance(e, ast.Name):
                                _keys.append(e.id)
                            elif isinstance(e, ast.Constant):
                                _keys.append(e.value)
                            elif isinstance(e, ast.Attribute):
                                _keys.append(e.attr)
                            else:
                                raise SchemaException(
                                    f"{m.get_table_name()} key type not support"
                                )
                        keys.append(_keys)

                    if any(itertools.chain(*keys)):
                        if "unique" in a.target.id:  # type: ignore
                            unique_keys = keys
                        else:
                            index_keys = keys
                elif a.target.id == "__table_abstracted":  # type: ignore
                    if not isinstance(a.value, ast.Constant):
                        raise SchemaException(
                            f"{m.get_table_name()}: __table_abstracted should be constant"
                        )
                    abstracted = bool(a.value.value)
                else:
                    ans = cls.FIELD_PATTERN.findall(
                        "".join(codes[a.lineno - 1 : a.end_lineno])
                    )
                    if ans:
                        fields[a.target.id] = ans[0]  # type: ignore
                    else:
                        fields[a.target.id] = ""  # type: ignore

        return fields, primary_key, unique_keys, index_keys, abstracted

    @classmethod
    def generate_keys(
        cls, keys: typing.List[typing.List[str]], unique=False
    ) -> typing.List[str]:
        row_keys = []
        for ks in keys:
            if not ks:
                continue
            row_keys.append(
                f"{'UNIQUE ' if unique else ''}KEY "
                f"`{'_'.join(ks)[:15]}_{random.randint(1, 10000)}({'_uiq' if unique else '_idx'})` "
                f"({', '.join(f'`{k}`' for k in ks)})"
            )

        return row_keys

    @classmethod
    def generate(cls, m: typing.Type[Model], force=False) -> str:
        fields = {}
        primary_key = ""
        unique_keys: typing.List[typing.List[str]] = []
        index_keys: typing.List[typing.List[str]] = []
        abstracted = False
        for _m in m.mro()[::-1]:
            if issubclass(_m, Model):
                (
                    _fields,
                    _primary_key,
                    _unique_keys,
                    _index_keys,
                    abstracted,
                ) = cls._generate(_m)
                fields.update(_fields)
                if _primary_key:
                    primary_key = _primary_key
                if _unique_keys:
                    unique_keys = _unique_keys
                if _index_keys:
                    index_keys = _index_keys
        # check
        miss_fields = set(f.name for f in dataclasses.fields(m)) - set(fields.keys())
        if miss_fields:
            raise SchemaException(
                f"Miss fields: {miss_fields} in table {m.get_table_name()}"
            )
        if primary_key not in fields:
            raise SchemaException(
                f"Miss primary key {primary_key} in table {m.get_table_name()}"
            )
        for ks in itertools.chain(unique_keys, index_keys):
            for k in ks:
                if k not in fields or not fields[k]:
                    raise SchemaException(f"Miss key {k} in table {m.get_table_name()}")
        # keys
        keys = [f"PRIMARY KEY (`{primary_key}`)"]
        keys.extend(cls.generate_keys(index_keys))
        keys.extend(cls.generate_keys(unique_keys, unique=True))

        if abstracted and not force:
            return ""
        else:
            return (
                f"CREATE TABLE `{m.get_table_name()}` (\n"
                + ",\n".join(itertools.chain((v for v in fields.values() if v), keys))
                + f"\n) {cls.POSTFIX}"
            )

    @classmethod
    def generate_all(cls, paths: typing.List[str], database="") -> str:
        """Generate all orm table by package path"""
        modules = []
        results = []
        if database:
            results.append(
                f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
            results.append(f"USE `{database}`;")
        # get all modules from packages and subpackages
        for path in paths:
            module: typing.Any = import_module(path)
            modules.append(module)
            if hasattr(module, "__path__"):
                package_path = []
                for _, name, ispkg in iter_modules(module.__path__):
                    next_path = path + "." + name
                    if ispkg:
                        package_path.append(next_path)
                    else:
                        modules.append(import_module(next_path))
                if len(package_path) > 0:
                    results.append(cls.generate_all(package_path))
        # get and sift ant class obj from modules
        for module in modules:
            for name, obj in inspect.getmembers(module):
                if (
                    isinstance(obj, type)
                    and issubclass(obj, Model)
                    and obj is not Model
                ):
                    results.append(cls.generate(obj))

        return "\n".join(results)
