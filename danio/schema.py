from __future__ import annotations

import ast
import contextlib
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

if typing.TYPE_CHECKING:
    from .database import Database

SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")


@dataclasses.dataclass
class Field:
    name: str
    db_name: str
    describe: str


@dataclasses.dataclass
class Index:
    fields: typing.List[Field]


@dataclasses.dataclass
class Schema:
    POSTFIX: typing.ClassVar[
        str
    ] = "ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    FIELD_DESCRIBE_PATTERN: typing.ClassVar = re.compile(r"\"database: (.*)\"")
    FIELD_DBNAME_PATTERN: typing.ClassVar = re.compile(r"^`(.*)`")

    name: str
    fields: typing.Dict[str, Field]
    primary_field: Field
    indexes: typing.List[Index]
    unique_indexes: typing.List[Index]
    abstracted: bool
    model: typing.Type[Model]

    @classmethod
    def _parse(
        cls: typing.Type[SCHEMA_TV], m: typing.Type[Model], schema: SCHEMA_TV
    ) -> SCHEMA_TV:
        schema.name = m.get_table_name()
        schema.abstracted = False
        primary_key = ""
        unique_keys: typing.List[typing.List[str]] = []
        index_keys: typing.List[typing.List[str]] = []
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
                # primary key
                if a.target.id == "__table_primary_key":  # type: ignore
                    primary_key = a.value.id  # type: ignore
                # index
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
                # abstract
                elif a.target.id == "__table_abstracted":  # type: ignore
                    if not isinstance(a.value, ast.Constant):
                        raise SchemaException(
                            f"{m.get_table_name()}: __table_abstracted should be constant"
                        )
                    schema.abstracted = bool(a.value.value)
                # field
                else:
                    ans = cls.FIELD_DESCRIBE_PATTERN.findall(
                        "".join(codes[a.lineno - 1 : a.end_lineno])
                    )
                    if ans:
                        try:
                            field_name = a.target.id  # type: ignore
                            describe: str = ans[0]
                            field_db_name = cls.FIELD_DBNAME_PATTERN.findall(describe)[
                                0
                            ]
                            if field_db_name == "{}":
                                field_db_name = field_name
                                describe = describe.replace(
                                    "`{name}`", f"`{field_db_name}`"
                                )
                            schema.fields[field_name] = Field(
                                name=field_name,
                                db_name=field_db_name,
                                describe=describe,
                            )
                        except IndexError as e:
                            raise SchemaException(
                                f"{schema.name}: can't find field db name"
                            ) from e
                    else:
                        with contextlib.suppress(KeyError):
                            schema.fields.pop(a.target.id)  # type: ignore

        try:
            if primary_key:
                schema.primary_field = schema.fields[primary_key]
            if index_keys:
                schema.indexes.clear()
                schema.indexes = [
                    Index(fields=[schema.fields[key] for key in keys])
                    for keys in index_keys
                ]
            if unique_keys:
                schema.unique_indexes.clear()
                schema.unique_indexes = [
                    Index(fields=[schema.fields[key] for key in keys])
                    for keys in unique_keys
                ]
        except KeyError as e:
            raise SchemaException(f"{schema.name}: missing field") from e

        return schema

    @classmethod
    def generate_indexes(
        cls, indexes: typing.List[Index], unique=False
    ) -> typing.List[str]:
        row_keys = []
        for index in indexes:
            row_keys.append(
                f"{'UNIQUE ' if unique else ''}KEY "
                f"`{'_'.join(f.name for f in index.fields)[:15]}_{random.randint(1, 10000)}({'_uiq' if unique else '_idx'})` "
                f"({', '.join(f'`{f.name}`' for f in index.fields)})"
            )

        return row_keys

    @classmethod
    def parse(cls: typing.Type[SCHEMA_TV], m: typing.Type[Model]) -> SCHEMA_TV:
        schema = cls(
            name=m.get_table_name(),
            fields={},
            primary_field=Field(name="", describe="", db_name=""),
            indexes=[],
            unique_indexes=[],
            abstracted=False,
            model=m,
        )

        for _m in m.mro()[::-1]:
            if issubclass(_m, Model):
                schema = cls._parse(_m, schema)
        m.SCHEMA = schema

        return schema

    @classmethod
    def parse_all(
        cls: typing.Type[SCHEMA_TV], paths: typing.List[str]
    ) -> typing.List[SCHEMA_TV]:
        """Parse all orm table by package path"""
        modules = []
        results = []
        models = set()
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
                    results.extend(cls.parse_all(package_path))
        # get and sift ant class obj from modules
        for module in modules:
            for name, obj in inspect.getmembers(module):
                if (
                    isinstance(obj, type)
                    and issubclass(obj, Model)
                    and obj is not Model
                    and obj not in models
                ):
                    models.add(obj)
                    schema = cls.parse(obj)
                    if not schema.abstracted:
                        results.append(schema)

        return results

    @classmethod
    def generate_all(cls, paths: typing.List[str], database="") -> str:
        """Generate all orm table define by package path"""
        results = []
        if database:
            results.append(
                f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
            results.append(f"USE `{database}`;")
        for s in cls.parse_all(paths):
            results.append(s.to_sql())

        return "\n".join(results)

    def to_sql(self) -> str:
        keys = [f"PRIMARY KEY (`{self.primary_field.name}`)"]
        keys.extend(self.__class__.generate_indexes(self.indexes))
        keys.extend(self.__class__.generate_indexes(self.unique_indexes, unique=True))

        return (
            f"CREATE TABLE `{self.name}` (\n"
            + ",\n".join(
                itertools.chain((v.describe for v in self.fields.values()), keys)
            )
            + f"\n) {self.POSTFIX}"
        )

    async def make_migration(self, database: Database) -> str:
        """compare model and DB table"""
        return ""
