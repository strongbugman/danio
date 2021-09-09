from __future__ import annotations

import ast
import contextlib
import dataclasses
import inspect
import itertools
import random
import re
import typing
from datetime import datetime

from .exception import SchemaException

if typing.TYPE_CHECKING:
    from .model import Model

if typing.TYPE_CHECKING:
    from .database import Database

SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")


@dataclasses.dataclass
class Field:
    name: str  # mapping model filed name
    db_name: str
    describe: str
    # db_type

    def __hash__(self):
        return hash(f"{self.db_name}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def to_sql(self) -> str:
        return self.describe


@dataclasses.dataclass
class Index:
    fields: typing.List[Field]
    unique: bool
    name: str = ""

    def __post_init__(self):
        if not self.name:
            self.name = f"`{'_'.join(f.db_name for f in self.fields)[:15]}_{random.randint(1, 10000)}{'_uiq' if self.unique else '_idx'}` "

    def __hash__(self):
        return hash((self.unique, tuple(f.db_name for f in self.fields)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Index):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def to_sql(self) -> str:
        return (
            f"{'UNIQUE ' if self.unique else ''}KEY "
            f"{self.name}"
            f"({', '.join(f'`{f.db_name}`' for f in self.fields)})"
        )


@dataclasses.dataclass
class Schema:
    POSTFIX: typing.ClassVar[
        str
    ] = "ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    FIELD_DESCRIBE_PATTERN: typing.ClassVar[re.Pattern] = re.compile(
        r"\"database: (.*)\""
    )
    FIELD_DBNAME_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"^`([^ ,]*)`")
    DB_FIELD_NAME_PATTERN: typing.ClassVar[re.Pattern] = re.compile(r"`([^ ,]*)`")

    name: str  # TODO: table name
    fields: typing.List[Field]  # TODO: set
    primary_field: Field
    indexes: typing.List[Index]
    abstracted: bool

    def __hash__(self):
        return hash(
            (
                self.name,
                (f for f in self.fields),
                self.primary_field,
                (i for i in self.indexes),
            )
        )

    def __eq__(self, other: object):
        if not isinstance(other, Schema):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def __sub__(self, other: object) -> Migration:
        if not isinstance(other, Schema):
            raise NotImplementedError()

        add_fields = set(self.fields) - set(other.fields)
        drop_fields = set(other.fields) - set(self.fields)
        return Migration(
            schema=self,
            add_indexes=list(set(self.indexes) - set(other.indexes)),
            drop_indexes=list(set(other.indexes) - set(self.indexes)),
            add_fields=list(add_fields),
            drop_fields=list(drop_fields),
        )

    def to_model_fields(self) -> typing.Dict[str, Field]:
        return {f.name: f for f in self.fields}

    def to_sql(self) -> str:
        keys = [f"PRIMARY KEY (`{self.primary_field.name}`)"]
        keys.extend([index.to_sql() for index in self.indexes])

        return (
            f"CREATE TABLE `{self.name}` (\n"
            + ",\n".join(itertools.chain((v.to_sql() for v in self.fields), keys))
            + f"\n) {self.POSTFIX}"
        )

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
                                    "`{}`", f"`{field_db_name}`"
                                )
                            field = Field(
                                name=field_name,
                                db_name=field_db_name,
                                describe=describe,
                            )
                            if field not in schema.fields:
                                schema.fields.append(
                                    Field(
                                        name=field_name,
                                        db_name=field_db_name,
                                        describe=describe,
                                    )
                                )
                        except IndexError as e:
                            raise SchemaException(
                                f"{schema.name}: can't find field db name"
                            ) from e
                    else:
                        with contextlib.suppress(ValueError):
                            for field in schema.fields:
                                if field.name == a.target.id:  # type: ignore
                                    schema.fields.remove(field)
                                    break

        try:
            model_fields = schema.to_model_fields()
            if primary_key:
                schema.primary_field = model_fields[primary_key]
            if index_keys:
                for index in schema.indexes.copy():
                    if not index.unique:
                        schema.indexes.remove(index)
                schema.indexes.extend(
                    [
                        Index(fields=[model_fields[key] for key in keys], unique=False)
                        for keys in index_keys
                    ]
                )
            if unique_keys:
                for index in schema.indexes.copy():
                    if index.unique:
                        schema.indexes.remove(index)
                schema.indexes.extend(
                    [
                        Index(fields=[model_fields[key] for key in keys], unique=True)
                        for keys in unique_keys
                    ]
                )
        except KeyError as e:
            raise SchemaException(f"{schema.name}: missing field") from e

        return schema

    @classmethod
    def from_model(cls: typing.Type[SCHEMA_TV], m: typing.Type[Model]) -> SCHEMA_TV:
        schema = cls(
            name=m.table_name,
            fields=[],
            primary_field=Field(name="", describe="", db_name=""),
            indexes=[],
            abstracted=False,
        )

        for _m in m.mro()[::-1]:
            if issubclass(_m, m.mro()[-2]):
                schema = cls._parse(_m, schema)  # type: ignore

        return schema

    @classmethod
    async def from_db(
        cls: typing.Type[SCHEMA_TV], database: Database, m: typing.Type[Model]
    ) -> SCHEMA_TV:
        schema = cls(
            name=m.table_name,
            fields=[],
            primary_field=Field(name="", describe="", db_name=""),
            indexes=[],
            abstracted=False,
        )
        db_names = {f.db_name: f.name for f in m.schema.fields}
        for line in (await database.fetch_all(f"SHOW CREATE TABLE {m.table_name}"))[0][
            1
        ].split("\n")[1:-1]:
            if "PRIMARY KEY" in line:
                db_name = cls.DB_FIELD_NAME_PATTERN.findall(line)[0]
                for f in schema.fields:
                    if db_name == f.db_name:
                        schema.primary_field = f
                        break
            elif "KEY" in line:
                fields = {f.db_name: f for f in schema.fields}
                index_fileds = []
                _names = cls.DB_FIELD_NAME_PATTERN.findall(line)
                index_name = _names[0]
                index_fileds = [fields[n] for n in _names[1:]]
                schema.indexes.append(
                    Index(fields=index_fileds, unique="UNIQUE" in line, name=index_name)
                )
            else:
                db_name = cls.DB_FIELD_NAME_PATTERN.findall(line)[0]
                if db_name in db_names:
                    name = db_names[db_name]
                else:
                    name = ""
                f = Field(name=name, db_name=db_name, describe=line[2:])
                schema.fields.append(f)

        return schema


@dataclasses.dataclass
class Migration:
    """
    Schema migration support
    Support table changes: add, drop, name change?
    Support field changes: add, drop, change type, name change?
    Support index changes: add, drop
    """

    schema: Schema
    name: str = ""
    add_schema: bool = False
    drop_schema: bool = False
    drop_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    add_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    change_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    add_indexes: typing.List[Index] = dataclasses.field(default_factory=list)
    drop_indexes: typing.List[Index] = dataclasses.field(default_factory=list)

    def __post__init__(self):
        if not self.name:
            self.name = f"{datetime.now()}"

    def to_sql(self) -> str:
        sqls = []
        if self.add_schema:
            sqls.append(self.schema.to_sql())
        elif self.drop_schema:
            sqls.append(f"DROP TABLE {self.schema.name}")
        else:
            for f in self.add_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} ADD COLUMN {f.to_sql()}")
            for f in self.drop_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} DROP COLUMN {f.db_name}")
            for i in self.add_indexes:
                sqls.append(
                    f"CREATE {'UNIQUE' if i.unique else ''} INDEX {i.name} on {self.schema.name} ({','.join('`' + f.db_name + '`' for f in i.fields)})"
                )
            for i in self.drop_indexes:
                if not set(i.fields) & set(self.drop_fields):
                    sqls.append(f"ALTER TABLE {self.schema.name} DROP INDEX {i.name}")
        if sqls:
            sqls[-1] += ";"

        return ";\n".join(sqls)
