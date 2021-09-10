from __future__ import annotations

import ast
import contextlib
import dataclasses
import inspect
import itertools
import random
import re
import typing

from .exception import SchemaException

if typing.TYPE_CHECKING:
    from .model import Model

if typing.TYPE_CHECKING:
    from .database import Database

SCHEMA_TV = typing.TypeVar("SCHEMA_TV", bound="Schema")


@dataclasses.dataclass
class Field:
    name: str
    describe: str
    model_name: str  # mapping model filed name
    type: str = ""

    def __post_init__(self):
        if not self.type and self.describe:
            self.type = self.describe.split(" ")[1]

    def __hash__(self):
        return hash((self.name, self.type))

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
            self.name = f"`{'_'.join(f.name for f in self.fields)[:15]}_{random.randint(1, 10000)}{'_uiq' if self.unique else '_idx'}` "

    def __hash__(self):
        return hash((self.unique, tuple(f.name for f in self.fields)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Index):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def to_sql(self) -> str:
        return (
            f"{'UNIQUE ' if self.unique else ''}KEY "
            f"{self.name}"
            f"({', '.join(f'`{f.name}`' for f in self.fields)})"
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

    name: str
    primary_field: typing.Optional[Field] = None
    indexes: typing.Set[Index] = dataclasses.field(default_factory=set)
    fields: typing.Set[Field] = dataclasses.field(default_factory=set)
    abstracted: bool = False
    model: typing.Optional[typing.Type[Model]] = None

    def __hash__(self):
        return hash(
            (
                self.name,
                tuple(f for f in sorted(self.fields, key=lambda f: f.name)),
                self.primary_field,
                tuple(i for i in sorted(self.indexes, key=lambda f: f.name)),
            )
        )

    def __eq__(self, other: object):
        if not isinstance(other, Schema):
            raise NotImplementedError()
        return self.__hash__() == other.__hash__()

    def __sub__(self, other: object) -> Migration:
        if other is None:
            return Migration(schema=self, add_schema=True)
        elif not isinstance(other, Schema):
            raise NotImplementedError()
        # fields
        add_fields = set(self.fields) - set(other.fields)
        _add_fields = {f.name: f for f in add_fields}
        drop_fields = set(other.fields) - set(self.fields)
        _drop_fields = {f.name: f for f in drop_fields}
        change_type_fileds = []
        change_type_field_names = set(f.name for f in add_fields) & set(
            f.name for f in drop_fields
        )
        for f_name in change_type_field_names:
            field = _add_fields[f_name]
            add_fields.remove(field)
            drop_fields.remove(_drop_fields[field.name])
            change_type_fileds.append(field)

        return Migration(
            schema=self,
            add_indexes=list(set(self.indexes) - set(other.indexes)),
            drop_indexes=list(set(other.indexes) - set(self.indexes)),
            add_fields=list(add_fields),
            drop_fields=list(drop_fields),
            change_type_fields=change_type_fileds,
        )

    def to_sql(self) -> str:
        if not self.primary_field:
            raise SchemaException("No primary field!")

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
                                name=field_db_name,
                                model_name=field_name,
                                describe=describe,
                            )
                            schema.fields.add(field)
                        except IndexError as e:
                            raise SchemaException(
                                f"{schema.name}: can't find field db name, line: {ans}"
                            ) from e
                    else:
                        with contextlib.suppress(ValueError):
                            for field in schema.fields:
                                if field.name == a.target.id:  # type: ignore
                                    schema.fields.remove(field)
                                    break

        try:
            model_fields = {f.name: f for f in schema.fields}
            if primary_key:
                schema.primary_field = model_fields[primary_key]
            if index_keys:
                for index in schema.indexes.copy():
                    if not index.unique:
                        schema.indexes.remove(index)
                for _keys in index_keys:
                    schema.indexes.add(
                        Index(fields=[model_fields[key] for key in _keys], unique=False)
                    )
            if unique_keys:
                for index in schema.indexes.copy():
                    if index.unique:
                        schema.indexes.remove(index)
                for _keys in unique_keys:
                    schema.indexes.add(
                        Index(fields=[model_fields[key] for key in _keys], unique=True)
                    )
        except KeyError as e:
            raise SchemaException(f"{schema.name}: missing field") from e

        return schema

    @classmethod
    def from_model(cls: typing.Type[SCHEMA_TV], m: typing.Type[Model]) -> SCHEMA_TV:
        schema = cls(name=m.table_name)

        for _m in m.mro()[::-1]:
            if issubclass(_m, m.mro()[-2]):
                schema = cls._parse(_m, schema)  # type: ignore
        schema.model = m

        return schema

    @classmethod
    async def from_db(
        cls: typing.Type[SCHEMA_TV], database: Database, m: typing.Type[Model]
    ) -> typing.Optional[SCHEMA_TV]:
        schema = cls(name=m.table_name, model=m)
        db_names = {f.name: f.model_name for f in m.schema.fields}
        try:
            for line in (await database.fetch_all(f"SHOW CREATE TABLE {m.table_name}"))[
                0
            ][1].split("\n")[1:-1]:
                if "PRIMARY KEY" in line:
                    db_name = cls.DB_FIELD_NAME_PATTERN.findall(line)[0]
                    for f in schema.fields:
                        if db_name == f.name:
                            schema.primary_field = f
                            break
                elif "KEY" in line:
                    fields = {f.name: f for f in schema.fields}
                    index_fileds = []
                    _names = cls.DB_FIELD_NAME_PATTERN.findall(line)
                    index_name = _names[0]
                    index_fileds = [fields[n] for n in _names[1:]]
                    schema.indexes.add(
                        Index(
                            fields=index_fileds,
                            unique="UNIQUE" in line,
                            name=index_name,
                        )
                    )
                else:
                    db_name = cls.DB_FIELD_NAME_PATTERN.findall(line)[0]
                    if db_name in db_names:
                        name = db_names[db_name]
                    else:
                        name = ""
                    schema.fields.add(
                        Field(
                            name=db_name,
                            describe=line[2:],
                            model_name=name,
                        )
                    )
        except Exception as e:
            if "doesn't exist" in str(e):
                return None
            else:
                raise e

        return schema


@dataclasses.dataclass
class Migration:
    schema: Schema  # migrate to this schema
    add_schema: bool = False
    drop_schema: bool = False
    old_schame_name: str = ""
    drop_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    add_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    change_type_fields: typing.List[Field] = dataclasses.field(default_factory=list)
    add_indexes: typing.List[Index] = dataclasses.field(default_factory=list)
    drop_indexes: typing.List[Index] = dataclasses.field(default_factory=list)

    def to_sql(self) -> str:
        sqls = []
        if self.add_schema:
            sqls.append(self.schema.to_sql())
        elif self.drop_schema:
            sqls.append(f"DROP TABLE {self.schema.name}")
        else:
            if self.old_schame_name:
                sqls.append(
                    f"ALTER TABLE {self.old_schame_name} RENAME {self.schema.name}"
                )
            for f in self.add_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} ADD COLUMN {f.to_sql()}")
            for f in self.drop_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} DROP COLUMN {f.name}")
            for f in self.change_type_fields:
                sqls.append(f"ALTER TABLE {self.schema.name} MODIFY {f.name} {f.type}")
            for i in self.add_indexes:
                sqls.append(
                    f"CREATE {'UNIQUE' if i.unique else ''} INDEX {i.name} on {self.schema.name} ({','.join('`' + f.name + '`' for f in i.fields)})"
                )
            for i in self.drop_indexes:
                if not set(i.fields) & set(self.drop_fields):
                    sqls.append(f"ALTER TABLE {self.schema.name} DROP INDEX {i.name}")
        if sqls:
            sqls[-1] += ";"

        return ";\n".join(sqls)
