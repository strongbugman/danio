import importlib.metadata

from . import manage
from .database import Database
from .exception import SchemaException, ValidateException
from .model import Model, id_to_many, id_to_one, model
from .schema import (
    BigIntField,
    BlobField,
    BoolField,
    BytesField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    Field,
    FloatField,
    IntField,
    JsonField,
    Migration,
    Operation,
    RelationField,
    Schema,
    SmallIntField,
    TextField,
    TimeField,
    TinyIntField,
    V,
    field,
)

try:
    __version__ = importlib.metadata.version("danio")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.5.1"

__all__ = (
    "BigIntField",
    "BlobField",
    "BoolField",
    "BytesField",
    "CharField",
    "Database",
    "DateField",
    "DateTimeField",
    "DecimalField",
    "Field",
    "FloatField",
    "IntField",
    "JsonField",
    "Migration",
    "Model",
    "Operation",
    "RelationField",
    "Schema",
    "SchemaException",
    "SmallIntField",
    "TextField",
    "TimeField",
    "TinyIntField",
    "V",
    "ValidateException",
    "__version__",
    "field",
    "id_to_many",
    "id_to_one",
    "manage",
    "model",
)
