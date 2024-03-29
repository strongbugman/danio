import enum

from databases import Database as _Database

from . import exception


class Database(_Database):
    class Type(enum.Enum):
        MYSQL = "mysql"
        POSTGRES = "postgres"
        SQLITE = "sqlite"

        def quote(self, content: str) -> str:
            qt = "`"
            if self == self.POSTGRES:
                qt = '"'

            return f"{qt}{content}{qt}"

    SUPPORTED_BACKENDS = {
        "mysql": "danio.mysql:MySQLBackend",
        "sqlite": "danio.sqlite:SQLiteBackend",
        "postgres": "danio.asyncpg:PostgresBackend",
        "aiopg": "danio.aiopg:PostgresBackend",
    }

    @property
    def type(self) -> Type:
        for t in self.Type:
            if t.value in str(self._backend).lower():
                return t
        raise exception.SchemaException(f"Can't determine {self._backend}'s type")
