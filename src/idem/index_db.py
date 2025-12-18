import sqlite3
from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import cast

from .sql import dirs, files, hashes


class IndexDB:
    def __init__(self, path: Path) -> None:
        self.connection: sqlite3.Connection = sqlite3.connect(
            path, detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row

        self._configure()
        self._create_schema_if_needed()

    def __enter__(self) -> "IndexDB":
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.connection.close()

    def _configure(self) -> None:
        cursor: sqlite3.Cursor = self.connection.cursor()

        statements: list[str] = [
            "PRAGMA journal_mode = WAL;",
            "PRAGMA synchronous = NORMAL;",
            "PRAGMA foreign_keys = ON;",
        ]

        for statement in statements:
            _ = cursor.execute(statement)

    def _create_schema_if_needed(self) -> None:
        self.begin()

        self._apply_schema(table_sql=dirs.CREATE_TABLE, index_sql=dirs.CREATE_INDEXES)
        self._apply_schema(table_sql=hashes.CREATE_TABLE)
        self._apply_schema(table_sql=files.CREATE_TABLE)

        self.commit()

    def _apply_schema(self, *, table_sql: str, index_sql: Sequence[str] | None = None) -> None:
        self.execute(sql=table_sql)

        if index_sql is not None:
            for sql in index_sql:
                self.execute(sql)

    def cursor(self) -> sqlite3.Cursor:
        return self.connection.cursor()

    def begin(self) -> None:
        _ = self.connection.execute("BEGIN;")

    def commit(self) -> None:
        _ = self.connection.execute("COMMIT;")

    def rollback(self) -> None:
        _ = self.connection.execute("ROLLBACK;")

    def execute(self, sql: str, params: Sequence[object] | None = None) -> None:
        cursor: sqlite3.Cursor = self.connection.cursor()

        if params is not None:
            _ = cursor.execute(sql, params)
        else:
            _ = cursor.execute(sql)

    def close(self) -> None:
        self.connection.close()

    def fetchone(self, sql: str, params: Sequence[object]) -> object:
        self.execute(sql=sql, params=params)

    def query_one(self, sql: str, params: Sequence[object] | None = None) -> sqlite3.Row | None:
        cursor: sqlite3.Cursor = self.connection.cursor()

        if params is None:
            _ = cursor.execute(sql)
        else:
            _ = cursor.execute(sql, params)
        row: sqlite3.Row | None = cast(sqlite3.Row | None, cursor.fetchone())

        cursor.close()

        return row
