import sqlite3
import time
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from .index_db import IndexDB
from .models import FileMetadata
from .sql import dirs, files, hashes


class IndexStore:
    def __init__(self, index_db: IndexDB) -> None:
        self.db: IndexDB = index_db

    def fetchone(self, sql: str, params: Sequence[object]) -> sqlite3.Row:
        cursor: sqlite3.Cursor = self.db.connection.cursor()

        _ = cursor.execute(sql, params)
        row: sqlite3.Row | None = self.query_one(sql, params)
        assert row is not None

        cursor.close()

        return row

    def insert_root_dirs(self, root_dirs: list[str]) -> None:
        for root_path in root_dirs:
            scan_started_at: int = time.time_ns()
            self.db.execute(sql=dirs.INSERT_ROOT_DIR, params=(root_path, scan_started_at))

    def get_dir_id(self, path: str) -> int:
        row: sqlite3.Row | None = self.query_one(sql=dirs.SELECT_DIR_ID, params=(path,))

        assert row is not None

        return cast(int, row["id"])

    def upsert_dirs(self, *, path: Path, root_id: int) -> None:
        self.db.execute(
            sql=dirs.UPSERT_DIRS,
            params=(
                str(path),
                root_id,
            ),
        )

    def upset_file_metadata(
        self,
        *,
        path: Path,
        dir_id: int,
        size: int,
        mtime_ns: int,
        inode: int,
        device: int,
        hash_id: int,
    ) -> None:
        self.db.execute(
            sql=files.UPSERT_FILE_METEDATA, params=(path, dir_id, size, mtime_ns, inode, device, hash_id)
        )

    def get_or_create_hash(self, hash_hex: str, file_size: int) -> int:
        self.db.execute(sql=hashes.STORE_HASH, params=(hash_hex, file_size))
        row: sqlite3.Row | None = self.db.query_one(sql=hashes.GET_HASH_ID, params=(hash_hex,))

        assert row is not None

        return cast(int, row["id"])

    def upsert_file_metadata(self, meta: FileMetadata) -> None:
        self.db.execute(
            sql=files.UPSERT_FILE_METEDATA,
            params=(
                meta.path,
                meta.dir_id,
                meta.size,
                meta.mtime_ns,
                meta.inode,
                meta.device,
                meta.hash_id,
            ),
        )

    def mark_files_seen_in_dir(self, dir_id: int, seen_at: int) -> None:
        self.db.execute(sql=files.UPDATE_LAST_SEEN, params=(dir_id, seen_at))

    def begin(self) -> None:
        self.db.begin()

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()

    def reset_inflight_dirs(self) -> None:
        """
        Reset directories that were left in 'indexing' state due to
        an interrupted run back to 'pending'.
        """
        self.db.execute(sql=dirs.RESET_INFLIGHT_DIR)

    def get_next_pending_dir(self) -> sqlite3.Row | None:
        """
        Return one directory with status='pending', or None if none remain.
        """

        row: sqlite3.Row | None = self.db.query_one(sql=dirs.GET_NEXT_PENDING_DIR)

        return row

    def mark_dir_indexing(self, dir_id: int) -> None:
        """
        Mark a directory as currently being indexed.
        """
        self.db.execute(sql=dirs.MARK_AS_INDEXING, params=(dir_id,))

    def mark_dir_done(self, dir_id: int, *, seen_at: int) -> None:
        """
        Mark a directory as fully indexed for this scan.
        """
        self.db.execute(sql=dirs.MARK_AS_DONE, params=(seen_at, dir_id))

    def insert_dir(self, *, path: str) -> None:
        """
        Insert a directory into the dirs table if it does not already exist.
        """
        self.db.execute(sql=dirs.INSERT_DIR, params=(path,))
