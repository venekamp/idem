import sqlite3
import time
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from .index_db import IndexDB
from .models import ContentStats, DirStats, FileMetadata, FileStats, IntegrityStats, StatusSnapshot
from .sql import dirs, files, hashes


class IndexStore:
    def __init__(self, index_db: IndexDB) -> None:
        self.db: IndexDB = index_db

    def fetchone(self, sql: str, params: Sequence[object]) -> sqlite3.Row:
        cursor: sqlite3.Cursor = self.db.connection.cursor()

        _ = cursor.execute(sql, params)
        row: sqlite3.Row | None = self.db.query_one(sql, params)
        assert row is not None

        cursor.close()

        return row

    def insert_root_dirs(self, root_dirs: list[str]) -> None:
        for root_path in root_dirs:
            scan_started_at: int = time.time_ns()
            self.db.execute(sql=dirs.INSERT_ROOT_DIR, params=(root_path, scan_started_at))

    def get_dir_id(self, path: str) -> int:
        row: sqlite3.Row | None = self.db.query_one(sql=dirs.SELECT_DIR_ID, params=(path,))

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

    def _get_dir_stats(self) -> DirStats:
        row: sqlite3.Row | None = self.db.query_one(
            """
            SELECT
            COUNT(*) AS total,
            SUM(status = 'pending')  AS pending,
            SUM(status = 'indexing') AS indexing,
            SUM(status = 'done')     AS done,
            MAX(CASE WHEN status = 'done' THEN last_seen END) AS last_completed_scan
            FROM dirs;
            """
        )

        assert row is not None

        return DirStats(
            total=cast(int, row["total"]),
            pending=cast(int, row["pending"]),
            indexing=cast(int, row["indexing"]),
            done=cast(int, row["done"]),
            last_completed_scan=cast(int, row["last_completed_scan"]),
        )

    def _get_file_stats(self, last_scan: int | None) -> FileStats:
        row = self.db.query_one(
            """
            SELECT
            COUNT(*) AS total_files,
            SUM(last_seen = ?) AS files_seen_last_scan,
            SUM(last_seen < ? OR last_seen IS NULL) AS stale_files,
            COALESCE(SUM(size), 0) AS total_bytes
            FROM files;
            """,
            params=(last_scan, last_scan),
        )

        assert row is not None

        return FileStats(
            total_files=cast(int, row["total_files"]),
            files_seen_last_scan=cast(int, row["files_seen_last_scan"] or 0),
            stale_files=cast(int, row["stale_files"] or 0),
            total_bytes=cast(int, row["total_bytes"]),
        )

    def _get_content_stats(self) -> ContentStats:
        row = self.db.query_one(
            """
            WITH per_hash AS (
            SELECT hash_id, COUNT(*) AS n
            FROM files
            GROUP BY hash_id
            ),
            ambiguous AS (
            SELECT hash_id
            FROM files
            GROUP BY hash_id
            HAVING COUNT(DISTINCT size) > 1
            )
            SELECT
            SUM(n = 1) AS unique_hashes,
            SUM(n > 1) AS duplicate_groups,
            COALESCE(SUM(CASE WHEN n > 1 THEN n ELSE 0 END), 0) AS duplicate_files,
            (SELECT COUNT(*) FROM ambiguous) AS unresolved_groups,
            (SELECT COUNT(*) FROM files WHERE hash_id IN (SELECT hash_id FROM ambiguous))
                AS unresolved_files
            FROM per_hash;
            """
        )

        assert row is not None

        return ContentStats(
            unique_hashes=cast(int, row["unique_hashes"] or 0),
            duplicate_groups=cast(int, row["duplicate_groups"] or 0),
            duplicate_files=cast(int, row["duplicate_files"] or 0),
            unresolved_groups=cast(int, row["unresolved_groups"]),
            unresolved_files=cast(int, row["unresolved_files"]),
        )

    def _get_integrity_stats(self) -> IntegrityStats:
        row = self.db.query_one(
            """
            SELECT
            (SELECT COUNT(*) FROM files WHERE dir_id NOT IN (SELECT id FROM dirs))
                AS orphaned_files,
            (SELECT COUNT(*) FROM hashes
                WHERE id NOT IN (SELECT DISTINCT hash_id FROM files))
                AS orphaned_hashes;
            """
        )

        assert row is not None

        return IntegrityStats(
            orphaned_files=cast(int, row["orphaned_files"]),
            orphaned_hashes=cast(int, row["orphaned_hashes"]),
        )

    def get_status_snapshot(self) -> StatusSnapshot:
        dirs: DirStats = self._get_dir_stats()
        files: FileStats = self._get_file_stats(dirs.last_completed_scan)
        content: ContentStats = self._get_content_stats()
        integrity: IntegrityStats = self._get_integrity_stats()

        return StatusSnapshot(
            last_completed_scan=dirs.last_completed_scan,
            indexing_in_progress=dirs.indexing > 0,
            total_dirs=dirs.total,
            pending_dirs=dirs.pending,
            indexing_dirs=dirs.indexing,
            done_dirs=dirs.done,
            total_files=files.total_files,
            files_seen_last_scan=files.files_seen_last_scan,
            stale_files=files.stale_files,
            total_bytes=files.total_bytes,
            unique_hashes=content.unique_hashes,
            duplicate_groups=content.duplicate_groups,
            duplicate_files=content.duplicate_files,
            unresolved_groups=content.unresolved_groups,
            unresolved_files=content.unresolved_files,
            orphaned_files=integrity.orphaned_files,
            orphaned_hashes=integrity.orphaned_hashes,
        )
