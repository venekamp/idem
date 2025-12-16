import fcntl
import hashlib
import os
import sqlite3
import time
from collections.abc import Generator, Iterator
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, cast

import typer

from .config import AppConfig
from .index_db import IndexDB
from .IndexStore import IndexStore
from .models import FileMetadata


@contextmanager
def dir_lock(path: Path) -> Generator[None, None, None]:
    lock_file: Path = path / ".lock-duplicate"
    lock_file.touch()

    try:
        with open(file=lock_file, mode="w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            yield
    finally:
        lock_file.unlink(missing_ok=True)


def find_all_files(path: Path) -> Iterator[str]:
    """
    Recursively yield all file paths under the given directory.

    Resolves the provided path, validates that it exists and is a
    directory, and then yields file paths relative to the root
    directory.

    Parameters
    ----------
    path : Path
        The directory to search in.

    Yields
    ------
    str
        Relative file paths.

    Raises
    ------
    ValueError
        If the path does not exist or is not a directory.
    """
    top_path: Path = path.resolve()

    if not top_path.exists():
        raise ValueError(f"{top_path} does not exists.")
    if not top_path.is_dir():
        raise ValueError(f"{top_path} is not a directorie.")

    for root, _, files in os.walk(top_path):
        for filename in files:
            full_path: Path = Path(root) / filename
            yield str(full_path)


def calculate_sha256(path: Path, chunk_size: int) -> str:
    hash = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash.update(chunk)
    return hash.hexdigest()


def hash_files_parallel_bounded(
    paths: Iterable[Path], max_workers: int, max_in_flight: int, chunk_size: int
) -> Iterator[tuple[Path, str]]:
    if max_in_flight <= 0:
        raise ValueError("max_in_flight must be > 0")
    if max_workers <= 0:
        raise ValueError("max_workers must be > 0")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        in_flight: dict[Future[str], Path] = {}

        def submit(path: Path) -> None:
            future: Future[str] = executor.submit(calculate_sha256, path, chunk_size)
            in_flight[future] = path

        for path in paths:
            # Apply backpressure
            while len(in_flight) >= max_in_flight:
                done: Future[str] = next(as_completed(in_flight))
                yield in_flight.pop(done), done.result()

            submit(path)

        # Drain remaining futures
        for future in as_completed(in_flight):
            yield in_flight[future], future.result()


def walk_files(root: Path) -> Iterator[Path]:
    resolved_root: Path = root.resolve()

    if not resolved_root.exists():
        raise ValueError(f"{resolved_root} does not exist")
    if not resolved_root.is_dir():
        raise ValueError(f"{resolved_root} is not a directory")

    for dirpath, _, filenames in os.walk(resolved_root):
        base: Path = Path(dirpath)
        for name in filenames:
            path: Path = base / name
            if path.is_symlink():
                print(f"Symlink: {path}")
                continue

            if path.stat().st_size == 0:
                continue

            try:
                _ = path.stat()
            except FileNotFoundError:
                continue

            yield base / name


def discover_dir_entries(path: Path) -> tuple[list[Path], list[Path]]:
    """
    Return immediate subdirectories and files of `path`.

    Symlinks and zero-length files are ignored.
    """
    subdirs: list[Path] = []
    files: list[Path] = []

    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_symlink():
                    continue

                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    try:
                        st = entry.stat()
                    except FileNotFoundError:
                        continue

                    if st.st_size == 0:
                        continue

                    files.append(Path(entry.path))
    except FileNotFoundError:
        # Directory disappeared between discovery and processing
        pass

    return subdirs, files


def index_files_in_dir(
    *,
    store: IndexStore,
    dir_id: int,
    files: list[Path],
    max_workers: int,
    max_in_flight: int,
    chunk_size: int,
    batch_size: int,
) -> None:
    processed: int = 0

    for file_path, hash_hex in hash_files_parallel_bounded(
        paths=files,
        max_workers=max_workers,
        max_in_flight=max_in_flight,
        chunk_size=chunk_size,
    ):
        try:
            st = file_path.stat()
        except (FileNotFoundError, PermissionError):
            continue

        hash_id = store.get_or_create_hash(
            hash_hex=hash_hex,
            file_size=st.st_size,
        )

        meta = FileMetadata(
            path=str(file_path),
            dir_id=dir_id,
            size=st.st_size,
            mtime_ns=st.st_mtime_ns,
            inode=st.st_ino,
            device=st.st_dev,
            hash_id=hash_id,
        )

        store.upsert_file_metadata(meta)

        processed += 1
        if processed >= batch_size:
            store.commit()
            store.begin()
            processed = 0

    if processed > 0:
        store.commit()
        store.begin()


def index_single_dir(
    *,
    store: IndexStore,
    dir_id: int,
    dir_path: Path,
    seen_at: int,
    max_workers: int,
    max_in_flight: int,
    chunk_size: int,
    batch_size: int,
) -> None:
    """
    Index exactly one directory.

    - Discover immediate subdirectories and enqueue them
    - Hash files in this directory only
    - Upsert file metadata
    - Mark files as seen for this scan
    """

    subdirs: list[Path] = []
    files: list[Path] = []

    subdirs, files = discover_dir_entries(path=dir_path)

    for subdir in subdirs:
        store.insert_dir(path=str(subdir))

    index_files_in_dir(
        store=store,
        dir_id=dir_id,
        files=files,
        max_workers=max_workers,
        max_in_flight=max_in_flight,
        chunk_size=chunk_size,
        batch_size=batch_size,
    )

    # --- finalize directory ---
    store.mark_files_seen_in_dir(
        dir_id=dir_id,
        seen_at=seen_at,
    )


def index_all_dirs(store: IndexStore, cfg: AppConfig) -> None:
    scan_started_at: int = time.time_ns()

    store.begin()
    store.reset_inflight_dirs()
    store.commit()

    store.begin()
    while True:
        dir_row: sqlite3.Row | None = store.get_next_pending_dir()

        if dir_row is None:
            break

        dir_id: int = cast(int, dir_row["id"])
        dir_path: str = cast(str, dir_row["path"])
        print(f"Indexing dir: {dir_path}")

        store.mark_dir_indexing(dir_id=dir_id)

        index_single_dir(
            store=store,
            dir_id=dir_id,
            dir_path=Path(dir_path),
            seen_at=scan_started_at,
            max_workers=cfg.max_workers,
            max_in_flight=cfg.max_inflight,
            chunk_size=cfg.chunk_size,
            batch_size=500,
        )

        store.mark_dir_done(dir_id, seen_at=scan_started_at)

    store.commit()


def index_command(cfg: AppConfig) -> None:
    typer.echo(f"🖥️  Gebruik {cfg.max_workers} CPU cores")

    with IndexDB(cfg.db_path) as db:
        index_store: IndexStore = IndexStore(db)
        index_all_dirs(store=index_store, cfg=cfg)
