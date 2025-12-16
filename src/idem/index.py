import fcntl
import hashlib
import os
from collections.abc import Generator, Iterator
from concurrent.futures import Future, InterpreterPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import typer
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

from .config import AppConfig


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


def calculate_sha256(filename: str, chunk_size: int) -> str:
    hash_sha256 = hashlib.sha256()

    try:
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except Exception as e:
        return f"ERROR:{e}"


def collect_serials(target_dir: Path, hash: str) -> set[int]:
    """Return all used serial numbers for files starting with <hash>-XX."""

    used_serials: set[int] = {
        int(path.name[-2:])
        for path in target_dir.glob(pattern=f"{hash}-*")
        if len(path.name) > 3 and path.name[-3:-2] == "-"
    }

    return used_serials


def get_first_available_serial(serials: set[int]) -> int:
    """Return first available serial number, i.e. lowest number not in set."""
    serial: int = 1
    while serial in serials:
        serial += 1

    return serial


def get_original_path(path: Path) -> Path:
    """Return the path pointed to by <hash_base>-01, if it exists."""
    link: Path = path.with_name(f"{path.name}-01")
    try:
        return Path(os.readlink(link))
    except FileNotFoundError:
        return Path()


def link_hash_to_orignal_file(filename: str, hash: str, dest_path: Path, n_chars: int) -> None:
    """ """
    src_path: Path = Path(filename)

    if src_path.is_symlink():
        # Skip if the source is a soft link. It won't count as a duplicate.
        typer.echo(f"Skipping: {src_path} as it is a soft link.")
        return

    prefix: str = hash[:n_chars]
    target_dir: Path = dest_path / prefix
    target_file: Path = target_dir / hash

    used_serials: set[int] = collect_serials(target_dir, hash)
    serial: int = get_first_available_serial(used_serials)

    target_with_serial_number: Path = target_file.with_name(f"{target_file.name}-{serial:02d}")
    original_path: Path = get_original_path(target_file)

    hash_from_filename: str = target_with_serial_number.name[:-3]
    if (
        hash_from_filename == hash
        and src_path.name == original_path.name
        and src_path.stat().st_size == original_path.stat().st_size
    ):
        typer.echo(f"Skipping: {src_path} is duplicate.")
        return

    with dir_lock(target_dir):
        target_with_serial_number.symlink_to(filename)


def organize_by_sha256(
    src_path: Path,
    dest_path: Path,
    prefix_length: int,
    max_workers: int,
    max_in_flight: int,
    chunk_size: int,
) -> None:
    file_iter: Iterator[str] = find_all_files(path=src_path)  # generator, no memory explosion

    with (
        InterpreterPoolExecutor(max_workers=max_workers) as executor,
        Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            TextColumn("{task.completed} hashed"),
        ) as progress,
    ):
        task_id = progress.add_task("Hashing files...", total=None)
        futures: dict[Future[str], str] = {}
        in_flight = 0

        # Streaming submission loop
        for filename in file_iter:
            if os.path.getsize(filename) == 0:
                typer.echo(f"Skipping: {filename} has zero length.")
                continue

            # Backpressure: limit number of active futures
            while in_flight >= max_in_flight:
                for finished in as_completed(futures):
                    original_filename: str = futures.pop(finished)
                    hash_val: str = finished.result()
                    link_hash_to_orignal_file(original_filename, hash_val, dest_path, prefix_length)
                    progress.update(task_id, advance=1)
                    in_flight -= 1
                break

            fut: Future[str] = executor.submit(calculate_sha256, filename, chunk_size)
            futures[fut] = filename
            in_flight += 1

        # Drain remaining tasks
        for finished in as_completed(futures):
            original_filename = futures.pop(finished)
            hash_val = finished.result()
            link_hash_to_orignal_file(original_filename, hash_val, dest_path, prefix_length)
            progress.update(task_id, advance=1)


def populate_dest_path(dest_path: Path, prefix_length: int) -> None:
    total_number_of_dirs: int = cast(int, 16**prefix_length)

    for dir_number in range(total_number_of_dirs):
        dir_name: str = f"{dir_number:0{prefix_length}x}"
        path: Path = dest_path / dir_name
        os.makedirs(path, exist_ok=True)


    typer.echo(f"🖥️  Gebruik {cfg.max_workers} CPU cores")
def index_all_dirs(store: IndexStore, cfg: AppConfig) -> None:

    resolved_root_path: Path = cfg.root_path.resolve()
    resolved_root_path.mkdir(parents=True, exist_ok=True)

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
        )
def index_command(cfg: AppConfig) -> None:
    with IndexDB(cfg.db_path) as db:
        index_store: IndexStore = IndexStore(db)
        index_all_dirs(store=index_store, cfg=cfg)
