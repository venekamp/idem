import multiprocessing
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

import typer

from .config import CONFIG_FILENAME, AppConfig
from .index import index_command
from .index_db import IndexDB
from .IndexStore import IndexStore

idem_version: str = version(distribution_name="idem")
app: typer.Typer = typer.Typer(
    help=f"idem — identify identical files\n\nVersion: {idem_version}",
)


def print_version(is_version: bool) -> None:
    """
    Callback for the global --version / -V option.

    Typer passes a boolean to this callback indicating whether the user
    supplied the --version flag. If the option is not provided, the value
    is False and the callback should return immediately so normal command
    execution can continue.

    When the flag *is* provided, this function prints the installed
    version of the 'idem' package and then terminates the program early
    by raising `typer.Exit()`. This ensures that --version works
    consistently regardless of which subcommand is invoked.
    """
    if not is_version:
        return

    try:
        ver: str = version(distribution_name="idem")
    except PackageNotFoundError:
        ver = "unknown (package not installed)"

    typer.echo(ver)
    raise typer.Exit()


def parse_chunk_size(value: str) -> int:
    # Normalize
    text: str = value.strip().upper()

    last_char: str = text[-1]
    # Match "[number][optional suffix]"
    if last_char in {"K", "M", "G"}:
        number: str = text[:-1]
        suffix: str | None = last_char
    else:
        number = text
        suffix = None

    try:
        base: int = int(number)

        if suffix == "K":
            return base * 1024
        elif suffix == "M":
            return base * 1024 * 1024
        elif suffix == "G":
            return base * 1024 * 1024 * 1024
        else:
            return base
    except ValueError:
        raise ValueError("Specified chuck size is not a number. Only K, M and G are allowed suffixes.")


def initialize_db_root_dirs(db_path: Path, root_paths: list[Path]) -> None:
    with IndexDB(db_path) as db:
        index_store: IndexStore = IndexStore(db)
        try:
            db.begin()
            index_store.insert_root_dirs([str(path) for path in root_paths])
            db.commit()
        except Exception:
            db.rollback()
            raise


@app.command()
def init(
    source_paths: list[Path],
    max_workers: Annotated[int, typer.Option()] = 0,
    max_inflight: Annotated[int, typer.Option()] = 200,
    chunk_size: Annotated[int, typer.Option()] = 4096,
    batch_size: Annotated[int, typer.Option()] = 500,
    force: Annotated[bool, typer.Option()] = False,
) -> None:
    """
    Initialze a directory.

    Initialze a directory with a config and root directory in
    which files are soft linked to their original files from one or
    more source directories.
    """

    if CONFIG_FILENAME.exists() and not force:
        typer.echo("Config file already exists. Use --force to overwrite.")
        raise typer.Exit(code=1)

    cfg: AppConfig = AppConfig(
        root_paths=[p.resolve() for p in source_paths],
        db_path=Path("idem.db"),
        max_workers=max_workers if max_workers != 0 else multiprocessing.cpu_count(),
        max_inflight=max_inflight,
        chunk_size=chunk_size,
        batch_size=batch_size,
    )

    cfg.save(CONFIG_FILENAME)
    typer.echo(f"Config written to {CONFIG_FILENAME}")

    initialize_db_root_dirs(cfg.db_path, cfg.root_paths)


@app.command()
def index(
    max_workers: Annotated[int | None, typer.Option()] = None,
    max_inflight: Annotated[int | None, typer.Option()] = None,
    chunk_size: Annotated[
        str | None, typer.Option(help="Size in bytes, or with suffix K/M/G (e.g. 32K, 4M, 1G)")
    ] = None,
) -> None:
    """Index all files from the configured source paths."""
    try:
        cfg: AppConfig = AppConfig.load()

        if not cfg.db_path.exists():
            initialize_db_root_dirs(cfg.db_path, cfg.root_paths)

        if max_workers is not None:
            cfg.max_workers = max_workers
        if max_inflight is not None:
            cfg.max_inflight = max_inflight

        if chunk_size is not None:
            try:
                normalized_chunk_size: int = parse_chunk_size(chunk_size)
                cfg.chunk_size = normalized_chunk_size
            except ValueError as e:
                raise typer.BadParameter(str(e))

        index_command(cfg)
    except FileNotFoundError as e:
        typer.echo(e)


@app.command()
def analyze() -> None:
    """Analyze the index root directory for unresolved duplicates."""
    pass


@app.command()
def add(paths: list[str]) -> None:
    """Add source directories to the configuration."""
    with IndexDB(Path("idem.db")) as db:
        store: IndexStore = IndexStore(db)
        store.insert_root_dirs([path for path in paths])
    pass


@app.command()
def show() -> None:
    """Show the configured source directories."""
    pass


@app.command()
def status() -> None:
    """Show which directories have been indexed."""
    pass


@app.command(name="version")
def version_cmd() -> None:
    """Print the installed version of idem."""
    print_version(True)


@app.callback()
def main(
    _version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=print_version,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """
    Global options for idem. All subcommands run after this callback unless
    --version is used.
    """
    # No action needed; callback handles version printing
    return


if __name__ == "__main__":
    app()
