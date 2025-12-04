from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

import typer

from .config import CONFIG_FILE, AppConfig
from .index import index_command

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


@app.command()
def init(
    source_paths: list[Path],
    root: Annotated[str, typer.Option()] = "root",
    prefix_length: Annotated[int, typer.Option()] = 3,
    max_workers: Annotated[int, typer.Option()] = 0,
    max_inflight: Annotated[int, typer.Option()] = 200,
    chunk_size: Annotated[int, typer.Option()] = 4096,
    force: Annotated[bool, typer.Option()] = False,
) -> None:
    """
    Initialze a directory.

    Initialze a directory with a config and root directory in
    which files are soft linked to their original files from one or
    more source directories.
    """
    root_path: Path = Path(root)

    if CONFIG_FILE.exists() and not force:
        typer.echo("Config file already exists. Use --force to overwrite.")
        raise typer.Exit(code=1)

    cfg: AppConfig = AppConfig(
        prefix_length=prefix_length,
        source_paths=[p for p in source_paths],
        root_path=root_path,
        max_workers=max_workers,
        max_inflight=max_inflight,
        chunk_size=chunk_size,
    )

    cfg.save(CONFIG_FILE)
    typer.echo(f"Config written to {CONFIG_FILE}")


@app.command()
def index(
    max_workers: Annotated[int | None, typer.Option()] = None,
    max_inflight: Annotated[int | None, typer.Option()] = None,
    chunk_size: Annotated[
        str | None, typer.Option(help="Size in bytes, or with suffix K/M/G (e.g. 32K, 4M, 1G)")
    ] = None,
) -> None:
    """Index all files from the configured source paths."""
    cfg: AppConfig = AppConfig.load()

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


@app.command()
def analyze() -> None:
    """Analyze the index root directory for unresolved duplicates."""
    pass


@app.command()
def add() -> None:
    """Add source directory to the configuration."""
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
