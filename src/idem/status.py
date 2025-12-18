from datetime import datetime

import typer

from .config import AppConfig
from .index_db import IndexDB
from .IndexStore import IndexStore
from .models import StatusSnapshot


def _format_ns(ts: int | None) -> str:
    if ts is None:
        return "never"
    return datetime.fromtimestamp(ts / 1e9).isoformat(timespec="seconds")


def status_command() -> None:
    """
    Show indexing status and high-level statistics.
    """
    cfg: AppConfig = AppConfig.load()

    with IndexDB(path=cfg.db_path) as db:
        store: IndexStore = IndexStore(index_db=db)
        s: StatusSnapshot = store.get_status_snapshot()

    typer.echo("Index status")
    typer.echo("------------")
    typer.echo(f"Last completed scan: {_format_ns(s.last_completed_scan)}")
    typer.echo(f"Index in progress:   {'yes' if s.indexing_in_progress else 'no'}")

    typer.echo("\nDirectories")
    typer.echo("-----------")
    typer.echo(f"Total discovered:    {s.total_dirs}")
    typer.echo(f"Pending:             {s.pending_dirs}")
    typer.echo(f"Indexing:            {s.indexing_dirs}")
    typer.echo(f"Done:                {s.done_dirs}")

    typer.echo("\nFiles")
    typer.echo("-----")
    typer.echo(f"Files indexed:       {s.total_files}")
    typer.echo(f"Seen last scan:      {s.files_seen_last_scan}")
    typer.echo(f"Stale files:         {s.stale_files}")
    typer.echo(f"Total size:          {s.total_bytes:,} bytes")

    typer.echo("\nContent")
    typer.echo("-------")
    typer.echo(f"Unique files:        {s.unique_hashes}")
    typer.echo(f"Duplicate groups:    {s.duplicate_groups}")
    typer.echo(f"Duplicate files:     {s.duplicate_files}")

    typer.echo("\nAttention required")
    typer.echo("------------------")
    if s.unresolved_groups == 0:
        typer.echo("No unresolved duplicates.")
    else:
        typer.echo(f"Unresolved duplicates: {s.unresolved_groups} groups ({s.unresolved_files} files)")

    if s.orphaned_files or s.orphaned_hashes:
        typer.echo("\nIntegrity warnings")
        typer.echo("------------------")
        typer.echo(f"Orphaned files:      {s.orphaned_files}")
        typer.echo(f"Orphaned hashes:     {s.orphaned_hashes}")
