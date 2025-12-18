from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FileMetadata:
    path: str
    dir_id: int
    size: int
    mtime_ns: int
    inode: int
    device: int
    hash_id: int


@dataclass(slots=True)
class DirStats:
    total: int
    pending: int
    indexing: int
    done: int
    last_completed_scan: int | None


@dataclass(frozen=True)
class FileStats:
    total_files: int
    files_seen_last_scan: int
    stale_files: int
    total_bytes: int


@dataclass(frozen=True)
class ContentStats:
    unique_hashes: int
    duplicate_groups: int
    duplicate_files: int
    unresolved_groups: int
    unresolved_files: int


@dataclass(frozen=True)
class IntegrityStats:
    orphaned_files: int
    orphaned_hashes: int


@dataclass(frozen=True)
class StatusSnapshot:
    # Index state
    last_completed_scan: int | None
    indexing_in_progress: bool

    # Directories
    total_dirs: int
    pending_dirs: int
    indexing_dirs: int
    done_dirs: int

    # Files
    total_files: int
    files_seen_last_scan: int
    stale_files: int
    total_bytes: int

    # Content
    unique_hashes: int
    duplicate_groups: int
    duplicate_files: int

    # Attention required
    unresolved_groups: int
    unresolved_files: int

    # Integrity
    orphaned_files: int
    orphaned_hashes: int
