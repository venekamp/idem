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
