CREATE_TABLE: str = """
    CREATE TABLE IF NOT EXISTS files (
        id        INTEGER PRIMARY KEY,
        path      TEXT NOT NULL UNIQUE,
        dir_id    INTEGER NOT NULL,
        size      INTEGER NOT NULL,
        mtime_ns  INTEGER NOT NULL,
        inode     INTEGER NOT NULL,
        device    INTEGER NOT NULL,
        hash_id   INTEGER NOT NULL,
        last_seen INTEGER
    );
"""

UPSERT_FILE_METEDATA: str = """
    INSERT INTO files (
        path,
        dir_id,
        size,
        mtime_ns,
        inode,
        device,
        hash_id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(path) DO UPDATE SET
        dir_id    = excluded.dir_id,
        size      = excluded.size,
        mtime_ns  = excluded.mtime_ns,
        inode     = excluded.inode,
        device    = excluded.device,
        hash_id   = excluded.hash_id
    WHERE
        files.dir_id    != excluded.dir_id
        OR files.size      != excluded.size
        OR files.mtime_ns  != excluded.mtime_ns
        OR files.inode     != excluded.inode
        OR files.device    != excluded.device
        OR files.hash_id   != excluded.hash_id;
"""

UPDATE_LAST_SEEN: str = """
    UPDATE files
    SET last_seen = ?
    WHERE dir_id = ?;
"""
