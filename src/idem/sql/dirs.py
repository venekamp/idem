CREATE_TABLE: str = """
CREATE TABLE IF NOT EXISTS dirs (
    id        INTEGER PRIMARY KEY,
    path      TEXT NOT NULL UNIQUE,
    status    TEXT NOT NULL CHECK (status IN ('pending', 'indexing', 'done')),
    last_seen INTEGER
);
"""
CREATE_INDEXES: tuple[str, ...] = ("CREATE INDEX IF NOT EXISTS idx_dirs_status ON dirs(status);",)

INSERT_ROOT_DIR: str = """
    INSERT INTO dirs (
        path,
        status,
        last_seen
    )
    VALUES (?, 'pending', ?)
    ON CONFLICT(path) DO NOTHING;
"""

SELECT_DIR_ID: str = """SELECT id from dirs WHERE path = ?;"""

UPSERT_DIRS: str = """
    INSERT INTO dirs (
        path,
        root_id,
        status
    )
    VALUES (?, ?, 'pending')
    ON CONFLICT(path) DO NOTHING;
"""

RESET_INFLIGHT_DIR: str = """
    UPDATE dirs
    SET status = 'pending'
    WHERE status = 'indexing';
"""

GET_NEXT_PENDING_DIR = """
    SELECT id, path
    FROM dirs
    WHERE status = 'pending'
    ORDER BY id
    LIMIT 1;
"""

MARK_AS_INDEXING: str = """
    UPDATE dirs
    SET status = 'indexing'
    WHERE id = ?;
"""

MARK_AS_DONE: str = """
    UPDATE dirs
    SET status = 'done',
        last_seen = ?
    WHERE id = ?;
"""

INSERT_DIR: str = """
    INSERT INTO dirs (path, status)
    VALUES (?, 'pending')
    ON CONFLICT(path) DO NOTHING;
"""
