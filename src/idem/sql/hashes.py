CREATE_TABLE: str = """
    CREATE TABLE IF NOT EXISTS hashes (
        id    INTEGER PRIMARY KEY,
        hash  TEXT NOT NULL UNIQUE,
        size  INTEGER NOT NULL
    );
"""

GET_HASH_ID: str = """SELECT id from hashes WHERE hash = ?;"""

STORE_HASH: str = """
    INSERT INTO hashes(hash, size)
        VALUES (?, ?)
        ON CONFLICT(hash) DO NOTHING
    ;
"""
