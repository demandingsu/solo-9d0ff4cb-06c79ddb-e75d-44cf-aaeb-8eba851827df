"""SQLite 存储层（aiosqlite 单连接，内部串行化，多协程共享安全）。"""
import os

import aiosqlite

DATA_DIR = os.environ.get("DATA_DIR", ".")
DB_PATH = os.path.join(DATA_DIR, "collector.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_url      TEXT NOT NULL,
    biz_date        TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'connecting',
    error           TEXT,
    epoch           INTEGER NOT NULL DEFAULT 0,
    events_received INTEGER NOT NULL DEFAULT 0,
    pages_fetched   INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS tickets (
    job_id      INTEGER NOT NULL,
    ticket_no   TEXT    NOT NULL,
    revision    INTEGER NOT NULL,
    plate       TEXT    NOT NULL,
    net_weight  REAL    NOT NULL,
    voided      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, ticket_no)
);
"""

_conn: aiosqlite.Connection | None = None


async def conn() -> aiosqlite.Connection:
    global _conn
    if _conn is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        _conn = await aiosqlite.connect(DB_PATH)
        _conn.row_factory = aiosqlite.Row
        # WAL：分析端的只读连接与采集写入互不阻塞；忙时最多等待 5s
        await _conn.execute("PRAGMA journal_mode=WAL")
        await _conn.execute("PRAGMA busy_timeout=5000")
        await _conn.executescript(SCHEMA)
        await _conn.commit()
    return _conn
