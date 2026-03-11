import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple
import time

MEMORY_DB = Path("memory.db")


@dataclass
class Message:
    role: str
    content: str


class MemoryStore:
    def __init__(self, path: Path = MEMORY_DB) -> None:
        self.path = path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    ts INTEGER NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def add_message(self, role: str, content: str) -> None:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO messages (role, content, ts) VALUES (?, ?, ?)",
                (role, content, int(time.time())),
            )
            conn.commit()

    def get_recent(self, limit: int = 15) -> List[Message]:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT role, content FROM messages ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows: List[Tuple[str, str]] = cur.fetchall()
        rows.reverse()
        return [Message(role=r, content=c) for r, c in rows]

    def get_summary(self) -> str:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT value FROM meta WHERE key = 'summary'")
            row = cur.fetchone()
        return row[0] if row else ""

    def count_messages(self) -> int:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(1) FROM messages")
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def set_summary(self, text: str) -> None:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO meta (key, value) VALUES ('summary', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (text,),
            )
            conn.commit()

    def reset(self) -> None:
        with sqlite3.connect(self.path) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM messages")
            cur.execute("DELETE FROM meta WHERE key = 'summary'")
            conn.commit()
