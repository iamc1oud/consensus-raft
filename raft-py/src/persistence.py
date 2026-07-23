import json
from .rtypes import LogEntry
from typing import Optional
import sqlite3


class PersistenceLayer:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self) -> None:
        self.cursor.execute("""
            -- Persistent state
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            -- key='current_term', value=int
            -- key='voted_for', value=str or null
            """)

        self.cursor.execute("""
            -- Log entries
            CREATE TABLE IF NOT EXISTS log_entries (
                log_index INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                command TEXT NOT NULL,  -- JSON
                timestamp REAL NOT NULL
            );
            """)

        self.cursor.execute("""
            -- Snapshots (for later)
            CREATE TABLE IF NOT EXISTS snapshots (
                last_included_index INTEGER PRIMARY KEY,
                last_included_term INTEGER NOT NULL,
                state TEXT NOT NULL  -- JSON of entire state machine
            );
            """)


    def save_term(self, term: int):
        self.cursor.execute("""
            INSERT OR REPLACE INTO metadata (key, value)
            VALUES ('current_term', ?)
            """, (term,))
        self.conn.commit()

    def load_term(self) -> int:
        self.cursor.execute("""
            SELECT value FROM metadata WHERE key='current_term'
            """)
        result = self.cursor.fetchone()
        return int(result[0]) if result else 0

    def save_voted_for(self, node_id: str | None) -> None:
        self.cursor.execute("""
            INSERT OR REPLACE INTO metadata (key, value)
            VALUES ('voted_for', ?)
            """, (node_id,))
        self.conn.commit()

    def load_voted_for(self) -> Optional[str]:
        self.cursor.execute("""
            SELECT value FROM metadata WHERE key='voted_for'
            """)
        result = self.cursor.fetchone()
        return result[0] if result else None

    def append_log_entry(self, entry: LogEntry) -> None:
        self.cursor.execute("""
            INSERT INTO log_entries (log_index, term, command, timestamp)
            VALUES (?, ?, ?, ?)
            """, (entry.index, entry.term, json.dumps(entry.command), entry.timestamp))
        self.conn.commit()

    def get_log_entries(self, start_index: int, end_index: int) -> list[LogEntry]:
        self.cursor.execute("""
            SELECT log_index, term, command, timestamp FROM log_entries
            WHERE log_index BETWEEN ? AND ?
            """, (start_index, end_index))
        results = self.cursor.fetchall()
        return [LogEntry(index=row[0], term=row[1], command=json.loads(row[2]), timestamp=row[3]) for row in results] if results else []

    def get_last_log_index(self) -> int:
        self.cursor.execute("""
            SELECT MAX(log_index) FROM log_entries
            """)
        result = self.cursor.fetchone()
        return result[0] if result[0] is not None else 0

    def get_last_log_term(self) -> int:
        self.cursor.execute("""
            SELECT MAX(term) FROM log_entries
            """)
        result = self.cursor.fetchone()
        return result[0] if result[0] is not None else 0

    def get_term_at_index(self, index: int) -> int:
        self.cursor.execute("""
            SELECT term FROM log_entries
            WHERE log_index = ?
            """, (index,))
        result = self.cursor.fetchone()
        return result[0] if result[0] is not None else 0
    
    def get_log_entry(self, index: int) -> LogEntry | None:
        self.cursor.execute("""
            SELECT * FROM log_entries
            WHERE log_index = ?
            """, (index,))
        
        row = self.cursor.fetchone()
        
        return LogEntry(index=row[0], term=row[1], command=json.loads(row[2]), timestamp=row[3]) if row[0] is not None else None