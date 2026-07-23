from .persistence import PersistenceLayer
from .rtypes import LogEntry
import json

class RaftLog:
    """
    Manages log entries with persistence and caching.
    Provides Raft-specific operations.
    """
    def __init__(self, persistence: PersistenceLayer):
        self.persistence = persistence
        self.cache = {} # index -> LogEntry
    
    def append(self, entries: list[LogEntry]):
        for entry in entries:
            self.persistence.append_log_entry(entry)
            self.cache[entry.index] = entry
    
    def get_entry(self, index: int) -> LogEntry | None:
        if index in self.cache:
            return self.cache[index]
        
        log_entry = self.persistence.get_log_entry(index=index)

        if log_entry:
            self.cache[log_entry.index] = log_entry
            return log_entry
        
        return None
    
    def get_entries(self, start_index: int, end_index: int) -> list[LogEntry]:
        """Get range of entries"""
        return self.persistence.get_log_entries(start_index=start_index, end_index=end_index)
    
    def truncate(self, index: int) -> None:
        """Delete entries from index onward"""
        self.persistence.cursor.execute("""
        DELETE from log_entries
        WHERE log_index >= ?
        """, (index,))
        # Remove from cache also
        self.cache = {k:v for k,v in self.cache.items() if k < index}
        self.persistence.conn.commit()
    
    def last_index(self) -> int:
        """Index of last entry"""
        return self.persistence.get_last_log_index()
    
    def last_term(self) -> int:
        """Term of last entry"""
        return self.persistence.get_last_log_term()
    
    def get_term(self, index: int) -> int:
        """Get term at specific index"""
        return self.persistence.get_term_at_index(index)