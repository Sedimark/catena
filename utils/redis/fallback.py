"""
In-memory key-value storage fallback when Redis is unavailable.
"""

import fnmatch
from typing import Dict, Tuple, List, Optional


class MemoryKV:
    """In-memory key-value store with Redis-like interface."""
    
    def __init__(self):
        self._store: Dict[str, str] = {}
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        return self._store.get(key)
    
    def set(self, key: str, value: str) -> None:
        """Set key-value pair."""
        self._store[key] = value
    
    def delete(self, key: str) -> int:
        """Delete key, return number of keys deleted."""
        if key in self._store:
            del self._store[key]
            return 1
        return 0
    
    def exists(self, key: str) -> int:
        """Check if key exists."""
        return 1 if key in self._store else 0
    
    def scan(self, cursor: int = 0, match: Optional[str] = None, count: int = 100) -> Tuple[int, List[str]]:
        """Scan keys with pattern matching."""
        keys = list(self._store.keys())
        if match:
            keys = [k for k in keys if fnmatch.fnmatch(k, match)]
        # Simple implementation: return all keys in one batch
        return 0, keys
    
    def flushdb(self) -> None:
        """Clear all keys."""
        self._store.clear()
    
    def ping(self) -> bool:
        """Health check - always returns True for in-memory store."""
        return True
