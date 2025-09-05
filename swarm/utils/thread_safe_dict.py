import threading
from typing import TypeVar, Generic, Optional, Iterator

K = TypeVar('K')
V = TypeVar('V')


class ThreadSafeDict(Generic[K, V]):
    """
    A thread-safe dictionary with locking around all access/modification.

    Example:
        ts_dict = ThreadSafeDict[str, int]()
        ts_dict.set("key1", 123)
        value = ts_dict.get("key1")
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._store: dict[K, V] = {}

    def set(self, key: K, value: V) -> None:
        """Set a key to a value in a thread-safe manner."""
        with self._lock:
            self._store[key] = value

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get the value for a key in a thread-safe manner."""
        with self._lock:
            return self._store.get(key, default)

    def remove(self, key: K) -> None:
        """Remove a key from the dictionary, if it exists."""
        with self._lock:
            if key in self._store:
                del self._store[key]

    def contains(self, key: K) -> bool:
        """Check if a key exists in the dictionary."""
        with self._lock:
            return key in self._store

    def size(self) -> int:
        """Return the number of items in the dictionary."""
        with self._lock:
            return len(self._store)

    def items(self) -> list[tuple[K, V]]:
        """Return all key-value pairs as a list."""
        with self._lock:
            return list(self._store.items())

    def keys(self) -> list[K]:
        """Return all keys in the dictionary."""
        with self._lock:
            return list(self._store.keys())

    def values(self) -> list[V]:
        """Return all values in the dictionary."""
        with self._lock:
            return list(self._store.values())

    def clear(self) -> None:
        """Remove all items from the dictionary."""
        with self._lock:
            self._store.clear()

    def __contains__(self, key: K) -> bool:
        return self.contains(key)

    def __len__(self) -> int:
        return self.size()

    def __iter__(self) -> Iterator[K]:
        with self._lock:
            return iter(self._store.copy())
