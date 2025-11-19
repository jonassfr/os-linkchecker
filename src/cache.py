# src/cache.py

from __future__ import annotations

from collections import OrderedDict
from threading import Lock
from typing import Generic, TypeVar, Optional, Dict

K = TypeVar("K")
V = TypeVar("V")


class LRUCache(Generic[K, V]):
    """
    Thread-sicherer LRU-Cache für Ergebnisse von check_link(...).

    - max_size: maximale Anzahl verschiedener Keys im Cache
    - get(key):   liefert Value oder None, zählt accesses/hits/misses
    - set(key,v): speichert Value, wirft bei Bedarf den least recently used Eintrag raus
    """

    def __init__(self, max_size: int = 10000) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")

        self.max_size: int = int(max_size)
        self._store: "OrderedDict[K, V]" = OrderedDict()
        self._lock: Lock = Lock()

        # Statistik-Zähler
        self.accesses: int = 0
        self.hits: int = 0
        self.misses: int = 0

    def get(self, key: K) -> Optional[V]:
        """
        Hole einen Eintrag aus dem Cache.
        Gibt None zurück, wenn der Key nicht vorhanden ist.
        """
        with self._lock:
            self.accesses += 1

            if key in self._store:
                self.hits += 1
                # Key nach hinten verschieben → "zuletzt verwendet"
                value = self._store.pop(key)
                self._store[key] = value
                return value

            # Miss
            self.misses += 1
            return None

    def set(self, key: K, value: V) -> None:
        """
        Setzt einen Eintrag im Cache und wirft bei Bedarf den LRU-Eintrag raus.
        """
        with self._lock:
            # Wenn der Key schon existiert, erst entfernen,
            # damit er am Ende "neu" (most recently used) ist.
            if key in self._store:
                self._store.pop(key)

            self._store[key] = value

            # Wenn wir über max_size kommen → ältesten Eintrag entfernen
            if len(self._store) > self.max_size:
                # popitem(last=False) entfernt den "least recently used"
                self._store.popitem(last=False)

    @property
    def stats(self) -> Dict[str, float]:
        """
        Liefert eine kleine Statistik als Dictionary:
        - accesses
        - hits
        - misses
        - hit_ratio
        """
        with self._lock:
            accesses = self.accesses
            hits = self.hits
            misses = self.misses

        hit_ratio = (hits / accesses) if accesses > 0 else 0.0

        return {
            "accesses": float(accesses),
            "hits": float(hits),
            "misses": float(misses),
            "hit_ratio": float(hit_ratio),
        }

    def __len__(self) -> int:
        """
        Aktuelle Anzahl der gespeicherten Einträge (nur für Debug/Monitoring).
        """
        with self._lock:
            return len(self._store)
