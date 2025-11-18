# src/scheduler.py
from urllib.parse import urlparse

def _path_depth(url: str) -> int:
    # Anzahl der "/" im Pfad (Root "/" zählt als 1, daher max(0, ...))
    path = urlparse(url).path or "/"
    return max(0, path.count("/"))

def _priority_score(url: str) -> tuple[int, int]:
    """
    Kleinere Werte = höhere Priorität.
    Score 1: Pfadtiefe + Query-Penalty
    Score 2: Gesamtlänge (kürzt "tiefe & lange" URLs nach hinten)
    Rückgabe als Tupel: (score, length) -> stabil und deterministisch
    """
    p = urlparse(url)
    score = _path_depth(url)
    if p.query:
        score += 2  # Query-Strings leicht nachrangig
    return (score, len(url))

def order_urls(urls, mode: str = "fifo"):
    mode = (mode or "fifo").lower()
    if mode == "priority":
        # deterministisch: bei Gleichstand entscheidet die URL-Länge,
        # danach (implizit) die URL selbst
        return sorted(urls, key=_priority_score)
    # FIFO: Reihenfolge beibehalten
    return list(urls)
