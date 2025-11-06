
from urllib.parse import urlparse

def _path_depth(url: str) -> int:
    return urlparse(url).path.count("/")

def _priority_score(url: str) -> int:
    # Kleinerer Score = höhere Priorität
    p = urlparse(url)
    score = 0
    if p.query:
        score += 2
    score += _path_depth(url)
    return score

def order_urls(urls, mode: str = "fifo"):
    if mode == "priority":
        return sorted(urls, key=_priority_score)
    # FIFO = Reihenfolge beibehalten
    return list(urls)
