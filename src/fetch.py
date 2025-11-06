import time
from pathlib import Path

def fetch_url(mock_path):
    t0 = time.perf_counter()
    html = Path(mock_path).read_text(encoding="utf-8")
    ms = (time.perf_counter() - t0) * 1000
    return html, ms
