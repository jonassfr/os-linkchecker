# src/crawl.py
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter, sleep
from pathlib import Path
from csv import writer
import threading
import queue
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

from src.scheduler import order_urls

_thread_local = threading.local()


def _norm_url(u: str) -> str:
    """Normalisiert URL für Vergleich: ohne Fragment, ohne trailing Slash,
    Schema/Host in Kleinbuchstaben, Query bleibt erhalten."""
    p = urlparse(u)
    scheme = (p.scheme or "").lower()
    netloc = (p.netloc or "").lower()
    path = (p.path or "/").rstrip("/")
    query = f"?{p.query}" if p.query else ""
    return f"{scheme}://{netloc}{path}{query}"

def _rate_limit(delay: float):
    if delay <= 0:
        return
    last = getattr(_thread_local, "last_ts", None)
    now = perf_counter()
    if last is None:
        sleep(delay)            # auch den ersten Call minimal drosseln
        _thread_local.last_ts = perf_counter()
        return
    wait = delay - (now - last)
    if wait > 0:
        sleep(wait)
    _thread_local.last_ts = perf_counter()

def _is_internal(u: str, allow_domains: set) -> bool:
    host = urlparse(u).netloc
    return any(host.endswith(dom) for dom in allow_domains)

SKIP_SCHEMES = {"tel", "javascript", "data"}

def _extract_internal_links(page_url: str, html: str, allow_domains: set, *, count_duplicates: bool = True) -> int:
    soup = BeautifulSoup(html, "html.parser")

    # Header/Nav/Footer/Aside entfernen
    for selector in ["header", "nav", "footer", "aside", ".site-header", ".site-footer", ".global-nav"]:
        for node in soup.select(selector):
            node.decompose()

    # Hauptbereich suchen (fallback: gesamtes Dokument)
    main = soup.find("main") or soup.find("div", {"id": "content"}) or soup.find("div", {"class": "content"})
    search_area = main if main else soup

    uniq_targets = set()
    occurrences = 0

    for a in search_area.find_all("a", href=True):
        raw = a["href"].strip()
        abs_url = urljoin(page_url, raw)
        p = urlparse(abs_url)

        # --- MAILTO-Sonderfall --------------------------------------------
        if p.scheme == "mailto":
            # p.path ist z.B. "name@marian.edu"
            email = p.path.strip().lower()
            # Domain extrahieren und gegen Allowlist prüfen
            if "@" in email:
                _, dom = email.rsplit("@", 1)
                if any(dom.endswith(ad) for ad in allow_domains):
                    occurrences += 1
                    # für Unique-Zählung "mailto:<email>" normalisieren
                    uniq_targets.add(f"mailto:{email}")
            continue
        # -------------------------------------------------------------------

        # irrelevante Schemata überspringen
        if p.scheme in SKIP_SCHEMES:
            continue

        # nur interne HTTP(S)-Links zählen
        if not any(p.netloc.endswith(dom) for dom in allow_domains):
            continue

        # Normalisieren für "unique"-Zählung
        norm = f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")
        if not p.path:
            continue

        occurrences += 1
        uniq_targets.add(norm)

    return occurrences if count_duplicates else len(uniq_targets)

def _worker(frontier: "queue.Queue[str]", visited: set, vlock: threading.Lock, results: list, cfg: dict):
    session = requests.Session()  # Session pro Thread (Keep-Alive)
    headers = {"User-Agent": cfg.get("user_agent", "MarianLinkChecker/0.2")}
    timeout = int(cfg.get("timeout", 10))
    delay = float(cfg.get("delay", 0.0))
    extract = bool(cfg.get("extract_links", True))
    allow = set(cfg.get("domain_allowlist", []))

    while True:
        try:
            url = frontier.get_nowait()          # <-- erst URL aus der Queue ziehen
        except queue.Empty:
            break

        # Duplicate-Schutz
        with vlock:
            if url in visited:
                frontier.task_done()
                continue
            visited.add(url)

        _rate_limit(delay)

        t_name = threading.current_thread().name
        ts_start = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        t0 = perf_counter()
        status = ""
        err = ""
        link_count = ""
        final_url = ""
        ctype = ""

        try:
            resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)  # <-- Session nutzen
            status = str(resp.status_code)
            final_url = resp.url
            ms = (perf_counter() - t0) * 1000.0
            # Redirects zu 301 “zusammenfalten”, ohne extra Spalten
            orig_norm  = _norm_url(url)
            final_norm = _norm_url(final_url)
            report_status = 301 if orig_norm != final_norm else resp.status_code

            ctype = resp.headers.get("Content-Type", "")
            if extract and resp.ok and "text/html" in ctype.lower():
                link_count = str(_extract_internal_links(url, resp.text, allow, count_duplicates=bool(cfg.get("count_duplicates", True))))

        except Exception as e:
            ms = (perf_counter() - t0) * 1000.0
            status = ""
            err = f"{type(e).__name__}: {str(e)[:120]}"

        ts_end = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        results.append([
            url, str(report_status), f"{ms:.2f}", t_name, ts_start, ts_end, err, link_count, final_url, ctype
        ])

        frontier.task_done()

def crawl_all(urls: list[str], cfg: dict, schedule_mode: str = None) -> float:
    if schedule_mode is None:
        schedule_mode = cfg.get("scheduler", "fifo")

    max_n = int(cfg.get("max_urls", 0)) or len(urls)

    # Reihenfolge + Limit anwenden
    ordered = order_urls(urls, schedule_mode)[:max_n]

    # Frontier + visited
    frontier: "queue.Queue[str]" = queue.Queue()
    for u in ordered:
        frontier.put(u)

    visited = set()
    vlock = threading.Lock()
    results = []

    threads = int(cfg.get("threads", 12))
    out_dir = Path(cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "links_multithread.csv"
    delimiter = cfg.get("csv_delimiter", ",")

    print(f"[crawl] mode={schedule_mode}, threads={threads}, delay={cfg.get('delay', 0)}s, total={len(ordered)}")
    t0 = perf_counter()

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(_worker, frontier, visited, vlock, results, cfg) for _ in range(threads)]
        for f in futures:
            f.result()  # block until all workers finish

    duration = perf_counter() - t0
    throughput = (len(results) / duration) if duration > 0 else 0.0
    print(f"[crawl] done: {len(results)} URLs in {duration:.2f}s -> {throughput:.2f} URLs/s")

    # CSV schreiben
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = writer(f, delimiter=delimiter)
        w.writerow([
            "url","status","time_ms","thread","start_utc","end_utc",
            "error","internal_links_found","final_url","content_type"
        ])
        w.writerows(results)

    

    print(f"[report] {out_csv}")

    # --- Append run summary for comparisons ---
    summary_csv = out_dir / "run_summary.csv"
    from datetime import datetime as _dt
    summary_row = [
        _dt.utcnow().isoformat(timespec="seconds") + "Z",
        schedule_mode,
        threads,
        str(cfg.get("delay", 0)),
        len(ordered),
        f"{duration:.2f}".replace(".", ","),     # Komma statt Punkt
        f"{throughput:.2f}".replace(".", ",")
    ]

    write_header = not summary_csv.exists()
    with open(summary_csv, "a", newline="", encoding="utf-8") as f:
        w = writer(f, delimiter=delimiter)
        if write_header:
            w.writerow(["ts_utc","scheduler","threads","delay_s","urls_total","duration_s","urls_per_s"])
        w.writerow(summary_row)
    print(f"[summary] appended -> {summary_csv}")
    # --- end summary ---

    return throughput
