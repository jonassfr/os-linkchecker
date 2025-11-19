
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
from src.cache import LRUCache  # NEU

try:
    import psutil
except ImportError:
    psutil = None

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

def _norm_link_target(u: str) -> str:
    """
    Normalisiert Ziel-URLs für den Cache-Key.

    Orientierung an _extract_internal_links:
    - http(s): scheme + netloc + path, ohne trailing Slash, ohne Fragment/Query
    - alles in Kleinbuchstaben bei scheme/netloc
    """
    p = urlparse(u)
    scheme = (p.scheme or "").lower()
    netloc = (p.netloc or "").lower()
    path = (p.path or "/").rstrip("/")
    # Query und Fragment ignorieren, damit z.B. ?utm=... nicht
    # tausend verschiedene Keys erzeugt
    return f"{scheme}://{netloc}{path}"


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

def is_cascade_login(url: str, patterns: list[str]) -> bool:
    """
    Gibt True zurück, wenn die URL einem der Cascade-Login-Pattern entspricht.
    Andere Logins (Microsoft, Okta, etc.) bleiben unberührt,
    solange sie nicht in den patterns stehen.
    """
    if not patterns:
        return False

    u = url.lower()
    for pat in patterns:
        if not pat:
            continue
        if pat.lower() in u:
            return True
    return False

def check_link(
    url: str,
    session: requests.Session,
    cfg: dict,
    cache=None,
):
    """
    Prüft einen einzelnen Link per HTTP-Request.

    Rückgabe-Tuple:
        (violation_type, status, final_url, note)

    - violation_type: "ok" oder "broken_link"
    - status:         HTTP-Statuscode (int) oder "" bei Fehlern ohne Response
    - final_url:      effektive Ziel-URL (nach Redirects)
    - note:           kurze Info, z.B. "status>=400", "timeout", "redirect ok"

    Wenn ein Cache übergeben wird, werden Ergebnisse pro Ziel-URL gecacht.
    """
    checker_cfg = cfg.get("checker", {})
    treat_redirect_as_ok = checker_cfg.get("treat_redirect_as_ok", True)

    timeout = int(cfg.get("timeout", 10))
    headers = {"User-Agent": cfg.get("user_agent", "MarianLinkChecker/0.2")}

    # --- 1) Cache-Lookup vor HTTP-Request ---
    key = _norm_link_target(url)
    if cache is not None:
        cached = cache.get(key)
        if cached is not None:
            # cached ist bereits das (violation_type, status, final_url, note)-Tuple
            return cached

    # --- 2) Normaler HTTP-Request + Logik wie vorher ---
    try:
        resp = session.get(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        )
        final_url = resp.url
        status = resp.status_code

        # Fall 1: Klar broken (>= 400)
        if status >= 400:
            result = ("broken_link", status, final_url, "status>=400")
        else:
            # Fall 2: Redirect-Handling
            if 300 <= status < 400:
                if treat_redirect_as_ok:
                    result = ("ok", status, final_url, "redirect ok")
                else:
                    result = ("broken_link", status, final_url, "redirect treated as broken")
            else:
                # Fall 3: Normale 2xx/1xx Antworten → ok
                note = "ok"
                if resp.history:
                    note = f"redirect chain len={len(resp.history)}"
                result = ("ok", status, final_url, note)

    except requests.RequestException as e:
        # Netzfehler, Timeout, DNS etc. → als broken_link reporten
        msg = f"{type(e).__name__}: {str(e)[:120]}"
        result = ("broken_link", "", "", msg)

    # --- 3) Ergebnis im Cache speichern (falls aktiv) ---
    if cache is not None:
        cache.set(key, result)

    return result



def _extract_internal_links(
    page_url: str,
    html: str,
    allow_domains: set,
    *,
    count_duplicates: bool = True,
):
    """
    Extrahiert alle internen Links aus dem Haupt-Content-Bereich.

    Rückgabe (Dict):
        {
            "links": [link1, link2, ...],   # normalisierte URLs (mit Duplikaten)
            "count": N                      # Anzahl (je nach count_duplicates)
        }
    """
    soup = BeautifulSoup(html, "html.parser")

    # Header/Nav/Footer/Aside entfernen
    for selector in ["header", "nav", "footer", "aside", ".site-header", ".site-footer", ".global-nav"]:
        for node in soup.select(selector):
            node.decompose()

    # Hauptbereich suchen (Fallback: gesamtes Dokument)
    main = soup.find("main") or soup.find("div", {"id": "content"}) or soup.find("div", {"class": "content"})
    search_area = main if main else soup

    uniq_targets = set()
    occurrences = 0
    links: list[str] = []

    for a in search_area.find_all("a", href=True):
        raw = a["href"].strip()
        abs_url = urljoin(page_url, raw)
        p = urlparse(abs_url)

        # MAILTO-Sonderfall 
        if p.scheme == "mailto":
            email = p.path.strip().lower()  # z.B. "name@marian.edu"
            if "@" in email:
                _, dom = email.rsplit("@", 1)
                if any(dom.endswith(ad) for ad in allow_domains):
                    occurrences += 1
                    norm_mail = f"mailto:{email}"
                    links.append(norm_mail)
                    uniq_targets.add(norm_mail)
            continue

        # irrelevante Schemata überspringen
        if p.scheme in SKIP_SCHEMES:
            continue

        # nur interne HTTP(S)-Links zählen
        if not any(p.netloc.endswith(dom) for dom in allow_domains):
            continue

        # Pfad muss existieren, sonst uninteressant
        if not p.path:
            continue

        # Normalisieren für "unique"-Zählung
        norm = f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")

        occurrences += 1
        links.append(norm)
        uniq_targets.add(norm)

    if count_duplicates:
        count = occurrences
    else:
        count = len(uniq_targets)


    return {
        "links": links,
        "count": count,
    }


def _worker(
    frontier: "queue.Queue[str]",
    visited: set,
    vlock: threading.Lock,
    results: list,
    cfg: dict,
    violations: list,
    violock: threading.Lock,
    processed: dict,
    plock: threading.Lock,
    total: int,
    cache,
):

    session = requests.Session()  # Session pro Thread (Keep-Alive)
    headers = {"User-Agent": cfg.get("user_agent", "MarianLinkChecker/0.2")}
    timeout = int(cfg.get("timeout", 10))
    delay = float(cfg.get("delay", 0.0))
    extract = bool(cfg.get("extract_links", True))
    allow = set(cfg.get("domain_allowlist", []))

    # Checker-Konfig aus config.yaml
    checker_cfg = cfg.get("checker", {})
    patterns = checker_cfg.get("cascade_login_patterns", [])
    # only_internal brauchst du aktuell nicht, weil _extract_internal_links
    # sowieso nur interne Links liefert – lassen wir für später drin:
    only_internal = checker_cfg.get("only_internal", True)
    max_links = checker_cfg.get("max_links_per_page", 300)

    while True:
        try:
            url = frontier.get_nowait()
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
        report_status = ""
        links_for_page: list[str] = []

        # NEU: seitenweite Violation-Daten
        violations_for_page = []  # (page_url, link_url, violation_type, status, final_url, note)
        violation_summary = "none"
        violations_count = 0

        try:
            resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            status = str(resp.status_code)
            final_url = resp.url
            ms = (perf_counter() - t0) * 1000.0

            # Redirects zu 301 “zusammenfalten”, ohne extra Spalten
            orig_norm = _norm_url(url)
            final_norm = _norm_url(final_url)
            report_status = 301 if orig_norm != final_norm else resp.status_code

            ctype = resp.headers.get("Content-Type", "")

            # Nur HTML-Seiten parsen und Links extrahieren
            if extract and resp.ok and "text/html" in ctype.lower():
                link_info = _extract_internal_links(
                    url,
                    resp.text,
                    allow,
                    count_duplicates=bool(cfg.get("count_duplicates", True)),
                )
                links_for_page = link_info["links"]
                link_count = str(link_info["count"])

                # Sicherheitslimit pro Seite anwenden
                if max_links and len(links_for_page) > max_links:
                    links_for_page = links_for_page[:max_links]

                # --- NEU: alle Links dieser Seite prüfen ---
                from urllib.parse import urlparse as _up

                for link_url in links_for_page:
                    parsed = _up(link_url)

                    # Nur http/https-Links prüfen – keine mailto:, tel:, etc.
                    if parsed.scheme not in ("http", "https"):
                        continue

                    # 1) Cascade-Login-Erkennung
                    if is_cascade_login(link_url, patterns):
                        violations_for_page.append([
                            url,          # page_url
                            link_url,     # link_url
                            "cascade_login",
                            "",           # status (nicht relevant)
                            "",           # final_url
                            "cascade login link",
                        ])
                        continue  # kein zusätzlicher HTTP-Check nötig

                    # 2) HTTP-Status prüfen
                    v_type, v_status, v_final_url, v_note = check_link(link_url, session, cfg, cache=cache)
                    if v_type == "broken_link":
                        violations_for_page.append([
                            url,
                            link_url,
                            "broken_link",
                            v_status,
                            v_final_url,
                            v_note,
                        ])

                # Seiten-Level-Attribute berechnen
                if violations_for_page:
                    types = {v[2] for v in violations_for_page}  # {"broken_link", "cascade_login", ...}
                    violation_summary = "+".join(sorted(types))
                    violations_count = len(violations_for_page)

        except Exception as e:
            ms = (perf_counter() - t0) * 1000.0
            status = ""
            err = f"{type(e).__name__}: {str(e)[:120]}"
            if not report_status:
                report_status = status or ""

        ts_end = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        # NEU: Page-Violations in globale Liste übernehmen
        if violations_for_page:
            with violock:
                violations.extend(violations_for_page)

        # URL-Row für links_multithread.csv
        results.append([
            url,
            str(report_status),
            f"{ms:.2f}",
            t_name,
            ts_start,
            ts_end,
            err,
            link_count,
            final_url,
            ctype,
            violation_summary,
            str(violations_count),
        ])
        with plock:
            processed["n"] += 1
            n = processed["n"]
            if n % 100 == 0 or n == total:
                print(f"[crawl] processed {n}/{total} pages...")

        frontier.task_done()



def crawl_all(urls: list[str], cfg: dict, schedule_mode: str = None) -> float:
    if schedule_mode is None:
        schedule_mode = cfg.get("scheduler", "fifo")

    max_n = int(cfg.get("max_urls", 0)) or len(urls)

    # Reihenfolge + Limit anwenden
    ordered = order_urls(urls, schedule_mode)[:max_n]
    total = len(ordered)

    # Frontier + visited
    frontier: "queue.Queue[str]" = queue.Queue()
    for u in ordered:
        frontier.put(u)

    visited = set()
    vlock = threading.Lock()
    results = []
    # NEU: globale Violations-Liste + Lock
    violations: list[list[str]] = []
    violock = threading.Lock()
    processed = {"n": 0}
    plock = threading.Lock()

    threads = int(cfg.get("threads", 12))
    out_dir = Path(cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "links_multithread.csv"
    delimiter = cfg.get("csv_delimiter", ",")

    # --- NEU: Cache nach config anlegen ---
    cache_cfg = cfg.get("cache", {})
    cache_mode = cache_cfg.get("mode", "none")
    cache_max_size = int(cache_cfg.get("max_size", 10000))

    if cache_mode == "lru":
        cache = LRUCache(max_size=cache_max_size)
    else:
        cache = None
        # --- Performance-Messung (CPU/RAM) vorbereiten ---
    cpu_percent_avg = None
    memory_rss_mb = None
    proc = None

    if psutil is not None:
        try:
            proc = psutil.Process()
            # Erster Call "primt" die Messung; der nächste Call gibt % seit hier
            proc.cpu_percent(interval=None)
        except Exception:
            proc = None



    print(f"[crawl] mode={schedule_mode}, threads={threads}, delay={cfg.get('delay', 0)}s, total={len(ordered)}")
    t0 = perf_counter()

    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [
            ex.submit(
                _worker,
                frontier,
                visited,
                vlock,
                results,
                cfg,
                violations,
                violock,
                processed,
                plock,
                total,
                cache,
            )
            for _ in range(threads)
        ]
        for f in futures:
            f.result()  # block until all workers finish


    duration = perf_counter() - t0
    throughput = (len(results) / duration) if duration > 0 else 0.0
    print(f"[crawl] done: {len(results)} URLs in {duration:.2f}s -> {throughput:.2f} URLs/s")

    # CPU/RAM nach dem Crawl messen (falls psutil verfügbar)
    if proc is not None:
        try:
            cpu_percent_avg = proc.cpu_percent(interval=None)  # % CPU seit dem letzten Aufruf
            mem_bytes = proc.memory_info().rss
            memory_rss_mb = mem_bytes / (1024 * 1024)
        except Exception:
            cpu_percent_avg = None
            memory_rss_mb = None


    # 1) Violations nach Typ zählen
    broken_links_total = sum(1 for v in violations if v[2] == "broken_link")
    cascade_logins_total = sum(1 for v in violations if v[2] == "cascade_login")

    # 2) Seiten mit mindestens einer Violation (egal welcher Typ)
    pages_with_violations = len({v[0] for v in violations})

    # 3) Gesamtzahl gefundener interner Links (Summe internal_links_found aus results)
    total_links_found = 0
    for row in results:
        val = row[7]  # "internal_links_found"
        try:
            links_here = int(val) if val not in ("", None) else 0
        except ValueError:
            links_here = 0
        total_links_found += links_here

    # 4) Cache-Statistiken
    cache_cfg = cfg.get("cache", {})
    cache_mode = cache_cfg.get("mode", "none")
    cache_max_size = int(cache_cfg.get("max_size", 10000))

    # Standardwerte, falls kein Cache verwendet wird
    cache_accesses = 0
    cache_hits = 0
    cache_misses = 0
    cache_hit_ratio = 0.0

    # Wenn ein echter Cache existiert, Stats auslesen
    if cache is not None:
        stats = cache.stats
        cache_accesses = int(stats.get("accesses", 0))
        cache_hits = int(stats.get("hits", 0))
        cache_misses = int(stats.get("misses", 0))
        cache_hit_ratio = float(stats.get("hit_ratio", 0.0))



    # CSV schreiben
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = writer(f, delimiter=delimiter)
        w.writerow([
            "url","status","time_ms","thread","start_utc","end_utc",
            "error","internal_links_found","final_url","content_type",
            "violation","violations_count"
        ])
        w.writerows(results)

    print(f"[report] {out_csv}")

    # NEU: Detail-Report aller einzelnen Violations
    viol_csv = out_dir / "violations_links.csv"
    with open(viol_csv, "w", newline="", encoding="utf-8") as f:
        w = writer(f, delimiter=delimiter)
        w.writerow([
            "page_url",
            "link_url",
            "violation_type",
            "status",
            "final_url",
            "note",
        ])
        w.writerows(violations)

    print(f"[violations] wrote {len(violations)} rows -> {viol_csv}")

    # --- Append run summary for comparisons ---
    summary_csv = out_dir / "run_summary.csv"
    from datetime import datetime as _dt
    # Hilfsfunktionen für optionale CPU/RAM-Werte
    def _fmt_opt_float(x):
        if isinstance(x, (int, float)):
            return f"{x:.2f}".replace(".", ",")
        return ""

    summary_row = [
        _dt.utcnow().isoformat(timespec="seconds") + "Z",  # ts_utc
        schedule_mode,                                     # scheduler
        threads,                                           # threads
        str(cfg.get("delay", 0)),                          # delay_s
        len(ordered),                                      # urls_total
        f"{duration:.2f}".replace(".", ","),               # duration_s
        f"{throughput:.2f}".replace(".", ","),             # urls_per_s
        broken_links_total,                                # broken_links_total
        cascade_logins_total,                              # cascade_logins_total
        pages_with_violations,                             # pages_with_violations
        total_links_found,                                 # total_links_found (extra, nützlich)
        cache_mode,                                        # cache_mode
        cache_max_size,                                    # cache_max_size
        cache_accesses,                                    # cache_accesses
        cache_hits,                                        # cache_hits
        cache_misses,                                      # cache_misses
        f"{cache_hit_ratio:.4f}".replace(".", ","),        # cache_hit_ratio
        _fmt_opt_float(cpu_percent_avg),                   # cpu_percent_avg
        _fmt_opt_float(memory_rss_mb),                     # memory_rss_mb
    ]




    write_header = not summary_csv.exists()
    with open(summary_csv, "a", newline="", encoding="utf-8") as f:
        w = writer(f, delimiter=delimiter)
        if write_header:
            w.writerow([
                "ts_utc",
                "scheduler",
                "threads",
                "delay_s",
                "urls_total",
                "duration_s",
                "urls_per_s",
                "broken_links_total",
                "cascade_logins_total",
                "pages_with_violations",
                "total_links_found",
                "cache_mode",
                "cache_max_size",
                "cache_accesses",
                "cache_hits",
                "cache_misses",
                "cache_hit_ratio",
                "cpu_percent_avg",
                "memory_rss_mb",
            ])


        w.writerow(summary_row)


    print(f"[summary] appended -> {summary_csv}")
    # --- end summary ---




    return throughput
