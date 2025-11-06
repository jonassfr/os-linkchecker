def parse_sitemap(sitemap_url: str, output_csv: str, mock_mode: bool=False, mock_path: str="") -> int:
    from pathlib import Path
    try:
        # 1) bevorzugt BeautifulSoup + lxml
        from bs4 import BeautifulSoup
        if mock_mode:
            xml = Path(mock_path).read_text(encoding="utf-8")
        else:
            import requests
            resp = requests.get(sitemap_url, timeout=20, headers={"User-Agent": "MarianLinkChecker/0.1"})
            resp.raise_for_status()
            xml = resp.text
        soup = BeautifulSoup(xml, "xml")  # nutzt lxml, wenn vorhanden
        locs = [loc.text.strip() for loc in soup.find_all("loc")]
    except Exception:
        # 2) Fallback: stdlib ElementTree (Namespace beachten)
        import xml.etree.ElementTree as ET
        NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        if mock_mode:
            xml = Path(mock_path).read_text(encoding="utf-8")
        else:
            import requests
            resp = requests.get(sitemap_url, timeout=20, headers={"User-Agent": "MarianLinkChecker/0.1"})
            resp.raise_for_status()
            xml = resp.text
        root = ET.fromstring(xml)
        locs = [e.text.strip() for e in root.findall(".//sm:loc", NS) if e is not None and e.text]

    # leichte Normalisierung + Dedupe
    def norm(u: str) -> str:
        u = u.strip()
        if u.startswith("http://"): u = "https://" + u[len("http://"):]
        if "#" in u: u = u.split("#", 1)[0]
        return u.rstrip("/")  # keine trailing slashes
    seen, out = set(), []
    for u in map(norm, locs):
        if u not in seen:
            seen.add(u); out.append(u)

    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        f.write("url\n")
        f.writelines(u + "\n" for u in out)
    return len(out)
