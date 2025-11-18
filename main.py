from src.sitemap import parse_sitemap
from src.fetch import fetch_url
from src.parse import extract_links
from pathlib import Path
from csv import writer
import yaml  # config.yaml
from src.crawl import crawl_all

def main():
    # 0) Config laden
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # 1) Sitemap einlesen -> data/urls_initial.csv (Mock oder Real)
    urls_csv = Path(cfg["data_dir"]) / "urls_initial.csv"
    if cfg.get("mock_mode", True):
        count = parse_sitemap(
            sitemap_url=cfg.get("sitemap_url", ""),   # wird im Mock nicht genutzt
            output_csv=str(urls_csv),
            mock_mode=True,
            mock_path=cfg["mock"]["sitemap_path"],
        )
    else:
        count = parse_sitemap(
            sitemap_url=cfg["sitemap_url"],
            output_csv=str(urls_csv),
            mock_mode=False
        )
    print(f"[sitemap] wrote {count} URLs -> {urls_csv}")

    # 2–4) Nur im Mock-Mode: Beispielseite laden, Links extrahieren, CSV schreiben
    if cfg.get("mock_mode", True):
        page_path = cfg["mock"]["sample_page_path"]
        html, ms = fetch_url(page_path)
        print(f"[fetch] sanity metric: {ms:.2f} ms / request")

        allow = set(cfg.get("domain_allowlist", []))
        internal, external = extract_links(cfg["mock"]["sample_page_url"], html, allow)
        print(f"[parse] internal={len(internal)} external={len(external)}")

        Path(cfg["output_dir"]).mkdir(parents=True, exist_ok=True)
        output_csv = Path(cfg["output_dir"]) / "links_sample.csv"
        delimiter = cfg.get("csv_delimiter", ",")
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            w = writer(f, delimiter=delimiter)
            w.writerow(["page_url", "link_url"])
            for link in internal:
                w.writerow([cfg["mock"]["sample_page_url"], link])
        print(f"[report] wrote internal links -> {output_csv}")

    # 5) Nur im Real-Mode: Multithreaded Crawl (Week 2)
    if not cfg.get("mock_mode", True):
        urls_path = Path(cfg["data_dir"]) / "urls_initial.csv"
        with open(urls_path, "r", encoding="utf-8") as f:
            next(f)  # header skip
            all_urls = [line.strip() for line in f if line.strip()]

        # Für erste Tests begrenzen
        max_n = int(cfg.get("max_urls", 0))
        if max_n > 0:
            url_batch = all_urls[:max_n]
        else:
            url_batch = all_urls

        crawl_all(url_batch, cfg)

if __name__ == "__main__":
    main()
