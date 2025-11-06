from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def extract_links(page_url, html, allow_domains):
    soup = BeautifulSoup(html, "html.parser")
    internal, external = set(), set()
    for a in soup.find_all("a", href=True):
        abs_url = urljoin(page_url, a["href"])
        if any(urlparse(abs_url).netloc.endswith(d) for d in allow_domains):
            internal.add(abs_url)
        else:
            external.add(abs_url)
    return sorted(internal), sorted(external)
