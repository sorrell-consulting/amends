#!/usr/bin/env python3
"""Check the built site's local links and this example's fixed chart data."""
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"


class Page(HTMLParser):
    def __init__(self, path):
        super().__init__(convert_charrefs=True)
        self.ids = set()
        self.links = []
        self.canonical = None
        self.feed(path.read_text())

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "link" and "canonical" in attrs.get("rel", "").split():
            self.canonical = attrs.get("href")
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for name in ("href", "src"):
            if name in attrs:
                self.links.append(attrs[name])


def main():
    errors = []
    pages = {p.resolve(): Page(p) for p in SITE.rglob("*.html")}
    if not pages:
        raise SystemExit("Build site/ first with make docs-build")
    # MkDocs renders site_url into the homepage's canonical URL. Use the built
    # configuration so both a custom-domain root and a project subpath work.
    home_page = pages.get((SITE / "index.html").resolve())
    if not home_page or not home_page.canonical:
        raise SystemExit("Built homepage is missing its canonical site URL")
    site_url = urlsplit(home_page.canonical)
    base_path = unquote(site_url.path or "/")
    if site_url.scheme not in ("http", "https") or not site_url.netloc or not base_path.endswith("/"):
        raise SystemExit(f"Invalid canonical site URL: {home_page.canonical}")
    checked = 0
    for source, page in pages.items():
        for link in page.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            path = unquote(url.path)
            if path.startswith("/"):
                if not path.startswith(base_path):
                    errors.append(f"{source.relative_to(SITE)}: outside project URL: {link}")
                    continue
                target = SITE / path.removeprefix(base_path)
            else:
                target = source.parent / path if path else source
            if target.is_dir():
                target /= "index.html"
            target = target.resolve()
            checked += 1
            if not target.is_relative_to(SITE) or not target.is_file():
                errors.append(f"{source.relative_to(SITE)}: missing local target {link}")
            elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].ids:
                errors.append(f"{source.relative_to(SITE)}: missing anchor {link}")

    # GitHub source links must name real repository files/directories, not site URLs.
    source_links = set()
    for page in pages.values():
        for link in page.links:
            match = re.match(r"https://github.com/sorrell-consulting/amends/(blob|tree)/main/([^?#]+)", link)
            if not match:
                continue
            target = ROOT / unquote(match[2])
            source_links.add(link)
            if not (target.is_file() if match[1] == "blob" else target.is_dir()):
                errors.append(f"Missing GitHub source target: {link}")

    script = (ROOT / "docs/assets/javascripts/inventory.js").read_text()
    data, _ = json.JSONDecoder().raw_decode(script.split("const inventory = ", 1)[1])
    config = json.loads((ROOT / "fixtures/config.json").read_text())["stock_keys"][0]
    assert data["threshold"] == config["reorder_threshold"]
    assert data["labels"] == ["Opening", "Day 1", "Day 2", "Day 3"]
    for name, fixture in [("original", "before"), ("corrected", "after")]:
        expected = json.loads((ROOT / f"fixtures/expected-{fixture}.json").read_text())
        values = [config["opening_balance"]] + [b["closing_balance"] for b in expected["balances"]]
        assert data[name] == values, f"{name} chart differs from hand-derived fixture"
        crossings = [i for i in range(1, len(values)) if values[i - 1] >= data["threshold"] > values[i]]
        assert crossings == [data["crossings"][name]]
        assert expected["active_crossing_alerts"][0]["crossing_day"] == expected["balances"][crossings[0] - 1]["day"]
    # The accessible table is essential, so check it against the same fixed values.
    home = (ROOT / "docs/index.md").read_text()
    for i, label in enumerate(["Opening", "Day 1 close", "Day 2 close", "Day 3 close"]):
        assert f"| {label} | {data['original'][i]} | {data['corrected'][i]} |" in home
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"Checked {len(pages)} HTML pages, {checked} local links/assets, {len(source_links)} GitHub source targets, and chart/table values against fixtures.")


if __name__ == "__main__":
    main()
