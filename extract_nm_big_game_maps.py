#!/usr/bin/env python3
"""Download all PDF links from the New Mexico Big Game Unit Maps page."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://wildlife.dgf.nm.gov/hunting/maps/big-game-unit-maps-pdfs/"
OUTPUT_DIR = Path("nm_big_game_unit_maps")


def sanitize_filename(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or "download.pdf"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def find_pdf_links(base_url: str) -> list[str]:
    response = requests.get(base_url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = anchor["href"].strip()
        if ".pdf" in href.lower():
            links.add(urljoin(base_url, href))

    return sorted(links)


def download_pdfs(urls: list[str], outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(urls)} PDF links")
    for url in urls:
        filename = sanitize_filename(url)
        outpath = outdir / filename

        if outpath.exists() and outpath.stat().st_size > 0:
            print(f"Skip existing: {filename}")
            continue

        response = requests.get(url, timeout=60)
        response.raise_for_status()
        outpath.write_bytes(response.content)
        print(f"Saved: {filename}")


if __name__ == "__main__":
    pdf_links = find_pdf_links(BASE_URL)
    download_pdfs(pdf_links, OUTPUT_DIR)
