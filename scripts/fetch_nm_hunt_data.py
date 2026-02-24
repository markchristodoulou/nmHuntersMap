#!/usr/bin/env python3
"""Fetch and normalize New Mexico hunt/draw data into app schema.

This script is intentionally dependency-light (stdlib only) so it can run anywhere.
It supports:
1) scraping a report index page for downloadable files (csv/json/xlsx links)
2) downloading matching files into data/raw/<year>
3) normalizing CSV/JSON rows into data/nm_hunt_data.<year>.json

Because state report column names can vary by year/species, this script includes
heuristic column mapping and a --column-map override.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import socket
import ssl
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_INDEX_URL = "https://www.wildlife.state.nm.us/home/hunting/"

# canonical output keys expected by app
CANONICAL_KEYS = {
    "year",
    "zone",
    "species",
    "weapon",
    "drawApplicants",
    "drawTags",
    "hunterSuccessRate",
}

# fuzzy mapping from source column names -> canonical schema
COLUMN_SYNONYMS = {
    "year": ["year", "season year", "license year"],
    "zone": ["zone", "gmu", "unit", "game management unit", "hunt code zone", "hunt unit"],
    "species": ["species", "animal", "game species"],
    "weapon": ["weapon", "sporting arm", "hunt type", "method"],
    "drawApplicants": [
        "draw applicants",
        "applicants",
        "first choice applicants",
        "total applicants",
        "apps",
    ],
    "drawTags": ["draw tags", "tags", "licenses", "permits", "quota", "available licenses"],
    "hunterSuccessRate": [
        "hunter success rate",
        "success rate",
        "harvest success",
        "success %",
        "percent success",
    ],
}


@dataclass
class SourceFile:
    url: str
    filename: str


def fetch_bytes_with_retry(url: str, timeout: int = 60, retries: int = 4, backoff_s: float = 1.25) -> bytes:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "nm-hunters-map-data-bot/1.0"})
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (URLError, HTTPError, TimeoutError, socket.timeout, ConnectionResetError, ssl.SSLError) as err:
            last_err = err
            if attempt == retries:
                break
            sleep_for = backoff_s * (2 ** (attempt - 1))
            print(f"retry {attempt}/{retries} after network error for {url}: {err}", file=sys.stderr)
            time.sleep(sleep_for)

    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_err}")


def fetch_text(url: str, timeout: int = 30, retries: int = 4) -> str:
    return fetch_bytes_with_retry(url, timeout=timeout, retries=retries).decode("utf-8", errors="replace")


def fetch_bytes(url: str, timeout: int = 60, retries: int = 4) -> bytes:
    return fetch_bytes_with_retry(url, timeout=timeout, retries=retries)


def discover_links(index_url: str, year: int | None) -> list[SourceFile]:
    html = fetch_text(index_url)
    # finds href="...csv|json|xlsx|xls"
    hrefs = re.findall(r'href=["\']([^"\']+\.(?:csv|json|xlsx|xls))[^"\']*["\']', html, flags=re.I)
    out: list[SourceFile] = []
    for href in hrefs:
        abs_url = urljoin(index_url, href)
        filename = Path(abs_url.split("?")[0]).name
        if year and str(year) not in abs_url and str(year) not in filename:
            continue
        out.append(SourceFile(url=abs_url, filename=filename))
    # de-duplicate by URL
    unique: dict[str, SourceFile] = {item.url: item for item in out}
    return sorted(unique.values(), key=lambda x: x.filename.lower())


def save_sources(files: list[SourceFile], dest_dir: Path, retries: int = 4) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    failed: list[str] = []
    for src in files:
        target = dest_dir / src.filename
        try:
            data = fetch_bytes(src.url, retries=retries)
            target.write_bytes(data)
            saved.append(target)
            print(f"downloaded: {src.url} -> {target}")
        except Exception as err:  # keep going to next file
            failed.append(src.url)
            print(f"failed: {src.url} -> {err}", file=sys.stderr)

    if failed:
        print(f"warning: failed downloads ({len(failed)}):", file=sys.stderr)
        for u in failed:
            print(f"  - {u}", file=sys.stderr)
    return saved


def normalize_header(h: str) -> str:
    return re.sub(r"\s+", " ", h.strip().lower())


def infer_column_map(headers: list[str]) -> dict[str, str]:
    normalized = {h: normalize_header(h) for h in headers}
    inferred: dict[str, str] = {}
    for canonical, candidates in COLUMN_SYNONYMS.items():
        for source_h, nh in normalized.items():
            if nh == canonical or nh in candidates:
                inferred[canonical] = source_h
                break
    return inferred


def parse_manual_column_map(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    # format: canonical=Source Column,canonical2=Another Column
    out: dict[str, str] = {}
    for part in raw.split(","):
        if "=" not in part:
            raise ValueError(f"Invalid --column-map item: {part}")
        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key not in CANONICAL_KEYS:
            raise ValueError(f"Unknown canonical key in --column-map: {key}")
        out[key] = value
    return out


def coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def canonical_row(raw: dict[str, Any], column_map: dict[str, str], fallback_year: int | None) -> dict[str, Any] | None:
    required = ["zone", "species", "weapon", "drawApplicants", "drawTags", "hunterSuccessRate"]

    def get(key: str) -> Any:
        source_col = column_map.get(key)
        return raw.get(source_col) if source_col else None

    zone = get("zone")
    species = get("species")
    weapon = get("weapon")
    applicants = coerce_number(get("drawApplicants"))
    tags = coerce_number(get("drawTags"))
    success = coerce_number(get("hunterSuccessRate"))

    y = coerce_number(get("year"))
    year = int(y) if y is not None else fallback_year

    if any(v is None for v in [zone, species, weapon, applicants, tags, success]):
        return None
    if year is None:
        return None

    return {
        "year": int(year),
        "zone": str(zone).strip(),
        "species": str(species).strip(),
        "weapon": str(weapon).strip(),
        "drawApplicants": int(round(applicants)),
        "drawTags": int(round(tags)),
        "hunterSuccessRate": round(float(success), 2),
    }


def normalize_csv(path: Path, fallback_year: int | None, manual_map: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        inferred = infer_column_map(headers)
        column_map = {**inferred, **manual_map}

        missing_core = [k for k in ["zone", "species", "weapon", "drawApplicants", "drawTags", "hunterSuccessRate"] if k not in column_map]
        if missing_core:
            print(f"skip {path.name}: missing mappings for {missing_core}", file=sys.stderr)
            return rows

        for raw in reader:
            c = canonical_row(raw, column_map, fallback_year)
            if c:
                rows.append(c)
    return rows


def normalize_json(path: Path, fallback_year: int | None, manual_map: dict[str, str]) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []

    rows: list[dict[str, Any]] = []
    if not payload:
        return rows

    sample_headers = list(payload[0].keys()) if isinstance(payload[0], dict) else []
    inferred = infer_column_map(sample_headers)
    column_map = {**inferred, **manual_map}

    for item in payload:
        if not isinstance(item, dict):
            continue
        c = canonical_row(item, column_map, fallback_year)
        if c:
            rows.append(c)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape and normalize NM hunt/draw data")
    parser.add_argument("--year", type=int, help="Target year for filtering links and fallback output year")
    parser.add_argument("--index-url", default=DEFAULT_INDEX_URL, help="Page to scrape for downloadable report files")
    parser.add_argument("--raw-dir", default="data/raw", help="Folder for downloaded source files")
    parser.add_argument("--retries", type=int, default=4, help="Network retries per request (default: 4)")
    parser.add_argument("--out", help="Output normalized JSON (default: data/nm_hunt_data.<year|merged>.json)")
    parser.add_argument(
        "--column-map",
        help="Override mapping with canonical=source pairs, e.g. zone=Unit,species=Species,weapon=Weapon",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Skip scraping/download and only normalize files already in --raw-dir[/year]",
    )
    args = parser.parse_args()

    raw_base = Path(args.raw_dir)
    raw_dir = raw_base / str(args.year) if args.year else raw_base
    manual_map = parse_manual_column_map(args.column_map)

    if not args.no_download:
        files = discover_links(args.index_url, args.year)
        if not files:
            print("No CSV/JSON/XLSX report links discovered on page (or page fetch failed). Try a specific --index-url and/or increase --retries.", file=sys.stderr)
        else:
            save_sources(files, raw_dir, retries=max(1, args.retries))

    csv_files = sorted(raw_dir.glob("*.csv"))
    json_files = sorted(raw_dir.glob("*.json"))
    xlsx_files = sorted([*raw_dir.glob("*.xlsx"), *raw_dir.glob("*.xls")])

    if xlsx_files:
        print(
            f"warning: found {len(xlsx_files)} xls/xlsx files. Convert them to CSV then re-run for normalization.",
            file=sys.stderr,
        )

    normalized: list[dict[str, Any]] = []
    for f in csv_files:
        normalized.extend(normalize_csv(f, args.year, manual_map))
    for f in json_files:
        normalized.extend(normalize_json(f, args.year, manual_map))

    # de-dup rows
    dedup_key = lambda r: (r["year"], r["zone"], r["species"], r["weapon"], r["drawApplicants"], r["drawTags"], r["hunterSuccessRate"])
    unique = {dedup_key(r): r for r in normalized}
    cleaned = sorted(unique.values(), key=lambda r: (r["year"], r["species"], r["weapon"], r["zone"]))

    suffix = str(args.year) if args.year else "merged"
    out_path = Path(args.out) if args.out else Path(f"data/nm_hunt_data.{suffix}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(cleaned, indent=2) + "\n", encoding="utf-8")

    print(f"normalized rows: {len(cleaned)}")
    print(f"output: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
