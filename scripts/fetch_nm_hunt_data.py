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
import io
import json
import re
import socket
import ssl
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

DEFAULT_INDEX_URL = "https://wildlife.dgf.nm.gov/home/hunting/"

# canonical output keys expected by app
CANONICAL_KEYS = {
    "year",
    "zone",
    "species",
    "weapon",
    "drawApplicants",
    "drawTags",
    "hunterSuccessRate",
    "huntCode",
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
    "huntCode": ["hunt code", "huntcode", "code", "hunt"],
}


@dataclass
class SourceFile:
    url: str
    filename: str


class HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)


REPORT_PAGE_KEYWORDS = (
    "harvest-report",
    "draw-report",
    "draw-odds",
    "draw-result",
    "draw-success",
)


def fetch_bytes_with_retry(url: str, timeout: int = 60, retries: int = 4, backoff_s: float = 1.25) -> bytes:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": "nm-hunters-map-data-bot/1.0",
                    "Connection": "close",
                    "Accept": "*/*",
                },
            )
            with urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                setattr(fetch_bytes_with_retry, "_last_headers", resp.headers)
                return data
        except (URLError, HTTPError, TimeoutError, socket.timeout, ConnectionResetError, ssl.SSLError) as err:
            last_err = err
            if attempt == retries:
                break
            # small jitter reduces retry stampedes and helps with flaky middleboxes/proxies
            jitter = 0.15 * attempt
            sleep_for = backoff_s * (2 ** (attempt - 1)) + jitter
            print(f"retry {attempt}/{retries} after network error for {url}: {err}", file=sys.stderr)
            time.sleep(sleep_for)

    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_err}")


def fetch_text(url: str, timeout: int = 30, retries: int = 4) -> str:
    return fetch_bytes_with_retry(url, timeout=timeout, retries=retries).decode("utf-8", errors="replace")


def fetch_bytes(url: str, timeout: int = 60, retries: int = 4) -> bytes:
    return fetch_bytes_with_retry(url, timeout=timeout, retries=retries)


def _guess_filename_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path
    name = Path(path).name
    return name or fallback




def _extract_years(text: str) -> set[int]:
    years = {int(y) for y in re.findall(r"(?<!\d)(20\d{2})(?!\d)", text)}
    for a, b in re.findall(r"(?<!\d)(20\d{2})\s*[-/]\s*(20\d{2})(?!\d)", text):
        ya, yb = int(a), int(b)
        if ya <= yb and yb - ya <= 2:
            years.update(range(ya, yb + 1))
    return years


def matches_target_year(text: str, year: int | None) -> bool:
    if year is None:
        return True
    years = _extract_years(text)
    return not years or year in years

def discover_links(index_url: str, year: int | None, include_pdf: bool = False, retries: int = 4, timeout: int = 45) -> list[SourceFile]:
    html = fetch_text(index_url, retries=retries, timeout=timeout)
    parser = HrefParser()
    parser.feed(html)

    # include explicit data files + wordpress download endpoints that may omit extension
    supported_ext = (".csv", ".json", ".xlsx", ".xls") + ((".pdf",) if include_pdf else ())
    out: list[SourceFile] = []
    for href in parser.hrefs:
        abs_url = urljoin(index_url, href)
        lower_url = abs_url.lower()
        if not (lower_url.endswith(supported_ext) or "/download/" in lower_url):
            continue

        filename = _guess_filename_from_url(abs_url.split("?")[0], "downloaded_report")
        if not matches_target_year(f"{abs_url} {filename}", year):
            continue
        out.append(SourceFile(url=abs_url, filename=filename))
    # de-duplicate by URL
    unique: dict[str, SourceFile] = {item.url: item for item in out}
    return sorted(unique.values(), key=lambda x: x.filename.lower())


def discover_report_pages(index_url: str, year: int | None, retries: int = 4, timeout: int = 45) -> list[str]:
    html = fetch_text(index_url, retries=retries, timeout=timeout)
    parser = HrefParser()
    parser.feed(html)

    pages: list[str] = []
    for href in parser.hrefs:
        abs_url = urljoin(index_url, href)
        lower_url = abs_url.lower()
        if not any(k in lower_url for k in REPORT_PAGE_KEYWORDS):
            continue
        if not matches_target_year(abs_url, year):
            continue
        pages.append(abs_url)

    unique_pages = sorted(set(pages))
    return unique_pages


def classify_source(url: str) -> str:
    u = url.lower()
    if "harvest" in u:
        return "harvest"
    if "draw" in u:
        return "draw"
    return "other"


def looks_like_direct_download(url: str) -> bool:
    lower = url.lower()
    parsed = urlparse(url)
    return "/download/" in lower or "wpdmdl=" in (parsed.query or "").lower()


def save_sources(files: list[SourceFile], dest_dir: Path, retries: int = 4, timeout: int = 60) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    failed: list[str] = []
    for src in files:
        target = dest_dir / src.filename
        try:
            data = fetch_bytes(src.url, retries=retries, timeout=timeout)
            headers = getattr(fetch_bytes_with_retry, "_last_headers", None)
            if headers:
                disposition = headers.get("Content-Disposition", "")
                match = re.search(r'filename="?([^";]+)"?', disposition)
                if match:
                    hinted = Path(match.group(1).replace("%20", " ")).name
                    if Path(src.filename).suffix == "" and Path(hinted).suffix:
                        target = dest_dir / hinted

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
    hunt_code = get("huntCode")

    y = coerce_number(get("year"))
    year = int(y) if y is not None else fallback_year

    if any(v is None for v in [zone, species, weapon, applicants, tags, success]):
        return None
    if year is None:
        return None

    out = {
        "year": int(year),
        "zone": str(zone).strip(),
        "species": str(species).strip(),
        "weapon": str(weapon).strip(),
        "drawApplicants": int(round(applicants)),
        "drawTags": int(round(tags)),
        "hunterSuccessRate": round(float(success), 2),
    }
    if hunt_code is not None and str(hunt_code).strip():
        out["huntCode"] = str(hunt_code).strip()
    return out


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


def _extract_json_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("rows", "data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
    return []


def _normalize_complete_draw_row(item: dict[str, Any], fallback_year: int | None) -> dict[str, Any] | None:
    # Supports nested draw report rows shaped like:
    # {year, species, huntCode, unitDescription, licenses, applicants:{huntTotal:{total}}, ...}
    hunt_code = str(item.get("huntCode") or "").strip()
    if not hunt_code:
        return None

    year_value = coerce_number(item.get("year"))
    year = int(year_value) if year_value is not None else fallback_year
    if year is None:
        return None

    species = str(item.get("species") or "").strip() or "Unknown"
    unit_description = str(item.get("unitDescription") or "").strip()

    applicants_block = item.get("applicants") if isinstance(item.get("applicants"), dict) else {}
    hunt_total = applicants_block.get("huntTotal") if isinstance(applicants_block.get("huntTotal"), dict) else {}
    applicant_total = coerce_number(hunt_total.get("total"))
    if applicant_total is None:
        applicant_total = coerce_number(item.get("applicants"))

    licenses = coerce_number(item.get("licenses"))
    alloc = item.get("allocation") if isinstance(item.get("allocation"), dict) else {}
    by_res = alloc.get("licensesByResidency") if isinstance(alloc.get("licensesByResidency"), dict) else {}
    alloc_total = coerce_number(by_res.get("total"))

    tags = alloc_total if alloc_total is not None else licenses
    applicants = applicant_total if applicant_total is not None else licenses
    if applicants is None or tags is None:
        return None

    zone = unit_description or hunt_code
    if unit_description:
        unit_match = re.search(r"\bUnits?\s+([^:]+)", unit_description, flags=re.IGNORECASE)
        if unit_match:
            zone = unit_match.group(1).strip()

    return {
        "year": int(year),
        "zone": zone,
        "huntCode": hunt_code,
        "species": species,
        "weapon": "Any",
        "drawApplicants": int(round(applicants)),
        "drawTags": int(round(tags)),
        "hunterSuccessRate": 0.0,
    }


def _normalize_harvest_row(item: dict[str, Any], fallback_year: int | None) -> dict[str, Any] | None:
    # Supports harvest parser rows where draw applicants are unknown.
    year_value = coerce_number(item.get("year"))
    year = int(year_value) if year_value is not None else fallback_year
    if year is None:
        return None

    zone = str(item.get("zone") or item.get("gmu") or "").strip()
    species = str(item.get("species") or "").strip() or "Unknown"
    weapon = str(item.get("weapon") or "").strip() or "Any"
    success = coerce_number(item.get("hunterSuccessRate"))

    if not zone or success is None:
        return None

    out: dict[str, Any] = {
        "year": int(year),
        "zone": zone,
        "species": species,
        "weapon": weapon,
        "hunterSuccessRate": round(float(success), 2),
    }

    passthrough = [
        "season",
        "gmu",
        "type",
        "huntCode",
        "huntDates",
        "bagLimit",
        "licensesSold",
        "huntersReporting",
        "percentReporting",
        "estimatedBulls",
        "estimatedCows",
        "estimatedHarvestTotal",
        "satisfactionRating",
        "daysHunted",
    ]
    for key in passthrough:
        value = item.get(key)
        if value is not None and value != "":
            out[key] = value

    return out


def normalize_json(path: Path, fallback_year: int | None, manual_map: dict[str, str]) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload_rows = _extract_json_rows(payload)
    if not payload_rows:
        return []

    # Dedicated handler for complete draw-report style JSON rows.
    if all("huntCode" in row and "applicants" in row for row in payload_rows[: min(10, len(payload_rows))]):
        out: list[dict[str, Any]] = []
        for item in payload_rows:
            c = _normalize_complete_draw_row(item, fallback_year)
            if c:
                out.append(c)
        if out:
            print(f"info: parsed {len(out)} rows from nested draw-report JSON {path.name}", file=sys.stderr)
        return out

    rows: list[dict[str, Any]] = []
    sample_headers = list(payload_rows[0].keys()) if isinstance(payload_rows[0], dict) else []
    inferred = infer_column_map(sample_headers)
    column_map = {**inferred, **manual_map}

    for item in payload_rows:
        # Prefer harvest mapping when harvest-specific fields are present so we retain
        # full harvest context instead of collapsing to draw-only canonical fields.
        if "licensesSold" in item or "estimatedHarvestTotal" in item:
            harvest_row = _normalize_harvest_row(item, fallback_year)
            if harvest_row:
                rows.append(harvest_row)
                continue

        c = canonical_row(item, column_map, fallback_year)
        if c:
            rows.append(c)
            continue

        harvest_row = _normalize_harvest_row(item, fallback_year)
        if harvest_row:
            rows.append(harvest_row)

    if rows and any("licensesSold" in r and "drawApplicants" not in r for r in rows):
        print(f"info: parsed {len(rows)} rows from harvest-style JSON {path.name}", file=sys.stderr)
    return rows


def _normalize_merge_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _merge_key(row: dict[str, Any]) -> tuple[Any, ...]:
    # Prefer hunt-level identity when a hunt code is present. This prevents
    # duplicate points for the same zone + hunt tag coming from multiple files.
    year = row.get("year")
    zone = _normalize_merge_text(row.get("zone"))
    hunt_code = _normalize_merge_text(row.get("huntCode"))
    if hunt_code:
        return (year, zone, hunt_code)

    # Fallback for rows that do not have hunt codes.
    species = _normalize_merge_text(row.get("species"))
    weapon = _normalize_merge_text(row.get("weapon"))
    return (year, zone, species, weapon)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _prefer_new_string(current: Any, new: Any) -> bool:
    if _is_empty(new):
        return False
    if _is_empty(current):
        return True
    current_s = str(current).strip()
    new_s = str(new).strip()
    if current_s.lower() == "any" and new_s.lower() != "any":
        return True
    if current_s.lower() == "unknown" and new_s.lower() != "unknown":
        return True
    return len(new_s) > len(current_s)


def merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    numeric_max_keys = {
        "drawApplicants",
        "drawTags",
        "licensesSold",
        "huntersReporting",
        "percentReporting",
        "estimatedBulls",
        "estimatedCows",
        "estimatedHarvestTotal",
        "hunterSuccessRate",
        "satisfactionRating",
        "daysHunted",
    }

    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = _merge_key(row)
        if key not in merged:
            merged[key] = dict(row)
            continue

        existing = merged[key]
        for field, value in row.items():
            if _is_empty(value):
                continue

            current = existing.get(field)
            if _is_empty(current):
                existing[field] = value
                continue

            if field in numeric_max_keys:
                incoming = coerce_number(value)
                present = coerce_number(current)
                if incoming is not None and (present is None or incoming > present):
                    existing[field] = value
                continue

            if isinstance(value, str) and _prefer_new_string(current, value):
                existing[field] = value

    return list(merged.values())



def _iter_pdf_rows(path: Path) -> list[dict[str, str]]:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        print(
            "warning: pypdf not installed; skipping PDF parsing. Install with: python3 -m pip install pypdf",
            file=sys.stderr,
        )
        return []

    reader = PdfReader(str(path))
    lines: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        for line in text.splitlines():
            line = line.strip()
            if line:
                lines.append(line)

    if not lines:
        return []

    header_idx = -1
    split_mode = ""
    for idx, line in enumerate(lines):
        lower = normalize_header(line)
        if "," in line and any(k in lower for k in ["zone", "unit", "species", "applicants", "permits", "tags", "success"]):
            header_idx = idx
            split_mode = "comma"
            break
        if re.search(r"\s{2,}", line) and any(k in lower for k in ["zone", "unit", "species", "applicants", "permits", "tags", "success"]):
            header_idx = idx
            split_mode = "spaces"
            break

    if header_idx < 0:
        return []

    def split_line(line: str) -> list[str]:
        if split_mode == "comma":
            return next(csv.reader(io.StringIO(line)))
        return [c.strip() for c in re.split(r"\s{2,}", line.strip())]

    headers = split_line(lines[header_idx])
    rows: list[dict[str, str]] = []
    for line in lines[header_idx + 1 :]:
        parts = split_line(line)
        if len(parts) < max(2, len(headers) // 2):
            continue
        if len(parts) < len(headers):
            parts += [""] * (len(headers) - len(parts))
        if len(parts) > len(headers):
            parts = parts[: len(headers) - 1] + [" ".join(parts[len(headers) - 1 :])]
        row = {h: v for h, v in zip(headers, parts)}
        rows.append(row)
    return rows


def normalize_pdf(path: Path, fallback_year: int | None, manual_map: dict[str, str]) -> list[dict[str, Any]]:
    rows = _iter_pdf_rows(path)
    if not rows:
        print(f"skip {path.name}: unable to detect tabular PDF structure", file=sys.stderr)
        return []

    sample_headers = list(rows[0].keys())
    inferred = infer_column_map(sample_headers)
    column_map = {**inferred, **manual_map}
    missing_core = [k for k in ["zone", "species", "weapon", "drawApplicants", "drawTags", "hunterSuccessRate"] if k not in column_map]
    if missing_core:
        print(f"skip {path.name}: PDF table missing mappings for {missing_core}", file=sys.stderr)
        return []

    out: list[dict[str, Any]] = []
    for raw in rows:
        c = canonical_row(raw, column_map, fallback_year)
        if c:
            out.append(c)

    if out:
        print(f"info: parsed {len(out)} rows from PDF {path.name}", file=sys.stderr)
    else:
        print(f"skip {path.name}: parsed PDF table but no canonical rows matched", file=sys.stderr)
    return out

def _xlsx_read_rows(path: Path) -> list[list[str]]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows: list[list[str]] = []
    with ZipFile(path) as zf:
        names = zf.namelist()
        if "xl/sharedStrings.xml" in names:
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            shared = ["".join(t.text or "" for t in si.findall(".//a:t", ns)) for si in root.findall("a:si", ns)]
        else:
            shared = []

        sheets = sorted(n for n in names if n.startswith("xl/worksheets/sheet") and n.endswith(".xml"))
        for sheet in sheets:
            sroot = ET.fromstring(zf.read(sheet))
            for row in sroot.findall(".//a:sheetData/a:row", ns):
                cells: list[str] = []
                for c in row.findall("a:c", ns):
                    value_node = c.find("a:v", ns)
                    if value_node is None:
                        cells.append("")
                        continue
                    value = value_node.text or ""
                    if c.attrib.get("t") == "s" and value.isdigit() and int(value) < len(shared):
                        value = shared[int(value)]
                    cells.append(value)
                rows.append(cells)
    return rows


def normalize_draw_odds_xlsx(path: Path, fallback_year: int | None) -> list[dict[str, Any]]:
    rows = _xlsx_read_rows(path)
    if not rows:
        return []

    header_idx = -1
    for idx, row in enumerate(rows):
        norm = [normalize_header(c) for c in row]
        if "hunt" in norm and "unit/description" in norm and "permits" in norm:
            header_idx = idx
            break
    if header_idx < 0:
        print(f"skip {path.name}: did not find draw-odds header row in xlsx", file=sys.stderr)
        return []

    header = [normalize_header(c) for c in rows[header_idx]]
    try:
        hunt_col = header.index("hunt")
        unit_col = header.index("unit/description")
        permits_col = header.index("permits")
    except ValueError:
        print(f"skip {path.name}: missing required xlsx columns", file=sys.stderr)
        return []

    total_apps_col = next((i for i in range(permits_col + 1, len(header)) if header[i] == "t"), None)
    if total_apps_col is None:
        print(f"skip {path.name}: unable to infer total-applicants column", file=sys.stderr)
        return []

    data: list[dict[str, Any]] = []
    current_species = "Unknown"
    year = fallback_year
    for row in rows[header_idx + 1 :]:
        if not any((c or "").strip() for c in row):
            continue
        first = (row[0] if row else "").strip()
        if first.isalpha() and len(first) > 2 and first.upper() == first:
            current_species = first.title()
            continue

        hunt_code = (row[hunt_col] if hunt_col < len(row) else "").strip()
        unit_text = (row[unit_col] if unit_col < len(row) else "").strip()
        permits = coerce_number(row[permits_col] if permits_col < len(row) else None)
        applicants = coerce_number(row[total_apps_col] if total_apps_col < len(row) else None)

        if not hunt_code or permits is None or applicants is None:
            continue

        zone_match = re.search(r"\bUnit\s+([0-9A-Za-z]+)", unit_text, flags=re.IGNORECASE)
        zone = zone_match.group(1) if zone_match else (unit_text or hunt_code)
        if year is None:
            ymatch = re.search(r"(20\d{2})", path.name)
            year = int(ymatch.group(1)) if ymatch else None
        if year is None:
            continue

        data.append(
            {
                "year": int(year),
                "zone": zone,
                "huntCode": hunt_code,
                "species": current_species,
                "weapon": "Any",
                "drawApplicants": int(round(applicants)),
                "drawTags": int(round(permits)),
                "hunterSuccessRate": 0.0,
            }
        )

    if data:
        print(
            f"info: {path.name} parsed as draw-odds xlsx; hunterSuccessRate is set to 0.0 until harvest data is merged.",
            file=sys.stderr,
        )
    return data


def detect_file_kind(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".csv", ".json", ".xlsx", ".xls", ".pdf"}:
        return ext.lstrip(".")

    head = path.read_bytes()[:8]
    if head.startswith(b"PK"):
        return "xlsx"
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.lstrip().startswith((b"{", b"[")):
        return "json"
    return "unknown"


def load_manifest_sources(manifest_path: Path, year: int | None, include_pdf: bool = False) -> tuple[list[SourceFile], list[str]]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")

    file_entries = payload.get("files")
    if not isinstance(file_entries, list):
        raise ValueError("manifest is missing a 'files' list")

    report_pages = payload.get("reportPages")
    if not isinstance(report_pages, list):
        report_pages = []

    files: list[SourceFile] = []
    for idx, entry in enumerate(file_entries, start=1):
        if not isinstance(entry, dict):
            continue
        url = entry.get("url")
        if not isinstance(url, str) or not url:
            continue

        filename = entry.get("filename")
        if not isinstance(filename, str) or not filename:
            filename = Path(url.split("?")[0]).name or f"source_{idx}.dat"

        if not matches_target_year(f"{url} {filename}", year):
            continue

        lower = f"{url} {filename}".lower()
        if not include_pdf and ".pdf" in lower:
            continue

        files.append(SourceFile(url=url, filename=filename))

    return files, [str(p) for p in report_pages if isinstance(p, str)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape and normalize NM hunt/draw data")
    parser.add_argument("--year", type=int, help="Target year for filtering links and fallback output year")
    parser.add_argument("--index-url", default=DEFAULT_INDEX_URL, help="Page to scrape for downloadable report files")
    parser.add_argument(
        "--discover-pages-from",
        help="Optional page to discover report pages first (e.g. main hunting page), then scrape files from each report page",
    )
    parser.add_argument(
        "--discover-only",
        action="store_true",
        help="Only discover/print report pages and source file links (no downloads, no normalization)",
    )
    parser.add_argument(
        "--manifest-out",
        help="Optional JSON manifest path. With --discover-only it writes discovery output; otherwise, if this file exists it can be reused as manifest input.",
    )
    parser.add_argument(
        "--manifest-in",
        help="Optional JSON manifest input path (from --manifest-out). Uses listed files as download sources.",
    )
    parser.add_argument("--raw-dir", default="data/raw", help="Folder for downloaded source files")
    parser.add_argument("--retries", type=int, default=4, help="Network retries per request (default: 4)")
    parser.add_argument("--timeout", type=int, default=60, help="Network timeout in seconds per request (default: 60)")
    parser.add_argument(
        "--source-url",
        action="append",
        default=[],
        help="Direct downloadable report URL (can be used multiple times); bypasses index scraping when provided",
    )
    parser.add_argument("--out", help="Output normalized JSON (default: data/nm_hunt_data.<year|merged>.json)")
    parser.add_argument(
        "--column-map",
        help="Override mapping with canonical=source pairs, e.g. zone=Unit,species=Species,weapon=Weapon",
    )
    parser.add_argument(
        "--include-pdf",
        action="store_true",
        help="Include PDF links during discovery/manifest replay downloads (default: skip PDFs).",
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
        files: list[SourceFile] = []
        report_pages: list[str] = []
        if args.manifest_in:
            manifest_path = Path(args.manifest_in)
            files, report_pages = load_manifest_sources(manifest_path, args.year, include_pdf=args.include_pdf)
            if not files:
                print(f"warning: no downloadable files found in manifest {manifest_path}", file=sys.stderr)
        elif args.manifest_out and not args.discover_only and Path(args.manifest_out).exists():
            manifest_path = Path(args.manifest_out)
            files, report_pages = load_manifest_sources(manifest_path, args.year, include_pdf=args.include_pdf)
            if not files:
                print(f"warning: no downloadable files found in manifest {manifest_path}", file=sys.stderr)
        elif args.source_url:
            files = [
                SourceFile(url=u, filename=Path(u.split("?")[0]).name or f"source_{idx}.dat")
                for idx, u in enumerate(args.source_url, start=1)
            ]
            report_pages = ["(direct --source-url)"]
        elif args.discover_pages_from:
            try:
                report_pages = discover_report_pages(
                    args.discover_pages_from,
                    args.year,
                    retries=max(1, args.retries),
                    timeout=max(10, args.timeout),
                )
            except Exception as err:
                report_pages = []
                print(
                    f"warning: failed to discover report pages: {err}. "
                    "Try setting --index-url directly to a known report page.",
                    file=sys.stderr,
                )

            if not report_pages:
                print("warning: no report pages discovered.", file=sys.stderr)
            for page in report_pages:
                if looks_like_direct_download(page):
                    files.append(
                        SourceFile(
                            url=page,
                            filename=_guess_filename_from_url(page.split("?")[0], "downloaded_report"),
                        )
                    )
                    continue
                try:
                    files.extend(discover_links(page, args.year, include_pdf=args.include_pdf, retries=max(1, args.retries), timeout=max(10, args.timeout)))
                except Exception as err:
                    print(f"warning: failed scraping report page {page}: {err}", file=sys.stderr)
        else:
            report_pages = []
            try:
                report_pages = discover_report_pages(
                    args.index_url,
                    args.year,
                    retries=max(1, args.retries),
                    timeout=max(10, args.timeout),
                )
            except Exception:
                report_pages = []

            if report_pages:
                for page in report_pages:
                    if looks_like_direct_download(page):
                        files.append(
                            SourceFile(
                                url=page,
                                filename=_guess_filename_from_url(page.split("?")[0], "downloaded_report"),
                            )
                        )
                        continue
                    try:
                        files.extend(discover_links(page, args.year, include_pdf=args.include_pdf, retries=max(1, args.retries), timeout=max(10, args.timeout)))
                    except Exception as err:
                        print(f"warning: failed scraping report page {page}: {err}", file=sys.stderr)
            else:
                report_pages = [args.index_url]
                try:
                    files = discover_links(args.index_url, args.year, include_pdf=args.include_pdf, retries=max(1, args.retries), timeout=max(10, args.timeout))
                except Exception as err:
                    files = []
                    print(
                        f"warning: failed to fetch/parse index page: {err}. "
                        "Try --source-url for direct files or run with --no-download after saving files manually.",
                        file=sys.stderr,
                    )
        # de-dup discovered file URLs
        file_unique: dict[str, SourceFile] = {f.url: f for f in files}
        files = sorted(file_unique.values(), key=lambda s: s.filename.lower())

        if args.discover_only:
            manifest = {
                "year": args.year,
                "source": args.discover_pages_from or args.index_url,
                "reportPages": report_pages,
                "files": [
                    {
                        "url": f.url,
                        "filename": f.filename,
                        "category": classify_source(f.url),
                    }
                    for f in files
                ],
            }
            for entry in manifest["files"]:
                print(f"[{entry['category']}] {entry['url']}")
            if args.manifest_out:
                out = Path(args.manifest_out)
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
                print(f"manifest: {out}")
            return 0

        if not files:
            print(
                "No report links discovered on page (CSV/JSON/XLS/XLSX/PDF/download endpoints). "
                "Try a specific --index-url and/or increase --retries.",
                file=sys.stderr,
            )
        else:
            save_sources(files, raw_dir, retries=max(1, args.retries), timeout=max(10, args.timeout))

    all_files = sorted([p for p in raw_dir.iterdir() if p.is_file()]) if raw_dir.exists() else []
    if args.year and not all_files and raw_base.exists() and raw_base != raw_dir:
        # Compatibility fallback: if files are stored directly under raw dir (not raw/<year>),
        # include files that match requested year tokens in filename.
        year_token = str(args.year)
        all_files = sorted(
            [
                p
                for p in raw_base.iterdir()
                if p.is_file() and matches_target_year(p.name, args.year) and year_token in p.name
            ]
        )
        if all_files:
            print(
                f"info: using {len(all_files)} year-matching files from {raw_base} (fallback when {raw_dir} is empty)",
                file=sys.stderr,
            )
    classified: dict[str, list[Path]] = {"csv": [], "json": [], "xlsx": [], "pdf": [], "unknown": []}
    for p in all_files:
        classified.setdefault(detect_file_kind(p), []).append(p)

    csv_files = sorted(classified.get("csv", []))
    json_files = sorted(classified.get("json", []))
    xlsx_files = sorted(classified.get("xlsx", []))
    pdf_files = sorted(classified.get("pdf", []))
    unknown_files = sorted(classified.get("unknown", []))

    if xlsx_files:
        print(
            f"info: found {len(xlsx_files)} xls/xlsx files. Attempting built-in draw-odds XLSX parsing when possible.",
            file=sys.stderr,
        )
    if pdf_files:
        print(
            f"info: found {len(pdf_files)} PDF files. Attempting PDF table parsing (requires pypdf for text extraction).",
            file=sys.stderr,
        )
    if unknown_files:
        print(f"warning: skipped {len(unknown_files)} unsupported files: {[p.name for p in unknown_files]}", file=sys.stderr)

    normalized: list[dict[str, Any]] = []
    for f in csv_files:
        normalized.extend(normalize_csv(f, args.year, manual_map))
    for f in json_files:
        normalized.extend(normalize_json(f, args.year, manual_map))
    for f in xlsx_files:
        normalized.extend(normalize_draw_odds_xlsx(f, args.year))
    for f in pdf_files:
        normalized.extend(normalize_pdf(f, args.year, manual_map))

    cleaned = sorted(
        merge_rows(normalized),
        key=lambda r: (r.get("year", 0), str(r.get("species", "")), str(r.get("weapon", "")), str(r.get("zone", ""))),
    )

    suffix = str(args.year) if args.year else "merged"
    out_path = Path(args.out) if args.out else Path(f"data/nm_hunt_data.{suffix}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(cleaned, indent=2) + "\n", encoding="utf-8")

    print(f"normalized rows: {len(cleaned)}")
    print(f"output: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
