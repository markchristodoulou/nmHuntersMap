#!/usr/bin/env python3
"""Parse the 2024-2025 NM elk harvest PDF into structured JSON.

Tuned specifically for New Mexico's "Elk_Harvest_Report_2024_Corrected.pdf" layout.
"""

from __future__ import annotations

import argparse
import io
import json
import re
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

DEFAULT_URL = "https://wildlife.dgf.nm.gov/download/2024-2025-elk-harvest-report/?wpdmdl=51252"
DEFAULT_OUT = "data/nm_elk_harvest_2024.json"

ROW_RE = re.compile(
    r"^(?P<typeLabel>.*?)\s*"
    r"(?P<huntCode>ELK-\d-\d{3})\s+"
    r"(?P<weapon>archery|muzzleloader|rifle)\s+"
    r"(?P<huntDates>.+?)\s+"
    r"(?P<bagLimit>[A-Z/]+)\s+"
    r"(?P<licensesSold>\d+)\s+"
    r"(?P<huntersReporting>\d+)\s+"
    r"(?P<percentReporting>\d+)%\s+"
    r"(?P<successRate>\d+)%\s+"
    r"(?P<estimatedBulls>\d+)\s+"
    r"(?P<estimatedCows>\d+)\s+"
    r"(?P<satisfactionRating>\d+(?:\.\d+)?)\s+"
    r"(?P<daysHunted>\d+(?:\.\d+)?)\s*$",
    flags=re.IGNORECASE,
)
GMU_RE = re.compile(r"\bGMU\s+([0-9]+[A-Z]?)\b", flags=re.IGNORECASE)


def fetch_pdf(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "nm-hunters-map-elk-parser/1.0"})
    with urlopen(req, timeout=90) as resp:
        return resp.read()


def extract_lines(pdf_bytes: bytes) -> list[str]:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as err:  # pragma: no cover - runtime dependency check
        raise RuntimeError("Missing dependency: pypdf. Install with `python3 -m pip install pypdf`") from err

    reader = PdfReader(io.BytesIO(pdf_bytes))
    lines: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        for line in text.splitlines():
            clean = re.sub(r"\s+", " ", line).strip()
            if clean:
                lines.append(clean)
    return lines


def _closest_gmu(line_index: int, gmu_markers: list[tuple[int, str]]) -> str | None:
    if not gmu_markers:
        return None
    nearest_i, nearest_gmu = min(gmu_markers, key=lambda item: abs(item[0] - line_index))
    return nearest_gmu


def parse_rows(lines: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    gmu_markers: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        m = GMU_RE.search(line)
        if m:
            gmu_markers.append((idx, m.group(1).upper()))

    for idx, line in enumerate(lines):
        match = ROW_RE.match(line)
        if not match:
            continue

        row = match.groupdict()
        type_label = row["typeLabel"].strip().replace(".", "") or "REG"
        m_gmu = _closest_gmu(idx, gmu_markers)

        licenses_sold = int(row["licensesSold"])
        hunters_reporting = int(row["huntersReporting"])
        estimated_bulls = int(row["estimatedBulls"])
        estimated_cows = int(row["estimatedCows"])

        out.append(
            {
                "year": 2024,
                "season": "2024-2025",
                "species": "Elk",
                "zone": m_gmu or type_label,
                "gmu": m_gmu,
                "type": type_label,
                "huntCode": row["huntCode"],
                "weapon": row["weapon"].title(),
                "huntDates": row["huntDates"],
                "bagLimit": row["bagLimit"],
                "licensesSold": licenses_sold,
                "huntersReporting": hunters_reporting,
                "percentReporting": int(row["percentReporting"]),
                "hunterSuccessRate": float(row["successRate"]),
                "estimatedBulls": estimated_bulls,
                "estimatedCows": estimated_cows,
                "estimatedHarvestTotal": estimated_bulls + estimated_cows,
                "satisfactionRating": float(row["satisfactionRating"]),
                "daysHunted": float(row["daysHunted"]),
            }
        )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse the NM 2024-2025 elk harvest report PDF into JSON")
    parser.add_argument("--url", default=DEFAULT_URL, help="Elk harvest report PDF URL")
    parser.add_argument("--pdf", help="Use a local PDF path instead of downloading --url")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output JSON path")
    args = parser.parse_args()

    pdf_bytes = Path(args.pdf).read_bytes() if args.pdf else fetch_pdf(args.url)
    lines = extract_lines(pdf_bytes)
    rows = parse_rows(lines)

    if not rows:
        raise SystemExit("No elk hunt rows were parsed. Verify the source PDF layout has not changed.")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"parsed rows: {len(rows)}")
    print(f"output: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
