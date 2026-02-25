# New Mexico Hunter Decision Map (Starter)

This project helps you compare New Mexico GMUs by:

- **Draw odds** (how likely you are to draw a tag)
- **Hunt success odds** (success rate once drawn)
- **Combined chance** (draw odds × hunt success)

It is designed so you can load yearly data and quickly answer:

- Which GMU gives me the best draw chance for a species + weapon?
- Which GMU gives me the best expected overall outcome?
- How zones compare year-over-year.

## What this includes

- A Leaflet **real map overlay** (OpenStreetMap base tiles + GMU polygons from GeoJSON).
- Filters for year, species, and weapon.
- A ranked GMU table by combined chance.
- A selected-GMU details panel.
- Sample odds data and sample geospatial boundaries.

## Data files

- Odds data: `data/nm_hunt_data.sample.json`
- GMU polygons: `data/nm_gmu_boundaries.geojson`

### Odds row format

```json
{
  "year": 2024,
  "zone": "2A",
  "huntCode": "ELK-1-201",
  "species": "Elk",
  "weapon": "Rifle",
  "drawApplicants": 150,
  "drawTags": 18,
  "hunterSuccessRate": 34
}
```

### GeoJSON expectations

Each GMU feature must include:

- `properties.zone` that matches odds data `zone`
- Polygon geometry in lon/lat (EPSG:4326)

## Run locally

```bash
python3 -m http.server 4173
```

Then open `http://localhost:4173`.

## Calculations

- **Draw odds (%)** = `(drawTags / drawApplicants) * 100`
- **Hunt success (%)** = `hunterSuccessRate`
- **Combined chance (%)** = `(draw odds * hunt success) / 100`

`huntCode` is optional but recommended; when present, the web app displays it and uses `(zone + huntCode)` as row identity so rows are not collapsed when similar codes appear across zones.

## Important note on GMU geometry quality

`data/nm_gmu_boundaries.geojson` in this starter is a geospatially placed sample for the included zones so the app works end-to-end on a real basemap.
For production accuracy, replace it with official New Mexico Game and Fish GMU boundaries while keeping `properties.zone` consistent.


## Pulling real state data (scrape + normalize script)

A helper script is included at `scripts/fetch_nm_hunt_data.py` to automate collecting and normalizing yearly files.

### What it does

1. Scrapes a report index page for links to `.csv`, `.json`, `.xlsx`, `.xls`, and WordPress `/download/...` pages (PDF optional via `--include-pdf`).
2. Downloads discovered files into `data/raw/<year>/`.
3. Normalizes CSV/JSON/XLSX rows to the app schema and writes output JSON.
4. Attempts PDF table parsing too (install `pypdf` if you use `--include-pdf`).
5. Detects actual downloaded file types (including WordPress `/download/...` links with no extension) via response headers and magic bytes.

### Quick start

```bash
# Example: pull 2024 files from the NM elk report download page and normalize them
python3 scripts/fetch_nm_hunt_data.py \
  --year 2024 \
  --index-url "https://wildlife.dgf.nm.gov/download/2024-2025-elk-harvest-report/" \
  --retries 6 \
  --timeout 90 \
  --out data/nm_hunt_data.2024.json
```

Then point the app to your new output file by replacing the fetch path in `app.js`.

Tip for one-command yearly runs: using only `--year <YYYY>` now accepts filenames/URLs containing that year directly (`2024`) and season ranges (`2023-2024`, `2024-2025`).

### Discover all report pages/files first (harvest + draw)

Use this when you want to inventory all likely 2024 report files before downloading:

```bash
python3 scripts/fetch_nm_hunt_data.py \
  --year 2024 \
  --discover-pages-from "https://wildlife.dgf.nm.gov/home/hunting/" \
  --discover-only \
  --manifest-out data/nm_report_manifest.2024.json
```

This prints discovered links and tags each as `harvest`, `draw`, or `other` based on URL text.
If discovery returns direct WordPress `/download/...` URLs, the script now treats those as downloadable files automatically.

Use the saved manifest later to fetch from that fixed set of URLs (skips fresh page scraping). If the manifest file already exists, `--manifest-out` is reused as input:

```bash
python3 scripts/fetch_nm_hunt_data.py \
  --year 2024 \
  --manifest-out data/nm_report_manifest.2024.json \
  --retries 6 \
  --timeout 90 \
  --out data/nm_hunt_data.2024.json
```

### One command per year (recommended)

For most seasons, this single command is enough:

```bash
python3 scripts/fetch_nm_hunt_data.py \
  --year 2024 \
  --index-url "https://wildlife.dgf.nm.gov/home/hunting/" \
  --retries 6 \
  --timeout 90 \
  --out data/nm_hunt_data.2024.json
```

The script now tries report-page discovery from `--index-url` first, then scrapes those pages for files; it falls back to direct link scraping if no report pages are found.

If you explicitly want PDFs downloaded too, add `--include-pdf` (otherwise they are skipped to avoid PDF-only warnings).

### If column names differ

Use `--column-map` to map source columns to expected keys:

```bash
python3 scripts/fetch_nm_hunt_data.py --year 2024 --no-download   --column-map "zone=Unit,species=Species,weapon=Weapon,drawApplicants=Applicants,drawTags=Tags,hunterSuccessRate=Success %"
```

### Using a complete draw-report JSON file

If you already exported the full draw report as JSON (with nested `applicants` / `allocation` objects and `huntCode` rows), place it in `data/raw/<year>/` and run:

```bash
python3 scripts/fetch_nm_hunt_data.py --year 2024 --no-download --out data/nm_hunt_data.2024.json
```

The normalizer now reads this nested format directly and maps totals into app fields (`drawApplicants`, `drawTags`) with `hunterSuccessRate` set to `0.0` when harvest data is not present.

### If index-page scraping is unstable

You can bypass the index page entirely and download known files directly:

```bash
python3 scripts/fetch_nm_hunt_data.py   --year 2024   --source-url "https://example.org/nm/elk_draw_2024.csv"   --source-url "https://example.org/nm/deer_draw_2024.csv"   --retries 8   --timeout 120   --out data/nm_hunt_data.2024.json
```

### Network stability note (Windows `WinError 10054`)

If you see `urllib.error.URLError` with `WinError 10054`, the remote host closed the connection.
The script now retries automatically and supports higher timeout values; try:

```bash
python3 scripts/fetch_nm_hunt_data.py --year 2024 --retries 8 --timeout 120
```

You can also run once with `--no-download` after files are saved locally to avoid repeated network calls.

### XLSX/PDF note

The script includes built-in parsing for NM draw-odds XLSX files and a best-effort PDF parser (requires `pypdf`).
For PDF-heavy seasons, enable PDF discovery with `--include-pdf`.

### Special parser: 2024-2025 Elk harvest report

If you want a parser tuned specifically to the NM 2024-2025 elk harvest PDF layout, use:

```bash
python3 scripts/parse_elk_harvest_2024.py --out data/nm_elk_harvest_2024.json
```

This script targets the official `Elk_Harvest_Report_2024_Corrected.pdf` format and extracts one JSON row per hunt code with elk-specific fields (`gmu`, `type`, `huntCode`, `bagLimit`, `estimatedBulls`, `estimatedCows`, etc.) plus compatible app keys (`drawApplicants`, `drawTags`, `hunterSuccessRate`).

Reference pages used for real-file workflow:
- Draw workflow: `https://wildlife.dgf.nm.gov/hunting/applications-and-draw-information/how-new-mexico-draw-works/`
- Harvest workflow: `https://wildlife.dgf.nm.gov/hunting/harvest-reporting-information/`

## Verify commit history

Use this command from the repo root to confirm commits are present locally:

```bash
git log --oneline --decorate -n 10
```
