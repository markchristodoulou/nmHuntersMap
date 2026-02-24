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

## Important note on GMU geometry quality

`data/nm_gmu_boundaries.geojson` in this starter is a geospatially placed sample for the included zones so the app works end-to-end on a real basemap.
For production accuracy, replace it with official New Mexico Game and Fish GMU boundaries while keeping `properties.zone` consistent.


## Pulling real state data (scrape + normalize script)

A helper script is included at `scripts/fetch_nm_hunt_data.py` to automate collecting and normalizing yearly files.

### What it does

1. Scrapes a report index page for links to `.csv`, `.json`, `.xlsx`, `.xls`, `.pdf`, and WordPress `/download/...` pages.
2. Downloads discovered files into `data/raw/<year>/`.
3. Normalizes CSV/JSON rows to the app schema and writes output JSON.

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

### If column names differ

Use `--column-map` to map source columns to expected keys:

```bash
python3 scripts/fetch_nm_hunt_data.py --year 2024 --no-download   --column-map "zone=Unit,species=Species,weapon=Weapon,drawApplicants=Applicants,drawTags=Tags,hunterSuccessRate=Success %"
```


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

The script warns if XLS/XLSX or PDF files are found (it does not parse spreadsheets or PDFs directly without extra dependencies).
Convert spreadsheets to CSV first, then run again with `--no-download`.

## Verify commit history

Use this command from the repo root to confirm commits are present locally:

```bash
git log --oneline --decorate -n 10
```
