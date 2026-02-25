const yearSelect = document.querySelector("#yearSelect");
const speciesSelect = document.querySelector("#speciesSelect");
const weaponSelect = document.querySelector("#weaponSelect");
const resultsTable = document.querySelector("#resultsTable");
const zoneDetails = document.querySelector("#zoneDetails");

const DATA_FILE = "./data/nm_hunt_data.sample.json";

let allRows = [];
let zoneFeatures = [];
let selectedRowId = null;
let map;
let geoLayer;

const pct = (value) => `${value.toFixed(1)}%`;

function calcMetrics(row) {
  const drawOdds = row.drawApplicants > 0 ? (row.drawTags / row.drawApplicants) * 100 : 0;
  const combined = (drawOdds * row.hunterSuccessRate) / 100;
  return { drawOdds, combined };
}

function rowId(row) {
  return `${row.zone}::${row.huntCode ?? ""}`;
}

function getFilteredRows() {
  const year = Number(yearSelect.value);
  const species = speciesSelect.value;
  const weapon = weaponSelect.value;

  return allRows
    .filter((row) => row.year === year && row.species === species && row.weapon === weapon)
    .map((row) => ({ ...row, huntCode: row.huntCode ?? "", ...calcMetrics(row) }))
    .sort((a, b) => b.combined - a.combined);
}

function fillSelect(select, values) {
  select.innerHTML = values
    .map((value) => `<option value="${value}">${value}</option>`)
    .join("");
}

function renderDetails(row) {
  if (!row) {
    zoneDetails.innerHTML = "Select a GMU from the map or table.";
    return;
  }

  zoneDetails.innerHTML = `
    <p><strong>Zone:</strong> ${row.zone}</p>
    <p><strong>Hunt Code:</strong> ${row.huntCode || "N/A"}</p>
    <p><strong>Applicants:</strong> ${row.drawApplicants}</p>
    <p><strong>Tags:</strong> ${row.drawTags}</p>
    <p><strong>Draw Odds:</strong> ${pct(row.drawOdds)}</p>
    <p><strong>Hunt Success:</strong> ${pct(row.hunterSuccessRate)}</p>
    <p><strong>Combined Chance:</strong> ${pct(row.combined)}</p>
  `;
}

function colorForCombined(combined, maxCombined) {
  if (!maxCombined) return "#dbeafe";
  const ratio = Math.max(0, Math.min(1, combined / maxCombined));
  if (ratio >= 0.85) return "#1d4ed8";
  if (ratio >= 0.65) return "#2563eb";
  if (ratio >= 0.45) return "#3b82f6";
  if (ratio >= 0.25) return "#60a5fa";
  return "#93c5fd";
}

function renderTable(rows) {
  resultsTable.innerHTML = rows
    .map(
      (row) => `
      <tr data-row-id="${rowId(row)}" class="${rowId(row) === selectedRowId ? "active" : ""}">
        <td>${row.zone}</td>
        <td>${row.huntCode || "-"}</td>
        <td>${row.drawApplicants}</td>
        <td>${row.drawTags}</td>
        <td>${pct(row.drawOdds)}</td>
        <td>${pct(row.hunterSuccessRate)}</td>
        <td>${pct(row.combined)}</td>
      </tr>
    `,
    )
    .join("");

  resultsTable.querySelectorAll("tr").forEach((tr) => {
    tr.addEventListener("click", () => {
      selectedRowId = tr.dataset.rowId;
      refresh();
    });
  });
}

function renderMap(rows) {
  const byZone = new Map();
  for (const row of rows) {
    if (!byZone.has(row.zone) || byZone.get(row.zone).combined < row.combined) {
      byZone.set(row.zone, row);
    }
  }
  const maxCombined = Math.max(...rows.map((row) => row.combined), 0);

  if (geoLayer) {
    geoLayer.remove();
  }

  geoLayer = L.geoJSON(zoneFeatures, {
    style: (feature) => {
      const zoneId = feature.properties.zone;
      const row = byZone.get(zoneId);
      return {
        color: row && rowId(row) === selectedRowId ? "#0f172a" : "#1e3a8a",
        weight: row && rowId(row) === selectedRowId ? 3 : 1.5,
        fillColor: row ? colorForCombined(row.combined, maxCombined) : "#e5e7eb",
        fillOpacity: row ? 0.72 : 0.3,
      };
    },
    onEachFeature: (feature, layer) => {
      const zoneId = feature.properties.zone;
      const row = byZone.get(zoneId);
      const tooltip = row
        ? `<div class="gmu-tooltip"><strong>GMU ${zoneId}</strong><br/>Combined: ${pct(row.combined)}<br/>Draw: ${pct(row.drawOdds)}<br/>Success: ${pct(row.hunterSuccessRate)}</div>`
        : `<div class="gmu-tooltip"><strong>GMU ${zoneId}</strong><br/>No data for selected filters.</div>`;

      layer.bindTooltip(tooltip);
      layer.on("click", () => {
        selectedRowId = row ? rowId(row) : null;
        refresh();
      });
    },
  }).addTo(map);
}

function refresh() {
  const rows = getFilteredRows();

  if (!selectedRowId && rows.length) {
    selectedRowId = rowId(rows[0]);
  }

  if (selectedRowId && !rows.some((row) => rowId(row) === selectedRowId)) {
    selectedRowId = rows[0] ? rowId(rows[0]) : null;
  }

  renderTable(rows);
  renderMap(rows);

  const selectedRow = rows.find((row) => rowId(row) === selectedRowId);
  renderDetails(selectedRow);
}

async function initMap() {
  map = L.map("zoneMap", { preferCanvas: true });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 12,
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(map);

  const geoResponse = await fetch("./data/nm_gmu_boundaries.geojson");
  const geojson = await geoResponse.json();
  zoneFeatures = geojson.features;

  const initialLayer = L.geoJSON(zoneFeatures).addTo(map);
  map.fitBounds(initialLayer.getBounds(), { padding: [10, 10] });
  initialLayer.remove();
}

async function init() {
  const response = await fetch(DATA_FILE);
  allRows = await response.json();

  const years = [...new Set(allRows.map((row) => row.year))].sort((a, b) => b - a);
  const species = [...new Set(allRows.map((row) => row.species))].sort();
  const weapons = [...new Set(allRows.map((row) => row.weapon))].sort();

  fillSelect(yearSelect, years);
  fillSelect(speciesSelect, species);
  fillSelect(weaponSelect, weapons);

  await initMap();

  [yearSelect, speciesSelect, weaponSelect].forEach((select) => {
    select.addEventListener("change", refresh);
  });

  refresh();
}

init();
