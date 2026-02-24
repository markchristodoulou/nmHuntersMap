#!/usr/bin/env python3
"""Convert ArcGIS-exported GMU GeoJSON into nmHuntersMap app format.

The app expects each feature to have `properties.zone` so it can join against
hunt rows (`row.zone`). This script reads a source GeoJSON, chooses a zone field
(or uses one supplied with --zone-field), and writes a normalized GeoJSON.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

DEFAULT_INPUT = "0_Game_Management_Units.geojson"
DEFAULT_OUTPUT = "data/nm_gmu_boundaries.geojson"

# Ordered by most likely for ArcGIS exports used in this repo/workflow.
ZONE_FIELD_CANDIDATES = [
    "zone",
    "GMU",
    "gmu",
    "UNIT",
    "Unit",
    "unit",
    "GMU_NAME",
    "GMU_ID",
    "NAME",
    "Name",
]


def load_geojson(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def detect_zone_field(features: list[dict[str, Any]], explicit: str | None) -> str:
    if explicit:
        return explicit

    if not features:
        raise ValueError("Input has no features; cannot detect zone field")

    # Prefer fields present in first feature properties.
    props = features[0].get("properties", {})
    for candidate in ZONE_FIELD_CANDIDATES:
        if candidate in props:
            return candidate

    # Fallback: first string-like property key with non-empty value.
    for key, value in props.items():
        if isinstance(value, (str, int, float)) and str(value).strip():
            return key

    raise ValueError(
        "Could not detect a zone field. Provide one explicitly with --zone-field."
    )


def to_zone_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def convert_features(features: list[dict[str, Any]], zone_field: str) -> list[dict[str, Any]]:
    converted: list[dict[str, Any]] = []

    for feature in features:
        properties = dict(feature.get("properties", {}))
        zone_value = to_zone_string(properties.get(zone_field))

        if not zone_value:
            # Skip unusable rows that cannot join to hunt data.
            continue

        properties["zone"] = zone_value

        converted.append(
            {
                "type": "Feature",
                "properties": properties,
                "geometry": feature.get("geometry"),
            }
        )

    return converted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert GMU GeoJSON to nmHuntersMap app-ready schema"
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Source GeoJSON path")
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT, help="Destination GeoJSON path"
    )
    parser.add_argument(
        "--zone-field",
        default=None,
        help="Property field to map into `properties.zone` (auto-detected if omitted)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    data = load_geojson(input_path)
    features = data.get("features", [])

    zone_field = detect_zone_field(features, args.zone_field)
    converted = convert_features(features, zone_field)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "type": "FeatureCollection",
        "features": converted,
    }
    output_path.write_text(json.dumps(output), encoding="utf-8")

    print(f"Zone field used: {zone_field}")
    print(f"Input features: {len(features)}")
    print(f"Output features: {len(converted)}")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
