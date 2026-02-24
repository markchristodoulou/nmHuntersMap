#!/usr/bin/env python3
"""Extract ArcGIS FeatureServer layer data from an Instant App URL.

Given an Instant App URL containing `appid=...`, this script resolves the app
configuration through ArcGIS REST APIs, discovers the backing FeatureServer
service(s), and downloads all features from each layer as GeoJSON.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import urlopen

DEFAULT_APP_URL = (
    "https://nmdgf.maps.arcgis.com/apps/instant/basic/index.html"
    "?appid=b5e7938d6c164e9fae453326c3b87e35"
)
MAX_NAME_LEN = 80


def http_get_json(url: str, timeout: int = 60) -> dict[str, Any]:
    with urlopen(url, timeout=timeout) as response:
        body = response.read().decode("utf-8", "ignore")
    return json.loads(body)


def get_app_id(app_url: str) -> str:
    parsed = urlparse(app_url)
    query = parse_qs(parsed.query)
    app_ids = query.get("appid", [])
    if not app_ids:
        raise ValueError("No `appid` query parameter found in the supplied URL")
    return app_ids[0]


def get_instant_app_config(app_id: str) -> dict[str, Any]:
    url = f"https://www.arcgis.com/sharing/rest/content/items/{app_id}/data?f=json"
    return http_get_json(url)


def discover_feature_services(config: dict[str, Any]) -> list[str]:
    services: set[str] = set()

    values = config.get("values", {})
    search_sources = values.get("searchConfiguration", {}).get("sources", [])
    for source in search_sources:
        layer = source.get("layer", {})
        layer_url = layer.get("url", "")
        if "FeatureServer" in layer_url:
            services.add(layer_url.split("/FeatureServer")[0] + "/FeatureServer")

    webmap_id = values.get("webmap")
    if webmap_id:
        map_url = (
            f"https://www.arcgis.com/sharing/rest/content/items/{webmap_id}/data?f=json"
        )
        webmap = http_get_json(map_url)
        for layer in webmap.get("operationalLayers", []):
            layer_url = layer.get("url", "")
            if "FeatureServer" in layer_url:
                services.add(layer_url.split("/FeatureServer")[0] + "/FeatureServer")

    if not services:
        raise RuntimeError(
            "Could not discover FeatureServer URLs from app config/web map metadata"
        )

    return sorted(services)


def sanitize_name(value: str, max_len: int = MAX_NAME_LEN) -> str:
    value = value.strip() or "layer"
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    value = value.strip("._") or "layer"
    return value[:max_len].rstrip("._") or "layer"


def short_service_tag(service_url: str) -> str:
    slug = service_url.rstrip("/").split("/")[-2]
    digest = hashlib.sha1(service_url.encode("utf-8")).hexdigest()[:8]
    return f"{sanitize_name(slug, max_len=32)}_{digest}"


def query_layer_features(service_url: str, layer_id: int) -> dict[str, Any]:
    layer_url = f"{service_url}/{layer_id}"
    query_url = f"{layer_url}/query"

    layer_meta = http_get_json(f"{layer_url}?f=pjson")
    max_record_count = int(layer_meta.get("maxRecordCount") or 2000)

    all_features: list[dict[str, Any]] = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": str(offset),
            "resultRecordCount": str(max_record_count),
        }
        page = http_get_json(f"{query_url}?{urlencode(params)}")
        features = page.get("features", [])
        all_features.extend(features)

        exceeded = bool(page.get("properties", {}).get("exceededTransferLimit"))
        if not features or not exceeded:
            break

        offset += len(features)

    return {
        "type": "FeatureCollection",
        "features": all_features,
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
    }


def export_service_layers(service_url: str, output_dir: Path) -> None:
    metadata = http_get_json(f"{service_url}?f=pjson")
    descriptive_name = sanitize_name(metadata.get("serviceDescription") or "service")
    service_dir_name = sanitize_name(
        f"{descriptive_name}_{short_service_tag(service_url)}", max_len=64
    )
    service_dir = output_dir / service_dir_name
    service_dir.mkdir(parents=True, exist_ok=True)

    for layer in metadata.get("layers", []):
        layer_id = int(layer["id"])
        layer_name = sanitize_name(layer.get("name", f"layer_{layer_id}"), max_len=48)
        outpath = service_dir / f"{layer_id}_{layer_name}.geojson"

        print(f"Downloading {service_url}/{layer_id} -> {outpath}")
        geojson = query_layer_features(service_url, layer_id)
        outpath.write_text(json.dumps(geojson), encoding="utf-8")
        print(f"  Saved {len(geojson.get('features', []))} features")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download ArcGIS Instant App FeatureServer layers as GeoJSON"
    )
    parser.add_argument(
        "--app-url",
        default=DEFAULT_APP_URL,
        help="ArcGIS Instant App URL containing an appid query parameter",
    )
    parser.add_argument(
        "--output-dir",
        default="nm_arcgis_exports",
        help="Output directory for downloaded GeoJSON files",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    app_id = get_app_id(args.app_url)
    print(f"App ID: {app_id}")

    app_config = get_instant_app_config(app_id)
    service_urls = discover_feature_services(app_config)
    print(f"Discovered {len(service_urls)} FeatureServer service(s)")

    for service_url in service_urls:
        print(f"Exporting service: {service_url}")
        export_service_layers(service_url, output_dir)

    print(f"Done. Files written to: {output_dir}")


if __name__ == "__main__":
    main()
