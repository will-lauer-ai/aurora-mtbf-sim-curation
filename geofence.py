#!/usr/bin/env python3
"""Geographic helpers for bounding box and polygon filtering.

No external dependencies — pure Python ray-casting for point-in-polygon.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_geofence(path: str | Path) -> dict:
    """Load a geofence JSON file.

    Supported formats:
        {"type": "bbox",
         "min_lat": 35.0, "max_lat": 35.5,
         "min_lon": 139.0, "max_lon": 139.5}

        {"type": "polygon",
         "points": [{"lat": 35.0, "lon": 139.0}, {"lat": 35.5, "lon": 139.0}, ...]}
    """
    with open(path) as f:
        gf = json.load(f)
    if "type" not in gf:
        raise ValueError(f"Geofence missing 'type' field: {path}")
    if gf["type"] == "bbox":
        for k in ("min_lat", "max_lat", "min_lon", "max_lon"):
            if k not in gf:
                raise ValueError(f"bbox geofence missing '{k}': {path}")
    elif gf["type"] == "polygon":
        if "points" not in gf or len(gf["points"]) < 3:
            raise ValueError(f"polygon geofence needs >= 3 points: {path}")
    else:
        raise ValueError(f"Unknown geofence type '{gf['type']}': {path}")
    return gf


def save_geofence(path: str | Path, geofence: dict) -> None:
    """Save a geofence to JSON."""
    with open(path, "w") as f:
        json.dump(geofence, f, indent=2)


def geofence_bbox(geofence: dict) -> tuple[float, float, float, float]:
    """Return (min_lat, max_lat, min_lon, max_lon) for any geofence type.

    For bbox geofences, returns the stored values directly.
    For polygon geofences, computes the bounding box of the polygon vertices.
    """
    if geofence["type"] == "bbox":
        return (
            geofence["min_lat"],
            geofence["max_lat"],
            geofence["min_lon"],
            geofence["max_lon"],
        )
    elif geofence["type"] == "polygon":
        lats = [p["lat"] for p in geofence["points"]]
        lons = [p["lon"] for p in geofence["points"]]
        return min(lats), max(lats), min(lons), max(lons)
    else:
        raise ValueError(f"Unknown geofence type: {geofence['type']}")


def point_in_bbox(lat: float, lon: float, bbox: tuple[float, float, float, float]) -> bool:
    """Check if a point is inside a bounding box.

    Args:
        lat, lon: point coordinates
        bbox: (min_lat, max_lat, min_lon, max_lon)
    """
    min_lat, max_lat, min_lon, max_lon = bbox
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def point_in_polygon(lat: float, lon: float, polygon: list[dict]) -> bool:
    """Check if a point is inside a polygon using ray-casting.

    Args:
        lat, lon: point coordinates
        polygon: list of {"lat": float, "lon": float} dicts (vertices in order)
    """
    n = len(polygon)
    if n < 3:
        return False

    inside = False
    j = n - 1  # last vertex
    for i in range(n):
        yi = polygon[i]["lat"]
        xi = polygon[i]["lon"]
        yj = polygon[j]["lat"]
        xj = polygon[j]["lon"]

        # Check if the ray from (lon, lat) going right crosses this edge
        if ((yi > lat) != (yj > lat)) and \
           (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_geofence(lat: float, lon: float, geofence: dict) -> bool:
    """Check if a point is inside a geofence (dispatches on type).

    For polygon geofences, first does a fast bbox pre-check, then the
    exact polygon containment test.
    """
    if geofence["type"] == "bbox":
        return point_in_bbox(lat, lon, geofence_bbox(geofence))
    elif geofence["type"] == "polygon":
        # Fast bbox pre-check
        bbox = geofence_bbox(geofence)
        if not point_in_bbox(lat, lon, bbox):
            return False
        # Exact polygon test
        return point_in_polygon(lat, lon, geofence["points"])
    else:
        raise ValueError(f"Unknown geofence type: {geofence['type']}")


def filter_points_in_geofence(
    points: list[dict],
    geofence: dict,
) -> list[dict]:
    """Filter a list of GPS points to those inside a geofence.

    Each point must have "lat" and "lon" keys.
    Returns only the points that fall inside the geofence.
    """
    return [p for p in points if point_in_geofence(p["lat"], p["lon"], geofence)]


if __name__ == "__main__":
    # Quick self-test

    # Test bbox
    bbox_gf = {
        "type": "bbox",
        "min_lat": 35.0,
        "max_lat": 35.5,
        "min_lon": 139.0,
        "max_lon": 139.5,
    }
    assert point_in_geofence(35.2, 139.2, bbox_gf) is True
    assert point_in_geofence(34.9, 139.2, bbox_gf) is False
    assert point_in_geofence(35.2, 138.9, bbox_gf) is False
    assert point_in_geofence(35.6, 139.2, bbox_gf) is False

    # Test polygon (a rough triangle)
    poly_gf = {
        "type": "polygon",
        "points": [
            {"lat": 35.0, "lon": 139.0},
            {"lat": 35.5, "lon": 139.0},
            {"lat": 35.25, "lon": 139.5},
        ],
    }
    assert point_in_geofence(35.2, 139.1, poly_gf) is True
    assert point_in_geofence(35.4, 139.15, poly_gf) is True   # inside near top-left
    assert point_in_geofence(35.4, 139.4, poly_gf) is False    # outside right edge
    assert point_in_geofence(35.0, 139.5, poly_gf) is False    # outside

    # Test bbox of polygon
    min_lat, max_lat, min_lon, max_lon = geofence_bbox(poly_gf)
    assert min_lat == 35.0
    assert max_lat == 35.5
    assert min_lon == 139.0
    assert max_lon == 139.5

    print("All self-tests passed!")
