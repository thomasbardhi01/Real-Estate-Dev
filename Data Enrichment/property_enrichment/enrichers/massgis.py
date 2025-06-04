"""MassGIS spatial data enricher."""

from __future__ import annotations

from typing import Dict, Any

from ..pipeline import BaseEnricher


class MassGISSpatialEnricher(BaseEnricher):
    """Example spatial enricher using MassGIS parcel data."""

    source_name = "MassGIS"

    def __init__(self, spatial_index: object | None = None) -> None:
        self.spatial_index = spatial_index

    def validate_requirements(self, property_data: Dict[str, Any]) -> bool:
        return bool(property_data.get("parcel_id"))

    def enrich(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        # Placeholder implementation. In a real system this would query spatial
        # datasets to obtain geometry and zoning information.
        parcel_id = property_data["parcel_id"]
        enrichment = {
            "geometry": f"GEOMETRY_FOR_{parcel_id}",
            "zoning": "Residential",
        }
        return enrichment

    def get_confidence_score(self, enrichment: Dict[str, Any]) -> float:
        return 0.9
