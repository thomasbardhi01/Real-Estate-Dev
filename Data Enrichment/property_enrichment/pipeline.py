"""Property enrichment pipeline and plugin architecture."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Generator


class BaseEnricher(ABC):
    """Abstract base class for enrichment plugins."""

    source_name: str = ""

    @abstractmethod
    def enrich(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """Return enrichment data for a property."""

    @abstractmethod
    def validate_requirements(self, property_data: Dict[str, Any]) -> bool:
        """Return True if the property has the necessary fields for this enricher."""

    def get_confidence_score(self, enrichment: Dict[str, Any]) -> float:
        """Optional confidence score. Defaults to 1.0."""
        return 1.0


class PropertyEnrichmentPipeline:
    """Pipeline that runs registered enrichers and produces standardized output."""

    def __init__(self, config_path: str | None = None) -> None:
        self.config_path = config_path
        self.enrichers: Dict[str, BaseEnricher] = {}
        self.output_schema = self._load_output_schema()

    def _load_output_schema(self) -> Dict[str, Any]:
        """Load output schema placeholder."""
        # In a real system this might load from a file. Here we just return an
        # empty dict as a placeholder for extensibility.
        return {}

    def register_enricher(self, name: str, enricher: BaseEnricher) -> None:
        """Register an enrichment plugin."""
        self.enrichers[name] = enricher

    def enrich_property(self, property_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run enrichment plugins and produce JSON-LD output."""
        enriched: Dict[str, Any] = {
            "@context": "http://schema.org/",
            "@type": "RealEstateProperty",
            "identifier": property_data.get("account_number"),
            "core_attributes": property_data,
            "enrichments": {},
            "relationships": [],
            "quality_scores": {"completeness": 1.0, "confidence": 1.0},
        }

        confidence_scores = []

        for name, enricher in self.enrichers.items():
            if not enricher.validate_requirements(property_data):
                continue
            result = enricher.enrich(property_data)
            confidence = enricher.get_confidence_score(result)
            confidence_scores.append(confidence)

            enriched["enrichments"][name] = {
                "source": getattr(enricher, "source_name", name),
                "timestamp": datetime.utcnow().isoformat(),
                "attributes": result,
            }

        if confidence_scores:
            enriched["quality_scores"]["confidence"] = sum(confidence_scores) / len(
                confidence_scores
            )

        return enriched


class EnrichedDataStore:
    """Simple file-based store for enriched properties."""

    def __init__(self, storage_path: str) -> None:
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

    def _record_path(self, property_id: str) -> Path:
        return self.storage_path / f"{property_id}.json"

    def save_enriched_property(self, enriched_property: Dict[str, Any]) -> None:
        path = self._record_path(str(enriched_property.get("identifier")))
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "data": enriched_property,
        }
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    def get_enriched_property(self, property_id: str) -> Dict[str, Any] | None:
        path = self._record_path(property_id)
        if not path.exists():
            return None
        last_line = None
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    last_line = json.loads(line)
        return last_line["data"] if last_line else None

    def export_for_neo4j(self) -> Generator[Dict[str, Any], None, None]:
        for file in self.storage_path.glob("*.json"):
            with file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    data = json.loads(line)["data"]
                    yield {
                        "node": data["identifier"],
                        "labels": [data["@type"]],
                        "properties": data["core_attributes"],
                    }

    def export_for_postgres(self) -> Generator[Dict[str, Any], None, None]:
        for file in self.storage_path.glob("*.json"):
            with file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    data = json.loads(line)["data"]
                    yield {
                        "id": data["identifier"],
                        "payload": json.dumps(data),
                    }
