"""Example execution of the property enrichment pipeline."""

from property_enrichment import (
    PropertyEnrichmentPipeline,
    EnrichedDataStore,
)
from property_enrichment.enrichers import MassGISSpatialEnricher


if __name__ == "__main__":
    # Example assessor record
    sample_property = {
        "account_number": "123",
        "parcel_id": "42-001",
        "location": "1 Main St",
    }

    pipeline = PropertyEnrichmentPipeline()
    pipeline.register_enricher("spatial", MassGISSpatialEnricher())

    enriched = pipeline.enrich_property(sample_property)
    store = EnrichedDataStore("./enriched_output")
    store.save_enriched_property(enriched)

    print(enriched)
