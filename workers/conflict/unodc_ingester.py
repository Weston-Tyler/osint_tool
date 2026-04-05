"""UNODC crime statistics ingestion worker.

Parses CSV exports from the UNODC Data Portal for intentional homicide,
kidnapping, and drug seizure data. Normalizes to CausalEvent-compatible
dicts and publishes to Kafka.

Source: https://dataunodc.un.org/dp-intentional-homicide-victims
"""

import csv
import json
import logging
import os
from datetime import datetime, timezone

from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.unodc")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "mda.unodc.crime_stats"

# Column mappings for the different UNODC CSV exports.
# UNODC CSVs vary slightly across datasets; these mappings cover the
# most common column names seen in the portal downloads.

HOMICIDE_COLUMN_MAP = {
    "Country": ["Country", "country", "Territory"],
    "Year": ["Year", "year", "Reporting year"],
    "Count": ["Count", "count", "VALUE", "Value", "Number"],
    "Rate": ["Rate per 100k", "Rate", "rate", "Rate per 100,000 population"],
    "Sex": ["Sex", "sex", "Indicator"],
    "Region": ["Region", "region", "Sub-region"],
    "ISO": ["Iso3_code", "ISO", "iso3", "ISO3"],
}

SEIZURE_COLUMN_MAP = {
    "Country": ["Country", "country", "Territory"],
    "Year": ["Year", "year", "Reference Year"],
    "DrugType": ["Drug type", "Drug Group", "drug_type", "Drug"],
    "Quantity": ["Quantity (kg)", "Quantity", "quantity", "Best estimate (kg)", "Amount"],
    "Unit": ["Unit", "unit", "Measurement"],
    "Region": ["Region", "region", "Sub-region"],
    "ISO": ["Iso3_code", "ISO", "iso3", "ISO3"],
}

KIDNAPPING_COLUMN_MAP = {
    "Country": ["Country", "country", "Territory"],
    "Year": ["Year", "year", "Reporting year"],
    "Count": ["Count", "count", "VALUE", "Value", "Number"],
    "Rate": ["Rate per 100k", "Rate", "rate"],
    "Region": ["Region", "region", "Sub-region"],
    "ISO": ["Iso3_code", "ISO", "iso3", "ISO3"],
}

DATA_TYPE_TO_EVENT_TYPE = {
    "homicide": "INTENTIONAL_HOMICIDE",
    "kidnapping": "KIDNAPPING",
    "seizure": "DRUG_SEIZURE",
}


def _resolve_column(header: list[str], candidates: list[str]) -> str | None:
    """Find the first matching column name from a list of candidates."""
    header_lower = {h.lower().strip(): h for h in header}
    for candidate in candidates:
        if candidate in header:
            return candidate
        if candidate.lower().strip() in header_lower:
            return header_lower[candidate.lower().strip()]
    return None


class UNODCIngester:
    """Parses UNODC CSV data and publishes to Kafka."""

    def __init__(self, kafka_bootstrap: str = KAFKA_BOOTSTRAP):
        self.kafka_bootstrap = kafka_bootstrap
        self._producer = None

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def close(self):
        """Flush and close the Kafka producer."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None

    def load_homicide_data(self, filepath: str) -> list[dict]:
        """Parse a UNODC intentional homicide CSV.

        Expected columns (flexible naming): Country, Year, Count, Rate per 100k.

        Args:
            filepath: Path to the CSV file.

        Returns:
            List of parsed row dicts with standardized keys.
        """
        return self._load_csv(filepath, HOMICIDE_COLUMN_MAP, "homicide")

    def load_seizure_data(self, filepath: str) -> list[dict]:
        """Parse a UNODC drug seizure CSV.

        Expected columns (flexible naming): Country, Year, Drug type, Quantity (kg).

        Args:
            filepath: Path to the CSV file.

        Returns:
            List of parsed row dicts with standardized keys.
        """
        return self._load_csv(filepath, SEIZURE_COLUMN_MAP, "seizure")

    def load_kidnapping_data(self, filepath: str) -> list[dict]:
        """Parse a UNODC kidnapping statistics CSV.

        Args:
            filepath: Path to the CSV file.

        Returns:
            List of parsed row dicts with standardized keys.
        """
        return self._load_csv(filepath, KIDNAPPING_COLUMN_MAP, "kidnapping")

    def _load_csv(
        self,
        filepath: str,
        column_map: dict[str, list[str]],
        data_type: str,
    ) -> list[dict]:
        """Generic CSV loader with flexible column resolution.

        Args:
            filepath: Path to the CSV file.
            column_map: Mapping of canonical field names to candidate column names.
            data_type: Label for the data type (homicide, seizure, kidnapping).

        Returns:
            List of dicts with canonical keys.
        """
        rows: list[dict] = []

        with open(filepath, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            header = reader.fieldnames or []

            # Resolve canonical column names
            resolved: dict[str, str | None] = {}
            for canonical, candidates in column_map.items():
                resolved[canonical] = _resolve_column(header, candidates)

            missing = [k for k, v in resolved.items() if v is None]
            if missing:
                logger.warning(
                    "Could not resolve columns %s in %s (header: %s)",
                    missing, filepath, header,
                )

            for raw_row in reader:
                row: dict = {"_data_type": data_type}
                for canonical, col_name in resolved.items():
                    if col_name is not None:
                        row[canonical] = raw_row.get(col_name, "").strip()
                    else:
                        row[canonical] = None
                rows.append(row)

        logger.info("Loaded %d %s rows from %s", len(rows), data_type, filepath)
        return rows

    @staticmethod
    def normalize_to_causal_event(row: dict, data_type: str) -> dict:
        """Normalize a parsed UNODC row to a CausalEvent-compatible dict.

        Args:
            row: Parsed row dict with canonical keys.
            data_type: One of 'homicide', 'seizure', 'kidnapping'.

        Returns:
            Normalized event dict.
        """
        event_type = DATA_TYPE_TO_EVENT_TYPE.get(data_type, data_type.upper())
        country = row.get("Country", "")
        year = row.get("Year", "")
        iso3 = row.get("ISO", "")

        # Build a deterministic event ID
        id_parts = ["unodc", data_type, iso3 or country, str(year)]
        if data_type == "seizure":
            drug_type = row.get("DrugType", "unknown")
            id_parts.append(drug_type)
        event_id = "_".join(p.replace(" ", "_").lower() for p in id_parts if p)

        event = {
            "event_id": event_id,
            "source": "unodc",
            "event_type": event_type,
            "occurred_at": f"{year}-01-01" if year else None,
            "location_country": country,
            "location_country_iso": iso3,
            "location_region": row.get("Region", ""),
            "year": _safe_int(year),
            "confidence": 0.85,  # Official UN statistics
            "ingest_time": datetime.now(timezone.utc).isoformat(),
        }

        if data_type == "homicide":
            event["fatalities"] = _safe_int(row.get("Count"))
            event["rate_per_100k"] = _safe_float(row.get("Rate"))
            event["sex"] = row.get("Sex", "")
        elif data_type == "seizure":
            event["drug_type"] = row.get("DrugType", "")
            event["quantity_kg"] = _safe_float(row.get("Quantity"))
            event["unit"] = row.get("Unit", "kg")
        elif data_type == "kidnapping":
            event["count"] = _safe_int(row.get("Count"))
            event["rate_per_100k"] = _safe_float(row.get("Rate"))

        return event

    def ingest_from_csv(self, filepath: str, data_type: str) -> int:
        """Load a UNODC CSV, normalize rows, and publish to Kafka.

        Args:
            filepath: Path to the CSV file.
            data_type: One of 'homicide', 'seizure', 'kidnapping'.

        Returns:
            Number of events published.
        """
        loaders = {
            "homicide": self.load_homicide_data,
            "seizure": self.load_seizure_data,
            "kidnapping": self.load_kidnapping_data,
        }

        loader = loaders.get(data_type)
        if loader is None:
            logger.error("Unknown data type: %s (expected: %s)", data_type, list(loaders.keys()))
            return 0

        rows = loader(filepath)
        count = 0

        for row in rows:
            event = self.normalize_to_causal_event(row, data_type)
            self.producer.send(KAFKA_TOPIC, value=event)
            count += 1

        self.producer.flush()
        logger.info(
            "Published %d UNODC %s events from %s",
            count, data_type, filepath,
        )
        return count


def _safe_int(value) -> int | None:
    """Safely convert a value to int, returning None on failure."""
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (ValueError, TypeError):
        return None


def _safe_float(value) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if len(sys.argv) < 3:
        print("Usage: python unodc_ingester.py <filepath> <data_type>")
        print("  data_type: homicide | seizure | kidnapping")
        sys.exit(1)

    ingester = UNODCIngester()
    try:
        published = ingester.ingest_from_csv(sys.argv[1], sys.argv[2])
        print(f"Published {published} events")
    finally:
        ingester.close()
