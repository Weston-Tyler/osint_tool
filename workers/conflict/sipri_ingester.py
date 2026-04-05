"""SIPRI Arms Transfers Database ingestion worker.

Parses CSV exports from the SIPRI Arms Transfers Database, normalizes
transfer records, detects arms buildup patterns, and publishes to Kafka.

Source: https://armstransfers.sipri.org/ArmsTransfer/TransferData
"""

import csv
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.sipri")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "mda.sipri.arms_transfers"

# SIPRI CSV column candidates (the export format can vary)
SIPRI_COLUMN_MAP = {
    "supplier": ["Supplier", "supplier", "Exporter", "Seller"],
    "recipient": ["Recipient", "recipient", "Importer", "Buyer"],
    "weapon_type": [
        "Armament category", "Weapon type", "weapon_type",
        "Description", "Weapon description", "Type",
    ],
    "weapon_designation": [
        "Weapon designation", "Designation", "designation",
        "Weapon", "Name",
    ],
    "quantity_ordered": ["No. ordered", "Quantity ordered", "Ordered", "Number ordered"],
    "quantity_delivered": ["No. delivered", "Quantity delivered", "Delivered", "Number delivered"],
    "year_of_order": ["Year of order", "Order year", "Year(s) of order"],
    "year_of_delivery": [
        "Year(s) of deliveries", "Delivery year", "Year of delivery",
        "Year(s) of delivery",
    ],
    "status": ["Status", "status", "Deal status"],
    "comment": ["Comments", "Comment", "comment", "SIPRI estimate"],
    "tiv_deal": ["SIPRI TIV of deal", "TIV deal", "TIV (deal)"],
    "tiv_delivered": [
        "SIPRI TIV of delivered weapons", "TIV delivered",
        "TIV (delivered)", "TIV of delivered",
    ],
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


def _safe_int(value) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(float(str(value).strip().replace(",", "")))
    except (ValueError, TypeError):
        return None


def _safe_float(value) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(str(value).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _parse_year_range(value: str) -> list[int]:
    """Parse year or year range strings like '2018', '2018-2020', '2018, 2020'."""
    if not value or not value.strip():
        return []
    years: list[int] = []
    for part in str(value).replace(",", " ").replace(";", " ").split():
        part = part.strip()
        if "-" in part:
            tokens = part.split("-")
            try:
                start = int(tokens[0])
                end = int(tokens[-1])
                years.extend(range(start, end + 1))
            except ValueError:
                pass
        else:
            try:
                years.append(int(part))
            except ValueError:
                pass
    return years


class SIPRIIngester:
    """Parses SIPRI arms transfer CSVs and publishes to Kafka."""

    def __init__(self, kafka_bootstrap: str = KAFKA_BOOTSTRAP):
        self.kafka_bootstrap = kafka_bootstrap
        self._producer = None
        self._transfers: list[dict] = []

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

    def load_transfers(self, filepath: str) -> list[dict]:
        """Parse a SIPRI arms transfer CSV export.

        Args:
            filepath: Path to the SIPRI CSV file.

        Returns:
            List of raw row dicts with resolved canonical keys.
        """
        rows: list[dict] = []

        with open(filepath, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            header = reader.fieldnames or []

            resolved: dict[str, str | None] = {}
            for canonical, candidates in SIPRI_COLUMN_MAP.items():
                resolved[canonical] = _resolve_column(header, candidates)

            missing = [k for k, v in resolved.items() if v is None]
            if missing:
                logger.warning(
                    "Could not resolve SIPRI columns %s in %s (header: %s)",
                    missing, filepath, header,
                )

            for raw_row in reader:
                row: dict = {}
                for canonical, col_name in resolved.items():
                    if col_name is not None:
                        row[canonical] = raw_row.get(col_name, "").strip()
                    else:
                        row[canonical] = None
                rows.append(row)

        self._transfers = rows
        logger.info("Loaded %d SIPRI transfer records from %s", len(rows), filepath)
        return rows

    @staticmethod
    def normalize_transfer(row: dict) -> dict:
        """Normalize a parsed SIPRI row to a structured transfer dict.

        Args:
            row: Parsed row dict with canonical keys.

        Returns:
            Normalized arms transfer dict.
        """
        supplier = row.get("supplier", "") or ""
        recipient = row.get("recipient", "") or ""
        weapon_type = row.get("weapon_type", "") or ""
        designation = row.get("weapon_designation", "") or ""

        order_years = _parse_year_range(row.get("year_of_order", "") or "")
        delivery_years = _parse_year_range(row.get("year_of_delivery", "") or "")

        # Primary year: earliest delivery year, or order year
        year = None
        if delivery_years:
            year = min(delivery_years)
        elif order_years:
            year = min(order_years)

        # Deterministic event ID
        id_parts = [
            "sipri",
            supplier.lower().replace(" ", "_"),
            recipient.lower().replace(" ", "_"),
            designation.lower().replace(" ", "_")[:30],
            str(year or "unknown"),
        ]
        event_id = "_".join(p for p in id_parts if p)

        return {
            "event_id": event_id,
            "source": "sipri",
            "event_type": "ARMS_TRANSFER",
            "supplier": supplier,
            "recipient": recipient,
            "weapon_type": weapon_type,
            "weapon_designation": designation,
            "quantity_ordered": _safe_int(row.get("quantity_ordered")),
            "quantity_delivered": _safe_int(row.get("quantity_delivered")),
            "year": year,
            "order_date": str(min(order_years)) if order_years else None,
            "delivery_date": str(min(delivery_years)) if delivery_years else None,
            "delivery_years": delivery_years,
            "status": row.get("status", ""),
            "tiv_deal": _safe_float(row.get("tiv_deal")),
            "tiv_delivered": _safe_float(row.get("tiv_delivered")),
            "comment": row.get("comment", ""),
            "confidence": 0.90,  # SIPRI is an authoritative source
            "ingest_time": datetime.now(timezone.utc).isoformat(),
        }

    def detect_buildup_pattern(
        self,
        country: str,
        lookback_years: int = 5,
    ) -> dict:
        """Detect accelerating arms import patterns for a given country.

        Flags a country as exhibiting a buildup if the trend in TIV delivered
        (or quantity delivered) over the lookback window is increasing year
        over year.

        Args:
            country: Country name to analyze (recipient).
            lookback_years: Number of years to analyze.

        Returns:
            Dict with buildup analysis: country, yearly totals, trend, flagged.
        """
        current_year = datetime.now(timezone.utc).year
        cutoff_year = current_year - lookback_years

        yearly_tiv: dict[int, float] = defaultdict(float)
        yearly_qty: dict[int, int] = defaultdict(int)

        for row in self._transfers:
            recipient = (row.get("recipient", "") or "").strip()
            if recipient.lower() != country.lower():
                continue

            delivery_years = _parse_year_range(row.get("year_of_delivery", "") or "")
            tiv = _safe_float(row.get("tiv_delivered")) or 0.0
            qty = _safe_int(row.get("quantity_delivered")) or 0

            for yr in delivery_years:
                if yr >= cutoff_year:
                    # Distribute TIV evenly across delivery years
                    n_years = len(delivery_years) if delivery_years else 1
                    yearly_tiv[yr] += tiv / n_years
                    yearly_qty[yr] += qty

        # Sort by year and compute trend
        years_sorted = sorted(yearly_tiv.keys())
        tiv_series = [yearly_tiv[y] for y in years_sorted]

        # Simple linear trend: positive slope means buildup
        flagged = False
        trend_slope = 0.0

        if len(tiv_series) >= 3:
            n = len(tiv_series)
            x_mean = (n - 1) / 2.0
            y_mean = sum(tiv_series) / n

            numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(tiv_series))
            denominator = sum((i - x_mean) ** 2 for i in range(n))

            if denominator > 0:
                trend_slope = numerator / denominator
                # Flag if slope is positive and last year exceeds average
                flagged = trend_slope > 0 and (
                    tiv_series[-1] > y_mean * 1.2 if y_mean > 0 else False
                )

        result = {
            "country": country,
            "lookback_years": lookback_years,
            "yearly_tiv": {str(y): round(yearly_tiv[y], 2) for y in years_sorted},
            "yearly_quantity": {str(y): yearly_qty[y] for y in years_sorted},
            "trend_slope": round(trend_slope, 4),
            "buildup_flagged": flagged,
            "analysis_time": datetime.now(timezone.utc).isoformat(),
        }

        if flagged:
            logger.warning(
                "Arms buildup pattern detected for %s (slope=%.4f)",
                country, trend_slope,
            )
        else:
            logger.info(
                "No buildup pattern for %s (slope=%.4f)",
                country, trend_slope,
            )

        return result

    def ingest_from_csv(self, filepath: str) -> int:
        """Load SIPRI CSV, normalize, and publish to Kafka.

        Args:
            filepath: Path to the SIPRI CSV file.

        Returns:
            Number of transfer records published.
        """
        raw_rows = self.load_transfers(filepath)
        count = 0

        for row in raw_rows:
            transfer = self.normalize_transfer(row)
            self.producer.send(KAFKA_TOPIC, value=transfer)
            count += 1

        self.producer.flush()
        logger.info("Published %d SIPRI arms transfer records from %s", count, filepath)
        return count


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python sipri_ingester.py <filepath> [--buildup <country>]")
        sys.exit(1)

    ingester = SIPRIIngester()
    try:
        published = ingester.ingest_from_csv(sys.argv[1])
        print(f"Published {published} transfer records")

        # Optional buildup detection
        if len(sys.argv) >= 4 and sys.argv[2] == "--buildup":
            result = ingester.detect_buildup_pattern(sys.argv[3])
            print(json.dumps(result, indent=2))
    finally:
        ingester.close()
