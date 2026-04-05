"""Dead Letter Queue processor — categorize, alert, and optionally retry failed events.

Consumes from mda.dlq and:
1. Logs structured failure records
2. Categorizes errors (schema validation, source unavailable, parse error, etc.)
3. Tracks failure rate per source
4. Emits alerts when failure spikes are detected
5. Optionally retries transient failures
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.dlq_processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Alert thresholds
FAILURE_SPIKE_THRESHOLD = 100  # failures per source in a window
FAILURE_WINDOW_SECONDS = 300  # 5-minute rolling window
RETRY_MAX_ATTEMPTS = 3


class FailureTracker:
    """Track failure counts per source in a rolling time window."""

    def __init__(self, window_seconds: int = FAILURE_WINDOW_SECONDS):
        self.window = window_seconds
        self.failures: dict[str, list[float]] = defaultdict(list)

    def record(self, source: str) -> int:
        """Record a failure and return the current count in the window."""
        now = time.time()
        self.failures[source].append(now)
        # Prune old entries
        cutoff = now - self.window
        self.failures[source] = [t for t in self.failures[source] if t > cutoff]
        return len(self.failures[source])

    def get_counts(self) -> dict[str, int]:
        """Get current failure counts per source."""
        now = time.time()
        cutoff = now - self.window
        return {
            source: len([t for t in times if t > cutoff])
            for source, times in self.failures.items()
        }


class DLQError:
    """Categorized DLQ error."""

    CATEGORIES = {
        "ValidationError": "SCHEMA_VALIDATION",
        "JSONDecodeError": "PARSE_ERROR",
        "ConnectionError": "SOURCE_UNAVAILABLE",
        "TimeoutError": "SOURCE_TIMEOUT",
        "HTTPError": "HTTP_ERROR",
        "KeyError": "MISSING_FIELD",
        "ValueError": "VALUE_ERROR",
    }

    @classmethod
    def categorize(cls, error_str: str) -> str:
        for key, category in cls.CATEGORIES.items():
            if key.lower() in error_str.lower():
                return category
        return "UNKNOWN_ERROR"

    @classmethod
    def is_retryable(cls, category: str) -> bool:
        return category in ("SOURCE_UNAVAILABLE", "SOURCE_TIMEOUT", "HTTP_ERROR")


class DLQProcessor:
    """Process dead letter queue messages."""

    def __init__(self):
        self.tracker = FailureTracker()
        self.alert_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        self.retry_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        self.alerted_sources: dict[str, datetime] = {}

    def process_message(self, msg: dict):
        """Process a single DLQ message."""
        source = msg.get("source", "unknown")
        error = msg.get("error", "unknown")
        raw_preview = str(msg.get("raw", ""))[:200]

        category = DLQError.categorize(error)
        count = self.tracker.record(source)

        logger.warning(
            "DLQ [%s] source=%s category=%s error=%s raw=%s",
            count,
            source,
            category,
            error[:100],
            raw_preview[:80],
        )

        # Check for failure spike
        if count >= FAILURE_SPIKE_THRESHOLD:
            self._maybe_alert(source, count, category)

        # Retry transient failures
        retry_count = msg.get("_retry_count", 0)
        if DLQError.is_retryable(category) and retry_count < RETRY_MAX_ATTEMPTS:
            original_topic = msg.get("_original_topic")
            if original_topic:
                msg["_retry_count"] = retry_count + 1
                msg["_retry_at"] = datetime.utcnow().isoformat()
                self.retry_producer.send(original_topic, msg)
                logger.info("Retrying message to %s (attempt %d)", original_topic, retry_count + 1)

    def _maybe_alert(self, source: str, count: int, category: str):
        """Send alert if not recently alerted for this source."""
        now = datetime.utcnow()
        last_alert = self.alerted_sources.get(source)

        if last_alert and (now - last_alert) < timedelta(minutes=10):
            return  # Suppress duplicate alerts

        alert = {
            "alert_id": f"dlq_spike_{source}_{int(now.timestamp())}",
            "alert_type": "INGESTION_FAILURE_SPIKE",
            "severity": "HIGH",
            "message": f"DLQ spike: {count} failures from {source} in {FAILURE_WINDOW_SECONDS}s (category: {category})",
            "timestamp": now.isoformat(),
            "source": "dlq_processor",
            "metadata": {
                "failing_source": source,
                "failure_count": count,
                "error_category": category,
                "window_seconds": FAILURE_WINDOW_SECONDS,
                "all_counts": self.tracker.get_counts(),
            },
        }
        self.alert_producer.send("mda.alerts.composite", alert)
        self.alerted_sources[source] = now
        logger.error("ALERT: DLQ spike from %s — %d failures", source, count)


def run():
    """Main DLQ consumer loop."""
    processor = DLQProcessor()

    consumer = KafkaConsumer(
        "mda.dlq",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mda-dlq-processor",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    logger.info("DLQ processor started")

    for msg in consumer:
        try:
            processor.process_message(msg.value)
        except Exception as e:
            logger.error("Error processing DLQ message: %s", e, exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
