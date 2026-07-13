"""Kafka publisher for WorldFish predictive events.

Wraps each :class:`~worldfish.prediction.CausalPrediction` in the versioned
envelope (see :mod:`worldfish.contract`) and publishes it to
:data:`~worldfish.contract.PREDICTION_TOPIC`, where the graph-processor consumes
it. Kafka is imported lazily, and ``dry_run`` needs no broker at all — so this
module loads and a dry run works without kafka-python or a running Redpanda.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable

from .contract import PREDICTION_TOPIC, build_prediction_event
from .prediction import CausalPrediction

logger = logging.getLogger("mda.worldfish.publisher")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


class PredictionPublisher:
    """Publishes prediction envelopes to Kafka (or logs them in dry-run)."""

    def __init__(self, bootstrap: str | None = None, topic: str = PREDICTION_TOPIC, dry_run: bool = False):
        self.bootstrap = bootstrap or KAFKA_BOOTSTRAP
        self.topic = topic
        self.dry_run = dry_run
        self._producer = None

    def _get_producer(self):
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
        return self._producer

    def publish(self, predictions: Iterable[CausalPrediction]) -> list[dict]:
        """Publish predictions; returns the envelopes that were (or would be) sent."""
        envelopes = [build_prediction_event(p) for p in predictions]
        if self.dry_run:
            logger.info("[dry-run] %d prediction(s) not sent to %s", len(envelopes), self.topic)
            return envelopes
        producer = self._get_producer()
        for env in envelopes:
            producer.send(self.topic, env)
        producer.flush()
        logger.info("published %d prediction(s) to %s", len(envelopes), self.topic)
        return envelopes

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
            self._producer = None
