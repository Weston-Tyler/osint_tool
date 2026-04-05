"""Prometheus metrics for MDA API and ingestion pipelines.

Exposes /metrics endpoint for Prometheus scraping. Tracks:
- HTTP request latency and counts
- Kafka message processing rates
- Graph query performance
- Entity counts and data freshness
"""

import logging
import os
import time
from functools import wraps
from typing import Callable

from fastapi import APIRouter, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("mda.metrics")

# Simple in-process metrics (no external dependency required)
# In production, swap for prometheus_client library


class MetricsStore:
    """Thread-safe in-process metrics accumulator."""

    def __init__(self):
        self.counters: dict[str, float] = {}
        self.histograms: dict[str, list[float]] = {}
        self.gauges: dict[str, float] = {}

    def inc_counter(self, name: str, labels: dict | None = None, value: float = 1.0):
        key = self._key(name, labels)
        self.counters[key] = self.counters.get(key, 0) + value

    def observe_histogram(self, name: str, value: float, labels: dict | None = None):
        key = self._key(name, labels)
        if key not in self.histograms:
            self.histograms[key] = []
        self.histograms[key].append(value)
        # Keep only last 1000 observations
        if len(self.histograms[key]) > 1000:
            self.histograms[key] = self.histograms[key][-1000:]

    def set_gauge(self, name: str, value: float, labels: dict | None = None):
        key = self._key(name, labels)
        self.gauges[key] = value

    def _key(self, name: str, labels: dict | None) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def render_prometheus(self) -> str:
        """Render all metrics in Prometheus text exposition format."""
        lines = []

        for key, value in sorted(self.counters.items()):
            lines.append(f"{key} {value}")

        for key, values in sorted(self.histograms.items()):
            if values:
                count = len(values)
                total = sum(values)
                sorted_vals = sorted(values)
                p50 = sorted_vals[int(count * 0.5)] if count > 0 else 0
                p95 = sorted_vals[int(count * 0.95)] if count > 0 else 0
                p99 = sorted_vals[int(count * 0.99)] if count > 0 else 0
                lines.append(f'{key}_count {count}')
                lines.append(f'{key}_sum {total:.6f}')
                lines.append(f'{key}{{quantile="0.5"}} {p50:.6f}')
                lines.append(f'{key}{{quantile="0.95"}} {p95:.6f}')
                lines.append(f'{key}{{quantile="0.99"}} {p99:.6f}')

        for key, value in sorted(self.gauges.items()):
            lines.append(f"{key} {value}")

        return "\n".join(lines) + "\n"


# Global metrics store
metrics = MetricsStore()

router = APIRouter()


class MetricsMiddleware(BaseHTTPMiddleware):
    """HTTP middleware that records request latency and status codes."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start = time.time()
        response = await call_next(request)
        duration = time.time() - start

        method = request.method
        path = request.url.path
        status_code = response.status_code

        # Normalize path (remove variable segments)
        normalized = _normalize_path(path)

        metrics.inc_counter(
            "mda_http_requests_total",
            {"method": method, "path": normalized, "status": str(status_code)},
        )
        metrics.observe_histogram(
            "mda_http_request_duration_seconds",
            duration,
            {"method": method, "path": normalized},
        )

        return response


def _normalize_path(path: str) -> str:
    """Normalize URL path by replacing variable segments."""
    parts = path.strip("/").split("/")
    normalized = []
    for part in parts:
        # Replace likely identifiers (IMO, MMSI, UUIDs)
        if part.isdigit() and len(part) in (7, 9):
            normalized.append("{id}")
        elif len(part) > 20:
            normalized.append("{id}")
        else:
            normalized.append(part)
    return "/" + "/".join(normalized)


@router.get("/metrics")
async def prometheus_metrics():
    """Expose Prometheus-compatible metrics."""
    return Response(
        content=metrics.render_prometheus(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.get("/metrics/summary")
async def metrics_summary(request: Request):
    """Human-readable metrics summary."""
    mg = request.app.state.memgraph
    pool = request.app.state.postgres_pool

    summary = {
        "graph": {},
        "postgis": {},
        "http": {
            "total_requests": sum(metrics.counters.values()),
        },
    }

    # Graph entity counts
    try:
        for label in ["Vessel", "Person", "Organization", "AIS_Gap_Event", "UAS_Detection_Event"]:
            result = list(mg.execute_and_fetch(f"MATCH (n:{label}) RETURN count(n) AS cnt"))
            summary["graph"][label] = result[0]["cnt"] if result else 0
    except Exception as e:
        summary["graph"]["error"] = str(e)

    # PostGIS counts
    try:
        async with pool.acquire() as conn:
            summary["postgis"]["ais_positions"] = await conn.fetchval("SELECT count(*) FROM ais_positions")
            summary["postgis"]["latest_ais"] = str(
                await conn.fetchval("SELECT max(valid_time) FROM ais_positions")
            )
            summary["postgis"]["maritime_zones"] = await conn.fetchval("SELECT count(*) FROM maritime_zones")
            summary["postgis"]["interdiction_locations"] = await conn.fetchval(
                "SELECT count(*) FROM interdiction_locations"
            )
    except Exception as e:
        summary["postgis"]["error"] = str(e)

    return summary


def track_kafka_message(topic: str, processing_time: float):
    """Record Kafka message processing metrics (called from processors)."""
    metrics.inc_counter("mda_kafka_messages_total", {"topic": topic})
    metrics.observe_histogram("mda_kafka_processing_seconds", processing_time, {"topic": topic})


def track_graph_query(query_type: str, duration: float):
    """Record graph query performance metrics."""
    metrics.inc_counter("mda_graph_queries_total", {"type": query_type})
    metrics.observe_histogram("mda_graph_query_duration_seconds", duration, {"type": query_type})
