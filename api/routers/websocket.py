"""WebSocket streaming endpoint for real-time MDA alerts.

Clients connect to /v1/ws/alerts and receive JSON-encoded alerts
as they are produced by the event processing pipeline.

Supports optional filters via query parameters:
- severity: CRITICAL, HIGH, MEDIUM, LOW
- alert_type: SANCTIONED_VESSEL_PROXIMITY, AIS_GAP, STS_DETECTED, UAS_DETECTION, etc.
- region: lat,lon,radius_km bounding circle
"""

import asyncio
import json
import logging
import os
from datetime import datetime

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer

logger = logging.getLogger("mda.api.websocket")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

router = APIRouter()


class ConnectionManager:
    """Manage active WebSocket connections with per-client filters."""

    def __init__(self):
        self.active_connections: list[dict] = []

    async def connect(self, websocket: WebSocket, filters: dict):
        await websocket.accept()
        self.active_connections.append({"ws": websocket, "filters": filters})
        logger.info("WebSocket connected (%d active), filters=%s", len(self.active_connections), filters)

    def disconnect(self, websocket: WebSocket):
        self.active_connections = [c for c in self.active_connections if c["ws"] != websocket]
        logger.info("WebSocket disconnected (%d active)", len(self.active_connections))

    def _matches_filter(self, alert: dict, filters: dict) -> bool:
        """Check if an alert matches the client's subscription filters."""
        if filters.get("severity"):
            if alert.get("severity") not in filters["severity"]:
                return False

        if filters.get("alert_type"):
            if alert.get("alert_type") not in filters["alert_type"]:
                return False

        if filters.get("region"):
            lat = alert.get("lat") or alert.get("subject_lat") or alert.get("detection_lat")
            lon = alert.get("lon") or alert.get("subject_lon") or alert.get("detection_lon")
            if lat is not None and lon is not None:
                from processors.graph.event_processor import haversine_km

                r = filters["region"]
                dist = haversine_km(r["lat"], r["lon"], float(lat), float(lon))
                if dist > r["radius_km"]:
                    return False

        return True

    async def broadcast(self, alert: dict):
        """Send alert to all matching connected clients."""
        disconnected = []
        for conn in self.active_connections:
            if self._matches_filter(alert, conn["filters"]):
                try:
                    await conn["ws"].send_json(alert)
                except Exception:
                    disconnected.append(conn["ws"])

        for ws in disconnected:
            self.disconnect(ws)


manager = ConnectionManager()


async def kafka_alert_consumer():
    """Background task: consume Kafka alerts and broadcast to WebSocket clients."""
    consumer = KafkaConsumer(
        "mda.alerts.composite",
        "mda.ais.gaps.detected",
        "mda.ais.encounters.detected",
        "mda.uas.detections.raw",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"ws-broadcaster-{os.getpid()}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        consumer_timeout_ms=1000,  # Non-blocking poll
    )

    logger.info("WebSocket Kafka consumer started")

    while True:
        try:
            # Poll with short timeout to allow asyncio event loop to breathe
            messages = consumer.poll(timeout_ms=500, max_records=100)
            for topic_partition, records in messages.items():
                for record in records:
                    alert = record.value
                    alert["_topic"] = record.topic
                    alert["_received_at"] = datetime.utcnow().isoformat()
                    await manager.broadcast(alert)
        except Exception as e:
            logger.error("Kafka consumer error: %s", e)

        await asyncio.sleep(0.1)


@router.websocket("/ws/alerts")
async def websocket_alerts(
    websocket: WebSocket,
    severity: str | None = Query(None),
    alert_type: str | None = Query(None),
    region: str | None = Query(None),
):
    """WebSocket endpoint for streaming MDA alerts.

    Query params:
        severity: comma-separated list (e.g., "CRITICAL,HIGH")
        alert_type: comma-separated list (e.g., "AIS_GAP,STS_DETECTED")
        region: "lat,lon,radius_km" (e.g., "5.0,-76.0,200")
    """
    filters: dict = {}

    if severity:
        filters["severity"] = [s.strip().upper() for s in severity.split(",")]
    if alert_type:
        filters["alert_type"] = [t.strip() for t in alert_type.split(",")]
    if region:
        try:
            parts = region.split(",")
            filters["region"] = {
                "lat": float(parts[0]),
                "lon": float(parts[1]),
                "radius_km": float(parts[2]),
            }
        except (ValueError, IndexError):
            pass

    await manager.connect(websocket, filters)

    try:
        while True:
            # Keep connection alive; receive pings or filter updates
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                # Allow clients to update filters dynamically
                if msg.get("action") == "update_filters":
                    for conn in manager.active_connections:
                        if conn["ws"] == websocket:
                            conn["filters"].update(msg.get("filters", {}))
                            await websocket.send_json({"status": "filters_updated", "filters": conn["filters"]})
                            break
                elif msg.get("action") == "ping":
                    await websocket.send_json({"status": "pong", "time": datetime.utcnow().isoformat()})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)
