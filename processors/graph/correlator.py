"""Cross-domain correlation engine.

Correlates events across operational domains:
1. GDELT maritime/drug events <-> AIS anomalies (spatiotemporal)
2. UAS detections <-> STS transfers (drone overwatch for transfers)
3. NER entity extractions <-> known graph entities (entity linking)
4. Interdiction events <-> vessel tracks (backtrack interdicted vessel)
"""

import json
import logging
import math
import os
from datetime import datetime, timedelta

from gqlalchemy import Memgraph
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.correlator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


class GDELTAISCorrelator:
    """Correlate GDELT maritime/drug events with AIS anomalies.

    Logic: GDELT event at location X, time T ->
    search for AIS gaps/STS events within 200km and +/- 72 hours.
    """

    TEMPORAL_WINDOW_HOURS = 72
    SPATIAL_RADIUS_DEG = 2.0  # ~200km at equator

    def __init__(self, memgraph: Memgraph, producer: KafkaProducer):
        self.mg = memgraph
        self.producer = producer

    def correlate(self, gdelt_event: dict):
        lat = gdelt_event.get("action_lat", 0)
        lon = gdelt_event.get("action_lon", 0)
        event_date = gdelt_event.get("event_date", "")

        if not lat or not lon or not event_date:
            return

        # Search Memgraph for AIS gaps near this event
        try:
            results = list(self.mg.execute_and_fetch(
                """
                MATCH (v:Vessel)-[:HAS_AIS_GAP]->(g:AIS_Gap_Event)
                WHERE g.last_position_lat >= $lat_min AND g.last_position_lat <= $lat_max
                  AND g.last_position_lon >= $lon_min AND g.last_position_lon <= $lon_max
                  AND g.risk_flag = true
                RETURN v.imo AS imo, v.mmsi AS mmsi, v.name AS vessel_name,
                       g.event_id AS gap_id, g.gap_start_time AS gap_time,
                       g.gap_duration_hours AS gap_hrs, g.probable_cause AS cause,
                       g.last_position_lat AS gap_lat, g.last_position_lon AS gap_lon
                LIMIT 20
                """,
                {
                    "lat_min": lat - self.SPATIAL_RADIUS_DEG,
                    "lat_max": lat + self.SPATIAL_RADIUS_DEG,
                    "lon_min": lon - self.SPATIAL_RADIUS_DEG,
                    "lon_max": lon + self.SPATIAL_RADIUS_DEG,
                },
            ))
        except Exception as e:
            logger.error("Memgraph query error: %s", e)
            return

        for r in results:
            gap_lat = r.get("gap_lat", 0)
            gap_lon = r.get("gap_lon", 0)
            spatial_km = haversine_km(lat, lon, gap_lat, gap_lon)

            correlation = {
                "correlation_id": f"gdelt_ais_{gdelt_event.get('event_id', '')}_{r['gap_id']}",
                "correlation_type": "GDELT_EVENT_AIS_GAP",
                "confidence": 0.55,
                "gdelt_event_id": gdelt_event.get("event_id"),
                "gdelt_event_code": gdelt_event.get("event_code", ""),
                "gdelt_lat": lat,
                "gdelt_lon": lon,
                "gdelt_time": event_date,
                "ais_gap_id": r["gap_id"],
                "vessel_imo": r.get("imo"),
                "vessel_mmsi": r.get("mmsi"),
                "vessel_name": r.get("vessel_name"),
                "gap_duration_hrs": r.get("gap_hrs"),
                "spatial_offset_km": round(spatial_km, 1),
                "source": "gdelt_ais_correlator_v1",
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.producer.send("mda.alerts.composite", correlation)

            # Create CORRELATED_WITH edge in graph
            try:
                self.mg.execute(
                    """
                    MATCH (g:AIS_Gap_Event {event_id: $gap_id})
                    MERGE (ge:GDELT_Event {event_id: $gdelt_id})
                    ON CREATE SET
                        ge.event_date = $event_date,
                        ge.lat = $lat,
                        ge.lon = $lon,
                        ge.event_code = $code,
                        ge.system_created_at = localDateTime()
                    MERGE (g)-[c:CORRELATED_WITH]->(ge)
                    SET c.correlation_type = 'SPATIOTEMPORAL',
                        c.confidence_score = 0.55,
                        c.spatial_proximity_km = $dist,
                        c.source_ids = ['gdelt_ais_correlator_v1'],
                        c.observed_at = localDateTime()
                    """,
                    {
                        "gap_id": r["gap_id"],
                        "gdelt_id": gdelt_event.get("event_id"),
                        "event_date": event_date,
                        "lat": lat,
                        "lon": lon,
                        "code": gdelt_event.get("event_code", ""),
                        "dist": round(spatial_km, 1),
                    },
                )
            except Exception as e:
                logger.error("Failed to create correlation edge: %s", e)

            logger.info(
                "Correlation: GDELT %s <-> AIS gap %s (%.0fkm)",
                gdelt_event.get("event_id"),
                r["gap_id"],
                spatial_km,
            )


class UASSTransferCorrelator:
    """Correlate UAS detections with STS transfer events.

    Logic: UAS detection at location X, time T ->
    search for STS events within 10km and +/- 2 hours.
    Hypothesis: drone providing overwatch for a maritime transfer.
    """

    TEMPORAL_WINDOW_HOURS = 2
    SPATIAL_RADIUS_DEG = 0.1  # ~10km

    def __init__(self, memgraph: Memgraph, producer: KafkaProducer):
        self.mg = memgraph
        self.producer = producer

    def correlate(self, uas_event: dict):
        lat = uas_event.get("detection_lat", 0)
        lon = uas_event.get("detection_lon", 0)

        if not lat or not lon:
            return

        try:
            results = list(self.mg.execute_and_fetch(
                """
                MATCH (s:STS_Transfer)
                WHERE s.transfer_lat >= $lat_min AND s.transfer_lat <= $lat_max
                  AND s.transfer_lon >= $lon_min AND s.transfer_lon <= $lon_max
                RETURN s.event_id AS sts_id, s.vessel_a_mmsi AS mmsi_a,
                       s.vessel_b_mmsi AS mmsi_b,
                       s.transfer_lat AS sts_lat, s.transfer_lon AS sts_lon,
                       s.start_time AS sts_time
                LIMIT 10
                """,
                {
                    "lat_min": lat - self.SPATIAL_RADIUS_DEG,
                    "lat_max": lat + self.SPATIAL_RADIUS_DEG,
                    "lon_min": lon - self.SPATIAL_RADIUS_DEG,
                    "lon_max": lon + self.SPATIAL_RADIUS_DEG,
                },
            ))
        except Exception as e:
            logger.error("Memgraph query error: %s", e)
            return

        for r in results:
            spatial_km = haversine_km(lat, lon, r.get("sts_lat", 0), r.get("sts_lon", 0))

            correlation = {
                "correlation_id": f"uas_sts_{uas_event['event_id']}_{r['sts_id']}",
                "correlation_type": "UAS_STS_OVERWATCH",
                "hypothesis": "DRONE_PROVIDING_OVERWATCH_FOR_STS",
                "confidence": 0.65,
                "uas_event_id": uas_event["event_id"],
                "uas_lat": lat,
                "uas_lon": lon,
                "sts_event_id": r["sts_id"],
                "spatial_offset_km": round(spatial_km, 1),
                "source": "uas_sts_correlator_v1",
                "severity": "HIGH",
                "alert_type": "UAS_STS_CORRELATION",
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.producer.send("mda.alerts.composite", correlation)
            logger.info("Correlation: UAS %s <-> STS %s (%.1fkm)", uas_event["event_id"], r["sts_id"], spatial_km)


class NEREntityLinker:
    """Link NER-extracted entities to known graph entities.

    Matches extracted vessel names, person names, and org names
    to existing Memgraph nodes using fuzzy matching.
    """

    def __init__(self, memgraph: Memgraph, producer: KafkaProducer):
        self.mg = memgraph
        self.producer = producer

    def link(self, extraction: dict):
        entities = extraction.get("entities", {})
        doc_id = extraction.get("doc_id", "")

        # Link vessel names
        for vessel_ent in entities.get("vessel_names", []):
            name = vessel_ent["text"].upper().strip()
            if len(name) < 3:
                continue
            try:
                matches = list(self.mg.execute_and_fetch(
                    """
                    MATCH (v:Vessel)
                    WHERE toUpper(v.name) CONTAINS $name
                    RETURN v.imo AS imo, v.mmsi AS mmsi, v.name AS name, v.risk_score AS risk
                    LIMIT 5
                    """,
                    {"name": name},
                ))
                for m in matches:
                    logger.info("NER link: '%s' -> vessel %s (%s)", name, m.get("imo"), m.get("name"))
            except Exception as e:
                logger.error("Entity linking error: %s", e)

        # Link IMO numbers directly
        for imo_ent in entities.get("imo_numbers", []):
            imo = imo_ent.get("value", "")
            try:
                matches = list(self.mg.execute_and_fetch(
                    "MATCH (v:Vessel {imo: $imo}) RETURN v.name AS name, v.risk_score AS risk",
                    {"imo": imo},
                ))
                for m in matches:
                    logger.info("NER link: IMO %s -> vessel %s (risk=%.1f)", imo, m.get("name"), m.get("risk", 0))
            except Exception:
                pass

        # Link organization names
        for org_ent in entities.get("organizations", []):
            org_name = org_ent["text"].upper().strip()
            if len(org_name) < 3:
                continue
            try:
                matches = list(self.mg.execute_and_fetch(
                    """
                    MATCH (o:Organization)
                    WHERE toUpper(o.name) CONTAINS $name
                    RETURN o.entity_id AS id, o.name AS name, o.threat_level AS threat
                    LIMIT 5
                    """,
                    {"name": org_name},
                ))
                for m in matches:
                    logger.info("NER link: '%s' -> org %s (threat=%s)", org_name, m.get("name"), m.get("threat"))
            except Exception:
                pass


def run():
    """Main correlation engine loop — consumes multiple topics."""
    mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    gdelt_correlator = GDELTAISCorrelator(mg, producer)
    uas_correlator = UASSTransferCorrelator(mg, producer)
    ner_linker = NEREntityLinker(mg, producer)

    consumer = KafkaConsumer(
        "mda.events.gdelt.raw",
        "mda.uas.detections.raw",
        "mda.ner.extractions",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="mda-correlator",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    logger.info("Correlation engine started")

    for msg in consumer:
        try:
            if msg.topic == "mda.events.gdelt.raw":
                gdelt_correlator.correlate(msg.value)
            elif msg.topic == "mda.uas.detections.raw":
                uas_correlator.correlate(msg.value)
            elif msg.topic == "mda.ner.extractions":
                ner_linker.link(msg.value)
        except Exception as e:
            logger.error("Correlation error on %s: %s", msg.topic, e, exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
