"""ATAK Causal Overlay Publisher.

Continuously publishes active CausalPredictions as CoT events to ATAK.
Uses UDP multicast to reach all ATAK clients on the network.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from gqlalchemy import Memgraph

logger = logging.getLogger("mda.atak_overlay")

MEMGRAPH_HOST = os.getenv("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT = int(os.getenv("MEMGRAPH_PORT", "7687"))
ATAK_MULTICAST_GROUP = os.getenv("ATAK_MULTICAST_GROUP", "239.2.3.1")
ATAK_MULTICAST_PORT = int(os.getenv("ATAK_MULTICAST_PORT", "6969"))
CONFIDENCE_THRESHOLD = float(os.getenv("PREDICTION_CONFIDENCE_THRESHOLD", "0.45"))
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL_SECONDS", "300"))

DOMAIN_COT_TYPE = {
    "maritime": "a-h-G-U-C-I",
    "territorial": "a-h-G-E",
    "market": "a-n-G",
    "humanitarian": "a-f-G",
}


def prediction_to_cot(pred: dict) -> str:
    """Convert a CausalPrediction to CoT XML for ATAK."""
    now = datetime.now(timezone.utc)
    min_days = int(pred.get("predicted_timeframe_min", 0))
    max_days = int(pred.get("predicted_timeframe_max", 90))
    start_dt = now + timedelta(days=min_days)
    stale_dt = now + timedelta(days=max_days)
    domain = pred.get("domain", "")
    cot_type = DOMAIN_COT_TYPE.get(domain, "a-u-G")
    cot_uid = pred.get("atak_cot_uid") or f"WF-{str(uuid.uuid4())[:8]}"
    lat = pred.get("predicted_location_lat") or 0.0
    lon = pred.get("predicted_location_lon") or 0.0
    confidence = float(pred.get("confidence", 0))
    uncertainty_m = float(pred.get("predicted_location_uncertainty_km", 50)) * 1000

    event = ET.Element("event", {
        "version": "2.0",
        "uid": cot_uid,
        "type": cot_type,
        "time": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "start": start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "stale": stale_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "how": "m-g",
    })
    ET.SubElement(event, "point", {
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "hae": "0.0",
        "ce": f"{uncertainty_m:.0f}",
        "le": "9999999.0",
    })
    detail = ET.SubElement(event, "detail")
    pid_short = pred.get("prediction_id", "")[:8].upper()
    ET.SubElement(detail, "contact", {"callsign": f"WF-PRED-{pid_short}"})
    ET.SubElement(detail, "remarks").text = (
        f"[WorldFish | Conf: {confidence:.0%} | {domain.upper()}]\n"
        f"{pred.get('predicted_event_description', '')}\n"
        f"Timeframe: {min_days}-{max_days} days\n"
        f"Trigger: {pred.get('trigger_event_id', '')}"
    )
    wf = ET.SubElement(detail, "worldfish")
    wf.set("prediction_id", pred.get("prediction_id", ""))
    wf.set("confidence", f"{confidence:.4f}")
    wf.set("domain", domain)

    return ET.tostring(event, encoding="unicode", xml_declaration=True)


class ATAKOverlayPublisher:
    """Polls Memgraph for predictions and pushes CoT to ATAK."""

    def __init__(self):
        self.mg = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
        self.sent: set[str] = set()

    def fetch_active_predictions(self) -> list[dict]:
        results = list(self.mg.execute_and_fetch("""
            MATCH (p:CausalPrediction)
            WHERE p.confidence >= $threshold
              AND p.predicted_location_lat IS NOT NULL
            RETURN properties(p) AS props
            ORDER BY p.confidence DESC
            LIMIT 50
        """, {"threshold": CONFIDENCE_THRESHOLD}))
        return [dict(r["props"]) for r in results]

    def send_cot(self, cot_xml: str):
        data = cot_xml.encode("utf-8")
        self.sock.sendto(data, (ATAK_MULTICAST_GROUP, ATAK_MULTICAST_PORT))

    def run(self):
        logger.info("ATAK Overlay Publisher started (interval=%ds)", REFRESH_INTERVAL)
        while True:
            try:
                predictions = self.fetch_active_predictions()
                sent_count = 0
                for pred in predictions:
                    pid = pred.get("prediction_id", "")
                    if pid in self.sent:
                        continue
                    cot = prediction_to_cot(pred)
                    self.send_cot(cot)
                    self.sent.add(pid)
                    sent_count += 1
                    time.sleep(0.05)

                if sent_count:
                    logger.info("Sent %d predictions to ATAK", sent_count)
            except Exception as e:
                logger.error("ATAK overlay error: %s", e)

            time.sleep(REFRESH_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ATAKOverlayPublisher().run()
