"""CoT (Cursor-on-Target) publisher for ATAK/TAK integration.

Consumes MDA alerts from Kafka and publishes CoT XML to TAK Server
for display on ATAK field operator clients.
"""

import json
import logging
import os
import socket
import ssl
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

from kafka import KafkaConsumer

logger = logging.getLogger("mda.cot_publisher")

TAK_SERVER_HOST = os.getenv("TAK_SERVER_HOST", "localhost")
TAK_SERVER_PORT = int(os.getenv("TAK_SERVER_PORT", "8089"))
TAK_SERVER_CERT = os.getenv("TAK_SERVER_CERT", "/certs/client.pem")
TAK_SERVER_KEY = os.getenv("TAK_SERVER_KEY", "/certs/client.key")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def build_vessel_cot(vessel: dict) -> str:
    """Build a CoT XML document for a vessel alert.

    CoT type taxonomy for maritime:
      a-h-S-X-M = hostile surface maritime (sanctioned/illicit)
      a-u-S-X-M = unknown surface maritime (dark vessel)
      a-n-S-X-M = neutral surface maritime
    """
    now = datetime.utcnow()
    stale = now + timedelta(minutes=30)
    time_str = now.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    stale_str = stale.strftime("%Y-%m-%dT%H:%M:%S.00Z")

    uid_source = vessel.get("imo") or vessel.get("mmsi", "unknown")
    uid = f"MDA-VESSEL-{uid_source}"

    # CoT type based on risk/sanctions
    if vessel.get("sanctions_status") == "SANCTIONED":
        cot_type = "a-h-S-X-M"
    elif vessel.get("ais_status") == "DARK":
        cot_type = "a-u-S-X-M"
    elif vessel.get("risk_score", 0) >= 7.0:
        cot_type = "a-u-S-X-M"
    else:
        cot_type = "a-n-S-X-M"

    event = ET.Element(
        "event",
        {"version": "2.0", "uid": uid, "type": cot_type, "time": time_str, "start": time_str, "stale": stale_str, "how": "m-g"},
    )

    lat = vessel.get("last_ais_position_lat") or vessel.get("lat", 0)
    lon = vessel.get("last_ais_position_lon") or vessel.get("lon", 0)
    ET.SubElement(event, "point", {"lat": str(lat), "lon": str(lon), "hae": "9999999.0", "ce": "100.0", "le": "9999999.0"})

    detail = ET.SubElement(event, "detail")
    ET.SubElement(detail, "contact", {"callsign": (vessel.get("name") or uid_source)[:20]})

    risk_score = vessel.get("risk_score", 0)
    remarks_text = (
        f"IMO:{vessel.get('imo', 'N/A')} MMSI:{vessel.get('mmsi', 'N/A')} "
        f"FLAG:{vessel.get('flag_state', 'N/A')} RISK:{risk_score:.1f}/10 "
        f"SANCTIONS:{vessel.get('sanctions_status', 'N/A')} AIS:{vessel.get('ais_status', 'N/A')}"
    )
    ET.SubElement(detail, "remarks").text = remarks_text

    mda_detail = ET.SubElement(detail, "mda_vessel")
    mda_detail.set("imo", vessel.get("imo", ""))
    mda_detail.set("mmsi", vessel.get("mmsi", ""))
    mda_detail.set("risk", str(risk_score))

    return ET.tostring(event, encoding="unicode", xml_declaration=True)


def build_uas_cot(detection: dict) -> str:
    """Build CoT XML for a UAS detection event."""
    now = datetime.utcnow()
    stale = now + timedelta(minutes=5)
    time_str = now.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    stale_str = stale.strftime("%Y-%m-%dT%H:%M:%S.00Z")

    uid = f"MDA-UAS-{detection['event_id']}"
    cot_type = "a-u-A-M-F-Q-r"  # Unknown UAS

    event = ET.Element(
        "event",
        {"version": "2.0", "uid": uid, "type": cot_type, "time": time_str, "start": time_str, "stale": stale_str, "how": "m-g"},
    )

    ET.SubElement(
        event,
        "point",
        {
            "lat": str(detection.get("detection_lat", 0)),
            "lon": str(detection.get("detection_lon", 0)),
            "hae": str(detection.get("detection_alt_m", 0)),
            "ce": str(detection.get("position_accuracy_m", 50)),
            "le": "50.0",
        },
    )

    detail = ET.SubElement(event, "detail")
    ET.SubElement(detail, "contact", {"callsign": f"UAS-{detection['event_id'][-8:]}"})

    conf_pct = int(detection.get("detection_confidence", 0) * 100)
    ET.SubElement(detail, "remarks").text = (
        f"UAS DETECTION CONF:{conf_pct}% "
        f"SENSOR:{detection.get('sensor_type', 'N/A')} "
        f"CLASS:{detection.get('uas_classification', 'N/A')}"
    )

    return ET.tostring(event, encoding="unicode", xml_declaration=True)


class TakServerPublisher:
    """Publish CoT messages to TAK Server via SSL/TCP."""

    def __init__(self):
        self.sock = None

    def connect(self):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        try:
            ctx.load_cert_chain(TAK_SERVER_CERT, TAK_SERVER_KEY)
        except (FileNotFoundError, ssl.SSLError):
            logger.warning("TAK Server certs not found, using insecure connection")
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock = ctx.wrap_socket(raw_sock, server_hostname=TAK_SERVER_HOST)
        self.sock.connect((TAK_SERVER_HOST, TAK_SERVER_PORT))
        logger.info("Connected to TAK Server %s:%d", TAK_SERVER_HOST, TAK_SERVER_PORT)

    def send_cot(self, cot_xml: str):
        if not self.sock:
            self.connect()
        self.sock.sendall(cot_xml.encode("utf-8") + b"\n")

    def consume_and_publish(self):
        consumer = KafkaConsumer(
            "mda.alerts.composite",
            "mda.ais.gaps.detected",
            "mda.uas.detections.raw",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="tak-cot-publisher",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        logger.info("CoT publisher started, consuming alerts")

        for msg in consumer:
            try:
                event = msg.value
                if msg.topic == "mda.uas.detections.raw":
                    cot = build_uas_cot(event)
                else:
                    cot = build_vessel_cot(event)
                self.send_cot(cot)
            except Exception as e:
                logger.error("Error publishing CoT: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    publisher = TakServerPublisher()
    publisher.consume_and_publish()
