"""NLP/NER pipeline for maritime and drug trafficking domain.

Consumes raw text events from Kafka (GDELT, intelligence reports, press releases)
and extracts named entities using spaCy with custom patterns for the MDA domain.

Entity types extracted:
- VESSEL_NAME — vessel names, IMO numbers, MMSI
- PERSON / PER — person names, aliases
- ORG / ORGANIZATION — cartel names, agencies, companies
- GPE / LOC — geographic locations, ports, maritime zones
- DRUG_TYPE — cocaine, heroin, fentanyl, methamphetamine, etc.
- QUANTITY — drug quantities, monetary values
- OPERATION_NAME — law enforcement operation codenames
"""

import json
import logging
import os
import re
from datetime import datetime

import spacy
from spacy.language import Language
from spacy.matcher import Matcher
from spacy.tokens import Span
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.nlp.ner")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MODEL_PATH = os.getenv("MODEL_PATH", "en_core_web_trf")

# Domain-specific patterns for the Matcher
DRUG_PATTERNS = [
    "cocaine", "heroin", "fentanyl", "methamphetamine", "meth",
    "marijuana", "cannabis", "MDMA", "ecstasy", "ketamine",
    "oxycodone", "opioid", "precursor chemicals", "coca paste",
    "crack cocaine", "crystal meth", "synthetic opioid",
]

VESSEL_TYPE_PATTERNS = [
    "go-fast boat", "go fast boat", "panga", "semi-submersible",
    "narco submarine", "narco-submarine", "fishing vessel",
    "bulk carrier", "container ship", "tanker", "cargo vessel",
    "speedboat", "motor vessel", "sailing vessel",
]

AGENCY_PATTERNS = [
    "USCG", "Coast Guard", "DEA", "CBP", "JIATF-South", "JIATF South",
    "Border Patrol", "USBP", "ICE", "HSI", "FBI", "ATF", "DOJ",
    "Navy", "USN", "Marines", "USMC", "SOUTHCOM",
    "Interpol", "Europol", "COLNAV", "Mexican Navy",
]

CARTEL_PATTERNS = [
    "Sinaloa Cartel", "CDS", "CJNG", "Jalisco New Generation",
    "Gulf Cartel", "Los Zetas", "Beltran Leyva", "Beltran-Leyva",
    "Arellano Felix", "Tijuana Cartel", "Juarez Cartel",
    "La Familia Michoacana", "Knights Templar", "Clan del Golfo",
    "FARC", "ELN", "Autodefensas",
]

# Regex patterns for structured identifiers
IMO_PATTERN = re.compile(r"\bIMO\s*(\d{7})\b", re.IGNORECASE)
MMSI_PATTERN = re.compile(r"\bMMSI\s*(\d{9})\b", re.IGNORECASE)
OPERATION_PATTERN = re.compile(r"\bOperation\s+([A-Z][A-Z\s]{2,30})\b")
QUANTITY_PATTERN = re.compile(
    r"(\d[\d,.]+)\s*(kg|kilogram|kilos|pound|lb|ton|metric ton|mt|"
    r"kilo(?:gram)?s?)\b",
    re.IGNORECASE,
)
MONEY_PATTERN = re.compile(r"\$\s*([\d,.]+)\s*(million|billion|thousand|M|B|K)?", re.IGNORECASE)


def load_nlp_model(model_path: str = MODEL_PATH) -> Language:
    """Load spaCy model, falling back to small model if transformer unavailable."""
    try:
        nlp = spacy.load(model_path)
        logger.info("Loaded spaCy model: %s", model_path)
    except OSError:
        logger.warning("Model %s not found, falling back to en_core_web_sm", model_path)
        try:
            nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.info("Downloading en_core_web_sm...")
            spacy.cli.download("en_core_web_sm")
            nlp = spacy.load("en_core_web_sm")

    # Add custom entity ruler for domain terms
    if "entity_ruler" not in nlp.pipe_names:
        ruler = nlp.add_pipe("entity_ruler", before="ner")
        patterns = []

        for drug in DRUG_PATTERNS:
            patterns.append({"label": "DRUG_TYPE", "pattern": drug})
            patterns.append({"label": "DRUG_TYPE", "pattern": drug.title()})
            patterns.append({"label": "DRUG_TYPE", "pattern": drug.upper()})

        for agency in AGENCY_PATTERNS:
            patterns.append({"label": "ORG", "pattern": agency})

        for cartel in CARTEL_PATTERNS:
            patterns.append({"label": "ORG", "pattern": cartel})

        for vtype in VESSEL_TYPE_PATTERNS:
            patterns.append({"label": "VESSEL_TYPE", "pattern": vtype})

        ruler.add_patterns(patterns)

    return nlp


def extract_regex_entities(text: str) -> list[dict]:
    """Extract structured identifiers via regex (IMO, MMSI, operations, quantities)."""
    entities = []

    for m in IMO_PATTERN.finditer(text):
        entities.append({
            "text": m.group(0),
            "label": "IMO_NUMBER",
            "start": m.start(),
            "end": m.end(),
            "value": m.group(1),
        })

    for m in MMSI_PATTERN.finditer(text):
        entities.append({
            "text": m.group(0),
            "label": "MMSI_NUMBER",
            "start": m.start(),
            "end": m.end(),
            "value": m.group(1),
        })

    for m in OPERATION_PATTERN.finditer(text):
        entities.append({
            "text": m.group(0),
            "label": "OPERATION_NAME",
            "start": m.start(),
            "end": m.end(),
            "value": m.group(1).strip(),
        })

    for m in QUANTITY_PATTERN.finditer(text):
        entities.append({
            "text": m.group(0),
            "label": "DRUG_QUANTITY",
            "start": m.start(),
            "end": m.end(),
            "value": m.group(1),
            "unit": m.group(2).lower(),
        })

    for m in MONEY_PATTERN.finditer(text):
        entities.append({
            "text": m.group(0),
            "label": "MONETARY_VALUE",
            "start": m.start(),
            "end": m.end(),
            "value": m.group(1),
            "multiplier": m.group(2),
        })

    return entities


def extract_entities(nlp: Language, text: str, doc_id: str, source: str) -> dict:
    """Extract named entities from text using spaCy + regex."""
    # Limit text length for processing
    text = text[:50000]
    doc = nlp(text)

    entities = {
        "vessel_names": [],
        "persons": [],
        "organizations": [],
        "locations": [],
        "drug_types": [],
        "quantities": [],
        "operation_names": [],
        "imo_numbers": [],
        "mmsi_numbers": [],
    }

    # spaCy NER entities
    for ent in doc.ents:
        record = {
            "text": ent.text,
            "label": ent.label_,
            "start": ent.start_char,
            "end": ent.end_char,
        }

        if ent.label_ == "VESSEL_NAME":
            entities["vessel_names"].append(record)
        elif ent.label_ in ("PERSON", "PER"):
            entities["persons"].append(record)
        elif ent.label_ in ("ORG", "ORGANIZATION"):
            entities["organizations"].append(record)
        elif ent.label_ in ("GPE", "LOC", "FAC"):
            entities["locations"].append(record)
        elif ent.label_ == "DRUG_TYPE":
            entities["drug_types"].append(record)
        elif ent.label_ in ("QUANTITY", "CARDINAL", "MONEY"):
            entities["quantities"].append(record)
        elif ent.label_ == "OPERATION_NAME":
            entities["operation_names"].append(record)
        elif ent.label_ == "VESSEL_TYPE":
            pass  # Not an entity we need to extract, just a descriptor

    # Regex entities (structured identifiers)
    regex_entities = extract_regex_entities(text)
    for re_ent in regex_entities:
        label = re_ent["label"]
        if label == "IMO_NUMBER":
            entities["imo_numbers"].append(re_ent)
        elif label == "MMSI_NUMBER":
            entities["mmsi_numbers"].append(re_ent)
        elif label == "OPERATION_NAME":
            entities["operation_names"].append(re_ent)
        elif label in ("DRUG_QUANTITY", "MONETARY_VALUE"):
            entities["quantities"].append(re_ent)

    total = sum(len(v) for v in entities.values())
    return {
        "doc_id": doc_id,
        "source": source,
        "entities": entities,
        "entity_count": total,
        "processed_at": datetime.utcnow().isoformat(),
    }


class NERPipeline:
    """Kafka consumer-producer NER pipeline."""

    def __init__(self):
        self.nlp = load_nlp_model()
        self.consumer = KafkaConsumer(
            "mda.events.gdelt.raw",
            "mda.intelligence.reports",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="nlp-ner-pipeline",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            max_poll_records=50,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def run(self):
        """Main NER processing loop."""
        logger.info("NER pipeline started")

        for msg in self.consumer:
            event = msg.value
            text = self._extract_text(event)
            if not text or len(text.strip()) < 20:
                continue

            doc_id = event.get("event_id") or event.get("report_id", "unknown")
            source = event.get("source", "unknown")

            try:
                extraction = extract_entities(self.nlp, text, doc_id, source)

                if extraction["entity_count"] > 0:
                    self.producer.send("mda.ner.extractions", {
                        **extraction,
                        "original_topic": msg.topic,
                        "original_event_id": doc_id,
                    })
                    logger.debug("Extracted %d entities from %s/%s", extraction["entity_count"], source, doc_id)

            except Exception as e:
                logger.error("NER error on %s: %s", doc_id, e)
                self.producer.send("mda.dlq", {
                    "source": "nlp_ner",
                    "error": str(e),
                    "doc_id": doc_id,
                    "_original_topic": msg.topic,
                })

    def _extract_text(self, event: dict) -> str:
        """Extract processable text from different event types."""
        parts = []

        # GDELT events
        if event.get("source") == "gdelt":
            parts.append(event.get("action_geo_fullname", ""))
            parts.append(event.get("actor1_name", ""))
            parts.append(event.get("actor2_name", ""))

        # Intelligence reports
        if event.get("full_text"):
            parts.append(event["full_text"])
        elif event.get("summary"):
            parts.append(event["summary"])
        elif event.get("title"):
            parts.append(event["title"])

        # Press release URLs contain useful text in the URL path
        url = event.get("source_url", "")
        if url:
            # Extract readable parts from URL
            path = url.split("/")[-1].replace("-", " ").replace("_", " ")
            if len(path) > 10:
                parts.append(path)

        return " ".join(filter(None, parts))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    pipeline = NERPipeline()
    pipeline.run()
