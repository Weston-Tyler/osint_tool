"""LLM-based causal relation extractor using Ollama.

Chunks input text into ~512-word windows with 50-word overlap,
sends each chunk through a structured extraction prompt to a local
Ollama model (default: qwen2.5:7b), validates responses with Pydantic,
deduplicates via content hashing, and emits results to Kafka.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

import ollama
from pydantic import BaseModel, Field, field_validator
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.causal.llm_extractor")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUME_TOPIC = os.getenv("CAUSAL_CONSUME_TOPIC", "mda.articles.raw")
PRODUCE_TOPIC = os.getenv("CAUSAL_PRODUCE_TOPIC", "mda.causal.extractions.raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "mda.dlq")
CONSUMER_GROUP = os.getenv("CAUSAL_LLM_CONSUMER_GROUP", "causal-llm-extractor")

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")

CHUNK_SIZE_WORDS = int(os.getenv("CHUNK_SIZE_WORDS", "512"))
CHUNK_OVERLAP_WORDS = int(os.getenv("CHUNK_OVERLAP_WORDS", "50"))
MAX_WORKERS = int(os.getenv("LLM_MAX_WORKERS", "4"))
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))

# ---------------------------------------------------------------------------
# Pydantic models for validated LLM output
# ---------------------------------------------------------------------------


class CausalTripletLLMOutput(BaseModel):
    """Schema for a single causal triplet returned by the LLM."""

    cause_event: str = Field(
        ..., min_length=3, max_length=500,
        description="The cause event described in the text.",
    )
    effect_event: str = Field(
        ..., min_length=3, max_length=500,
        description="The effect event described in the text.",
    )
    relation_type: str = Field(
        default="CAUSES",
        description="Type of causal relation: CAUSES, ENABLES, PREVENTS, CONTRIBUTES_TO.",
    )
    relation_confidence: float = Field(
        default=0.5, ge=0.0, le=1.0,
        description="Model confidence in this extraction (0-1).",
    )
    lag_estimate_days: Optional[int] = Field(
        default=None, ge=0, le=3650,
        description="Estimated temporal lag between cause and effect in days.",
    )
    mechanism: Optional[str] = Field(
        default=None, max_length=300,
        description="Brief description of the causal mechanism.",
    )
    negated: bool = Field(
        default=False,
        description="Whether the causal relation is negated in the text.",
    )

    @field_validator("relation_type")
    @classmethod
    def normalise_relation_type(cls, v: str) -> str:
        allowed = {
            "CAUSES", "ENABLES", "PREVENTS", "CONTRIBUTES_TO",
            "TRIGGERS", "INHIBITS", "CORRELATES_WITH",
        }
        upper = v.upper().replace(" ", "_")
        return upper if upper in allowed else "CAUSES"

    @field_validator("relation_confidence", mode="before")
    @classmethod
    def clamp_confidence(cls, v: Any) -> float:
        try:
            v = float(v)
        except (TypeError, ValueError):
            return 0.5
        return max(0.0, min(1.0, v))


class CausalExtractionBatch(BaseModel):
    """Wrapper for a list of extracted triplets (what the LLM returns)."""
    triplets: list[CausalTripletLLMOutput] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Extraction prompt
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """You are an expert analyst for a Maritime Domain Awareness (MDA) intelligence system.
Your task is to extract ALL causal relationships from the following text.

For each causal relation you identify, return a JSON object with these fields:
- cause_event: string — the cause event (concise but complete)
- effect_event: string — the effect event (concise but complete)
- relation_type: one of CAUSES, ENABLES, PREVENTS, CONTRIBUTES_TO, TRIGGERS, INHIBITS, CORRELATES_WITH
- relation_confidence: float 0.0-1.0 — your confidence this is a real causal link
- lag_estimate_days: integer or null — estimated days between cause and effect
- mechanism: string or null — brief mechanism description
- negated: boolean — true if the text says the cause did NOT lead to the effect

Return ONLY a JSON array of objects. No explanation, no markdown fences. If no causal relations exist, return an empty array [].

Domain context: This text may discuss drug trafficking, maritime security, cartel operations, sanctions, vessel movements, humanitarian crises, armed conflicts, market disruptions, or geopolitical events in Latin America and the Caribbean.

TEXT:
{text}

JSON ARRAY:"""

# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------


def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE_WORDS,
    overlap: int = CHUNK_OVERLAP_WORDS,
) -> list[str]:
    """Split text into overlapping word-level chunks."""
    words = text.split()
    if len(words) <= chunk_size:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]
        chunks.append(" ".join(chunk_words))
        if end >= len(words):
            break
        start = end - overlap

    return chunks


# ---------------------------------------------------------------------------
# Content-hash deduplication
# ---------------------------------------------------------------------------


def _content_hash(cause: str, effect: str) -> str:
    """Generate a stable hash for a cause-effect pair for deduplication."""
    normalised = f"{cause.lower().strip()}||{effect.lower().strip()}"
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# LLM interaction
# ---------------------------------------------------------------------------


def _call_ollama(
    chunk: str,
    model: str = OLLAMA_MODEL,
    host: str = OLLAMA_HOST,
) -> list[CausalTripletLLMOutput]:
    """Send a single chunk to Ollama and parse the response."""
    client = ollama.Client(host=host)

    prompt = EXTRACTION_PROMPT.format(text=chunk)

    try:
        response = client.generate(
            model=model,
            prompt=prompt,
            options={
                "temperature": 0.1,
                "top_p": 0.9,
                "num_predict": 2048,
            },
        )
    except Exception as exc:
        logger.error("Ollama request failed: %s", exc)
        return []

    raw_text = response.get("response", "").strip()

    # Try to extract JSON from the response
    triplets = _parse_llm_json(raw_text)
    return triplets


def _parse_llm_json(raw: str) -> list[CausalTripletLLMOutput]:
    """Parse and validate JSON output from the LLM."""
    # Strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())

    # Try direct parse first
    parsed: list[dict] = []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Try to find a JSON array in the response
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(0))
            except json.JSONDecodeError:
                logger.warning("Could not parse LLM JSON output")
                return []

    if not isinstance(parsed, list):
        if isinstance(parsed, dict):
            parsed = [parsed]
        else:
            return []

    validated: list[CausalTripletLLMOutput] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        try:
            triplet = CausalTripletLLMOutput(**item)
            validated.append(triplet)
        except Exception as exc:
            logger.debug("Validation failed for LLM triplet: %s — %s", item, exc)

    return validated


# ---------------------------------------------------------------------------
# Main extractor class
# ---------------------------------------------------------------------------


@dataclass
class LLMExtractionResult:
    """Result of LLM extraction for a single document."""
    doc_id: str
    source: str
    triplets: list[dict] = field(default_factory=list)
    chunk_count: int = 0
    raw_triplet_count: int = 0
    deduped_triplet_count: int = 0
    extraction_method: str = "llm_ollama_v1"
    model: str = OLLAMA_MODEL
    extracted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class LLMCausalExtractor:
    """LLM-based causal extractor with parallel chunk processing."""

    def __init__(
        self,
        model: str = OLLAMA_MODEL,
        host: str = OLLAMA_HOST,
        max_workers: int = MAX_WORKERS,
        chunk_size: int = CHUNK_SIZE_WORDS,
        chunk_overlap: int = CHUNK_OVERLAP_WORDS,
    ) -> None:
        self.model = model
        self.host = host
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._seen_hashes: set[str] = set()
        self._hash_ttl_count = 0
        self._hash_max_size = 100_000

        logger.info(
            "LLMCausalExtractor initialised — model=%s host=%s workers=%d",
            model, host, max_workers,
        )

    def _flush_hash_cache_if_needed(self) -> None:
        """Prevent unbounded memory growth of dedup cache."""
        if len(self._seen_hashes) > self._hash_max_size:
            logger.info("Flushing content-hash dedup cache (%d entries)", len(self._seen_hashes))
            self._seen_hashes.clear()

    def extract(
        self, text: str, doc_id: str = "", source: str = "",
    ) -> LLMExtractionResult:
        """Extract causal relations from text via LLM with chunking and dedup."""
        self._flush_hash_cache_if_needed()

        chunks = chunk_text(text, self.chunk_size, self.chunk_overlap)
        result = LLMExtractionResult(
            doc_id=doc_id,
            source=source,
            chunk_count=len(chunks),
            model=self.model,
        )

        all_triplets: list[CausalTripletLLMOutput] = []

        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(_call_ollama, chunk, self.model, self.host): idx
                for idx, chunk in enumerate(chunks)
            }

            for future in as_completed(futures):
                chunk_idx = futures[future]
                try:
                    chunk_triplets = future.result()
                    all_triplets.extend(chunk_triplets)
                except Exception as exc:
                    logger.error("Chunk %d extraction failed: %s", chunk_idx, exc)

        result.raw_triplet_count = len(all_triplets)

        # Deduplicate by content hash
        unique_triplets: list[dict] = []
        for triplet in all_triplets:
            h = _content_hash(triplet.cause_event, triplet.effect_event)
            if h in self._seen_hashes:
                continue
            self._seen_hashes.add(h)

            triplet_dict = triplet.model_dump()
            triplet_dict["triplet_id"] = str(uuid4())
            triplet_dict["content_hash"] = h
            triplet_dict["doc_id"] = doc_id
            triplet_dict["source"] = source
            triplet_dict["extraction_method"] = "llm_ollama_v1"
            triplet_dict["model"] = self.model
            triplet_dict["extracted_at"] = datetime.now(timezone.utc).isoformat()
            unique_triplets.append(triplet_dict)

        result.triplets = unique_triplets
        result.deduped_triplet_count = len(unique_triplets)

        return result


# ---------------------------------------------------------------------------
# Kafka pipeline
# ---------------------------------------------------------------------------


class LLMCausalExtractionPipeline:
    """Kafka consumer/producer pipeline for LLM-based causal extraction."""

    def __init__(self) -> None:
        self.extractor = LLMCausalExtractor()

        self.consumer = KafkaConsumer(
            CONSUME_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            max_poll_records=10,
            enable_auto_commit=True,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
        )

        logger.info("LLMCausalExtractionPipeline initialised — consuming %s", CONSUME_TOPIC)

    def _extract_text(self, event: dict) -> str:
        for key in ("full_text", "body", "text", "content", "summary", "title"):
            val = event.get(key)
            if val and isinstance(val, str) and len(val.strip()) > 30:
                return val
        return ""

    def run(self) -> None:
        """Main processing loop."""
        logger.info("LLM causal extraction pipeline started (model=%s)", OLLAMA_MODEL)

        for msg in self.consumer:
            event = msg.value
            text = self._extract_text(event)
            if not text:
                continue

            doc_id = (
                event.get("event_id")
                or event.get("article_id")
                or event.get("doc_id", "unknown")
            )
            source = event.get("source", "unknown")

            try:
                result = self.extractor.extract(text, doc_id=doc_id, source=source)

                for triplet in result.triplets:
                    self.producer.send(PRODUCE_TOPIC, triplet)

                if result.deduped_triplet_count > 0:
                    logger.info(
                        "LLM extracted %d triplets (%d raw, %d chunks) from %s/%s",
                        result.deduped_triplet_count,
                        result.raw_triplet_count,
                        result.chunk_count,
                        source,
                        doc_id,
                    )

            except Exception as exc:
                logger.error("LLM extraction error on %s: %s", doc_id, exc, exc_info=True)
                self.producer.send(DLQ_TOPIC, {
                    "source": "causal_llm_extractor",
                    "error": str(exc),
                    "doc_id": doc_id,
                    "_original_topic": msg.topic,
                })


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    pipeline = LLMCausalExtractionPipeline()
    pipeline.run()
