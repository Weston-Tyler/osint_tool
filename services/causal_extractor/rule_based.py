"""Rule-based causal relation extractor using spaCy dependency parsing.

Bilingual EN/ES extraction engine.  Consumes raw articles from Kafka
topic ``mda.articles.raw``, identifies causal connectives via phrase
matchers and dependency-tree heuristics, handles passive voice and
negation, and emits ``RawCausalTriplet`` objects to
``mda.causal.extractions.raw``.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import spacy
from spacy.language import Language
from spacy.matcher import PhraseMatcher
from spacy.tokens import Doc, Span, Token
from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger("mda.causal.rule_based")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUME_TOPIC = os.getenv("CAUSAL_CONSUME_TOPIC", "mda.articles.raw")
PRODUCE_TOPIC = os.getenv("CAUSAL_PRODUCE_TOPIC", "mda.causal.extractions.raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "mda.dlq")
CONSUMER_GROUP = os.getenv("CAUSAL_CONSUMER_GROUP", "causal-rule-extractor")

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class RawCausalTriplet:
    """Single extracted causal relation."""
    triplet_id: str = field(default_factory=lambda: str(uuid4()))
    cause_text: str = ""
    effect_text: str = ""
    relation_marker: str = ""
    relation_type: str = "CAUSES"
    confidence: float = 0.0
    confidence_tier: str = "low"
    negated: bool = False
    passive_voice: bool = False
    language: str = "en"
    sentence: str = ""
    doc_id: str = ""
    source: str = ""
    extraction_method: str = "rule_based_v1"
    extracted_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Causal connective dictionaries — organised by confidence tier
# ---------------------------------------------------------------------------

# English connectives
EN_CAUSAL_CONNECTIVES: dict[str, list[str]] = {
    "high": [
        "caused by",
        "was caused by",
        "were caused by",
        "resulted in",
        "resulting in",
        "led to",
        "leading to",
        "gave rise to",
        "brought about",
        "triggered",
        "precipitated",
        "induced",
        "produced",
        "generated",
        "provoked",
        "sparked",
        "engendered",
        "necessitated",
        "entailed",
    ],
    "medium": [
        "contributed to",
        "contributing to",
        "because of",
        "due to",
        "owing to",
        "on account of",
        "as a result of",
        "as a consequence of",
        "in response to",
        "stemmed from",
        "stems from",
        "arose from",
        "arises from",
        "originated from",
        "originates from",
        "attributable to",
        "attributed to",
        "linked to",
        "connected to",
        "associated with",
        "in the wake of",
        "in the aftermath of",
        "following",
        "after",
        "subsequently",
        "consequently",
        "therefore",
        "thus",
        "hence",
        "accordingly",
        "for this reason",
    ],
    "low": [
        "may have caused",
        "may have led to",
        "could have triggered",
        "might have resulted in",
        "possibly caused",
        "potentially led to",
        "appears to have caused",
        "seems to have caused",
        "is believed to have caused",
        "is thought to have triggered",
        "is suspected of causing",
        "coincided with",
        "correlated with",
        "occurred alongside",
        "preceded",
        "came before",
        "was followed by",
        "followed by",
    ],
}

# Spanish connectives
ES_CAUSAL_CONNECTIVES: dict[str, list[str]] = {
    "high": [
        "causado por",
        "causada por",
        "causados por",
        "causadas por",
        "provocado por",
        "provocada por",
        "provocados por",
        "provocadas por",
        "generado por",
        "generada por",
        "resultado de",
        "resultante de",
        "condujo a",
        "llevó a",
        "desencadenó",
        "desencadenado por",
        "originó",
        "originado por",
        "produjo",
        "producido por",
        "dio lugar a",
        "dando lugar a",
        "trajo consigo",
        "derivó en",
        "derivado de",
        "suscitó",
    ],
    "medium": [
        "debido a",
        "a causa de",
        "a raíz de",
        "como resultado de",
        "como consecuencia de",
        "en respuesta a",
        "a consecuencia de",
        "por culpa de",
        "por motivo de",
        "por razón de",
        "en razón de",
        "contribuyó a",
        "contribuyendo a",
        "atribuido a",
        "atribuible a",
        "vinculado a",
        "vinculada a",
        "asociado con",
        "asociada con",
        "relacionado con",
        "relacionada con",
        "tras",
        "después de",
        "por lo tanto",
        "por consiguiente",
        "en consecuencia",
        "por ende",
        "de ahí que",
    ],
    "low": [
        "podría haber causado",
        "podría haber provocado",
        "posiblemente causó",
        "aparentemente causado por",
        "se cree que causó",
        "se sospecha que causó",
        "coincidió con",
        "correlacionado con",
        "precedió a",
        "seguido de",
        "seguido por",
    ],
}

# Confidence scores per tier
TIER_CONFIDENCE: dict[str, float] = {
    "high": 0.85,
    "medium": 0.60,
    "low": 0.35,
}

# ---------------------------------------------------------------------------
# Passive voice regex patterns
# ---------------------------------------------------------------------------

EN_PASSIVE_RE = re.compile(
    r"\b(?:was|were|is|are|has been|have been|had been|being)\s+"
    r"(?:\w+\s+){0,3}"
    r"(?:caused|triggered|provoked|produced|generated|induced|sparked|"
    r"precipitated|brought about|led|attributed)\b",
    re.IGNORECASE,
)

ES_PASSIVE_RE = re.compile(
    r"\b(?:fue|fueron|es|son|ha sido|han sido|había sido|siendo)\s+"
    r"(?:\w+\s+){0,3}"
    r"(?:causado|causada|provocado|provocada|generado|generada|"
    r"producido|producida|desencadenado|desencadenada|originado|originada)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Negation cues
# ---------------------------------------------------------------------------

EN_NEGATION_CUES = {
    "not", "no", "never", "neither", "nor", "without", "hardly",
    "barely", "scarcely", "unlikely", "cannot", "didn't", "doesn't",
    "don't", "wasn't", "weren't", "hasn't", "haven't", "hadn't",
    "won't", "wouldn't", "shouldn't", "couldn't", "isn't", "aren't",
}

ES_NEGATION_CUES = {
    "no", "nunca", "jamás", "tampoco", "ni", "sin", "apenas",
    "difícilmente", "improbable",
}

# ---------------------------------------------------------------------------
# spaCy model loading
# ---------------------------------------------------------------------------


def _load_model(lang: str) -> Language:
    """Load spaCy model for the given language code."""
    model_map = {"en": "en_core_web_sm", "es": "es_core_news_sm"}
    name = model_map.get(lang, "en_core_web_sm")
    try:
        return spacy.load(name)
    except OSError:
        logger.info("Downloading spaCy model %s ...", name)
        spacy.cli.download(name)
        return spacy.load(name)


def _build_phrase_matcher(
    nlp: Language,
    connectives: dict[str, list[str]],
) -> tuple[PhraseMatcher, dict[str, str]]:
    """Build a PhraseMatcher from connective dict and return a
    label-to-tier lookup keyed by match_id string."""
    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    label_tier: dict[str, str] = {}

    for tier, phrases in connectives.items():
        for phrase in phrases:
            safe_label = f"CAUSAL_{tier.upper()}_{phrase.replace(' ', '_').upper()}"
            patterns = [nlp.make_doc(phrase)]
            matcher.add(safe_label, patterns)
            label_tier[safe_label] = tier

    return matcher, label_tier


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------


def _detect_negation(sent: Span, match_start: int, match_end: int, lang: str) -> bool:
    """Return True if the causal marker is negated in the sentence."""
    cues = EN_NEGATION_CUES if lang == "en" else ES_NEGATION_CUES
    # Check tokens in a 4-token window before the match
    window_start = max(sent.start, match_start - 4)
    for tok in sent.doc[window_start:match_start]:
        if tok.text.lower() in cues or tok.dep_ == "neg":
            return True
    return False


def _detect_passive(sent_text: str, lang: str) -> bool:
    """Return True if the sentence uses passive voice around a causal verb."""
    pattern = EN_PASSIVE_RE if lang == "en" else ES_PASSIVE_RE
    return bool(pattern.search(sent_text))


def _extract_clause(sent: Span, boundary: int, direction: str) -> str:
    """Extract a text span from a sentence, either 'left' or 'right' of
    the causal marker boundary token index.

    Uses dependency subtree heuristics when possible, otherwise falls
    back to simple token-window extraction.
    """
    tokens: list[Token] = []

    if direction == "left":
        # Collect tokens from sentence start to marker start
        for tok in sent:
            if tok.i >= boundary:
                break
            tokens.append(tok)
    else:
        # Collect tokens from marker end to sentence end
        for tok in sent:
            if tok.i >= boundary:
                tokens.append(tok)

    if not tokens:
        return ""

    text = sent.doc[tokens[0].i: tokens[-1].i + 1].text.strip()
    # Strip leading/trailing punctuation and whitespace
    text = text.strip(" ,;:—–-\t\n")
    return text


def _extract_from_sentence(
    sent: Span,
    match_start: int,
    match_end: int,
    marker_text: str,
    tier: str,
    lang: str,
) -> Optional[RawCausalTriplet]:
    """Given a sentence and a causal marker match, extract cause/effect."""
    negated = _detect_negation(sent, match_start, match_end, lang)
    passive = _detect_passive(sent.text, lang)

    left = _extract_clause(sent, match_start, "left")
    right = _extract_clause(sent, match_end, "right")

    if not left or not right:
        return None

    # Determine directionality based on marker semantics.
    # "caused by", "due to", "because of", "a causa de", etc. point
    # backwards: effect <marker> cause.
    backward_markers = {
        "caused by", "was caused by", "were caused by",
        "due to", "owing to", "because of", "on account of",
        "as a result of", "as a consequence of", "stemmed from",
        "stems from", "arose from", "arises from", "originated from",
        "originates from", "attributable to", "attributed to",
        "in response to", "in the wake of", "in the aftermath of",
        # Spanish backward markers
        "causado por", "causada por", "causados por", "causadas por",
        "provocado por", "provocada por", "provocados por", "provocadas por",
        "generado por", "generada por", "resultado de", "resultante de",
        "desencadenado por", "originado por", "producido por", "derivado de",
        "debido a", "a causa de", "a raíz de", "como resultado de",
        "como consecuencia de", "en respuesta a", "a consecuencia de",
        "por culpa de", "por motivo de", "por razón de", "en razón de",
        "atribuido a", "atribuible a",
    }

    marker_lower = marker_text.lower()
    if marker_lower in backward_markers:
        cause_text = right
        effect_text = left
    else:
        cause_text = left
        effect_text = right

    # Short clauses are likely parsing artefacts
    if len(cause_text.split()) < 2 or len(effect_text.split()) < 2:
        return None

    return RawCausalTriplet(
        cause_text=cause_text,
        effect_text=effect_text,
        relation_marker=marker_text,
        relation_type="CAUSES",
        confidence=TIER_CONFIDENCE[tier],
        confidence_tier=tier,
        negated=negated,
        passive_voice=passive,
        language=lang,
        sentence=sent.text.strip(),
    )


# ---------------------------------------------------------------------------
# Dependency-based fallback extractor
# ---------------------------------------------------------------------------

_DEP_CAUSAL_VERBS_EN = {
    "cause", "trigger", "lead", "produce", "generate", "induce",
    "spark", "precipitate", "provoke", "result", "create", "drive",
    "force", "compel", "prompt", "yield", "bring",
}

_DEP_CAUSAL_VERBS_ES = {
    "causar", "provocar", "generar", "producir", "desencadenar",
    "originar", "derivar", "conducir", "llevar", "resultar",
    "suscitar", "motivar", "impulsar",
}


def _dep_extract(sent: Span, lang: str) -> list[RawCausalTriplet]:
    """Use dependency parsing to find causal verbs and their subject/object."""
    causal_verbs = _DEP_CAUSAL_VERBS_EN if lang == "en" else _DEP_CAUSAL_VERBS_ES
    triplets: list[RawCausalTriplet] = []

    for tok in sent:
        if tok.lemma_.lower() not in causal_verbs:
            continue
        if tok.pos_ != "VERB":
            continue

        subj_tokens: list[Token] = []
        obj_tokens: list[Token] = []

        for child in tok.children:
            if child.dep_ in ("nsubj", "nsubjpass", "csubj"):
                subj_tokens = list(child.subtree)
            elif child.dep_ in ("dobj", "obj", "attr", "oprd", "xcomp", "ccomp"):
                obj_tokens = list(child.subtree)
            elif child.dep_ == "prep":
                # "led to X"
                for grandchild in child.children:
                    if grandchild.dep_ == "pobj":
                        obj_tokens = list(grandchild.subtree)

        if not subj_tokens or not obj_tokens:
            continue

        subj_text = sent.doc[
            subj_tokens[0].i: subj_tokens[-1].i + 1
        ].text.strip()
        obj_text = sent.doc[
            obj_tokens[0].i: obj_tokens[-1].i + 1
        ].text.strip()

        if len(subj_text.split()) < 2 or len(obj_text.split()) < 2:
            continue

        negated = any(
            c.dep_ == "neg" for c in tok.children
        )

        # Passive: subject is actually the effect
        is_passive = tok.tag_ in ("VBN",) and any(
            c.dep_ == "auxpass" for c in tok.children
        )

        if is_passive:
            cause_text, effect_text = obj_text, subj_text
        else:
            cause_text, effect_text = subj_text, obj_text

        triplets.append(RawCausalTriplet(
            cause_text=cause_text,
            effect_text=effect_text,
            relation_marker=tok.text,
            relation_type="CAUSES",
            confidence=0.55,
            confidence_tier="medium",
            negated=negated,
            passive_voice=is_passive,
            language=lang,
            sentence=sent.text.strip(),
        ))

    return triplets


# ---------------------------------------------------------------------------
# Main extractor class
# ---------------------------------------------------------------------------


class RuleBasedCausalExtractor:
    """Bilingual EN/ES rule-based causal relation extractor."""

    def __init__(self) -> None:
        logger.info("Loading spaCy models ...")
        self.nlp_en = _load_model("en")
        self.nlp_es = _load_model("es")

        self.matcher_en, self.tier_map_en = _build_phrase_matcher(
            self.nlp_en, EN_CAUSAL_CONNECTIVES,
        )
        self.matcher_es, self.tier_map_es = _build_phrase_matcher(
            self.nlp_es, ES_CAUSAL_CONNECTIVES,
        )

        logger.info(
            "Loaded %d EN connective patterns, %d ES connective patterns",
            sum(len(v) for v in EN_CAUSAL_CONNECTIVES.values()),
            sum(len(v) for v in ES_CAUSAL_CONNECTIVES.values()),
        )

    # ---- language detection (simple heuristic) ----------------------------

    @staticmethod
    def detect_language(text: str) -> str:
        """Very lightweight language detection — defaults to English."""
        es_indicators = {
            "el", "la", "los", "las", "del", "por", "una", "uno",
            "como", "que", "fue", "con", "para", "según", "pero",
            "este", "esta", "estos", "estas", "más",
        }
        words = text.lower().split()[:100]
        es_count = sum(1 for w in words if w in es_indicators)
        return "es" if es_count > len(words) * 0.15 else "en"

    # ---- single-document extraction ---------------------------------------

    def extract(
        self, text: str, doc_id: str = "", source: str = "", lang: str | None = None,
    ) -> list[RawCausalTriplet]:
        """Extract causal triplets from a single text."""
        if not text or len(text.strip()) < 30:
            return []

        if lang is None:
            lang = self.detect_language(text)

        nlp = self.nlp_en if lang == "en" else self.nlp_es
        matcher = self.matcher_en if lang == "en" else self.matcher_es
        tier_map = self.tier_map_en if lang == "en" else self.tier_map_es

        # Limit input size
        doc: Doc = nlp(text[:50000])

        triplets: list[RawCausalTriplet] = []
        seen_sentences: set[int] = set()

        # --- Phase 1: Phrase-matcher based extraction ----------------------
        matches = matcher(doc)
        for match_id, start, end in matches:
            label = nlp.vocab.strings[match_id]
            tier = tier_map.get(label, "low")
            marker_text = doc[start:end].text

            # Find enclosing sentence
            sent: Span | None = None
            for s in doc.sents:
                if s.start <= start and end <= s.end:
                    sent = s
                    break
            if sent is None:
                continue

            triplet = _extract_from_sentence(
                sent, start, end, marker_text, tier, lang,
            )
            if triplet is not None:
                triplet.doc_id = doc_id
                triplet.source = source
                triplets.append(triplet)
                seen_sentences.add(sent.start)

        # --- Phase 2: Dependency-parse fallback for uncovered sentences ----
        for sent in doc.sents:
            if sent.start in seen_sentences:
                continue
            dep_triplets = _dep_extract(sent, lang)
            for t in dep_triplets:
                t.doc_id = doc_id
                t.source = source
                triplets.append(t)

        return triplets


# ---------------------------------------------------------------------------
# Kafka consumer / producer loop
# ---------------------------------------------------------------------------


class CausalExtractionPipeline:
    """Kafka-connected pipeline that consumes articles, extracts causal
    relations, and produces extraction results."""

    def __init__(self) -> None:
        self.extractor = RuleBasedCausalExtractor()

        self.consumer = KafkaConsumer(
            CONSUME_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            max_poll_records=50,
            enable_auto_commit=True,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
        )

        logger.info("CausalExtractionPipeline initialised — consuming %s", CONSUME_TOPIC)

    def _extract_text(self, event: dict) -> str:
        """Pull processable text from incoming message."""
        for key in ("full_text", "body", "text", "content", "summary", "title"):
            val = event.get(key)
            if val and isinstance(val, str) and len(val.strip()) > 20:
                return val
        return ""

    def run(self) -> None:
        """Main processing loop."""
        logger.info("Rule-based causal extraction pipeline started")

        for msg in self.consumer:
            event = msg.value
            text = self._extract_text(event)
            if not text:
                continue

            doc_id = event.get("event_id") or event.get("article_id") or event.get("doc_id", "unknown")
            source = event.get("source", "unknown")
            lang = event.get("language")

            try:
                triplets = self.extractor.extract(text, doc_id=doc_id, source=source, lang=lang)

                for triplet in triplets:
                    self.producer.send(PRODUCE_TOPIC, asdict(triplet))

                if triplets:
                    logger.info(
                        "Extracted %d causal triplets from %s/%s",
                        len(triplets), source, doc_id,
                    )

            except Exception as exc:
                logger.error("Extraction error on %s: %s", doc_id, exc, exc_info=True)
                self.producer.send(DLQ_TOPIC, {
                    "source": "causal_rule_extractor",
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
    pipeline = CausalExtractionPipeline()
    pipeline.run()
