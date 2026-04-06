"""Cross-registry entity resolution for MDA Corporate Ownership Graph.

Resolves the same legal entity across multiple registries (GLEIF,
OpenCorporates, Companies House, ICIJ Offshore Leaks, SEC EDGAR) using
both deterministic key matching and probabilistic record linkage via
Splink.

Pipeline stages:
    1. Deterministic cross-reference via GLEIF <-> OpenCorporates mapping
    2. Probabilistic linkage with Splink (blocking + comparison)
    3. Human-in-the-loop review queue for uncertain matches
    4. Graph merge: SAME_AS edges in Memgraph
"""

from __future__ import annotations

import csv
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from neo4j import GraphDatabase

logger = logging.getLogger("mda.corporate.entity_resolver")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASS = os.getenv("MEMGRAPH_PASS", "")

GLEIF_OC_MAPPING_CSV = os.getenv(
    "GLEIF_OC_MAPPING_CSV",
    "/data/corporate/gleif_opencorporates_mapping.csv",
)

AUTO_MERGE_THRESHOLD = float(os.getenv("AUTO_MERGE_THRESHOLD", "0.92"))
REVIEW_THRESHOLD = float(os.getenv("REVIEW_THRESHOLD", "0.70"))

# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

_COMPANY_SUFFIXES = re.compile(
    r"\b("
    r"ltd|limited|llc|l\.l\.c|inc|incorporated|corp|corporation|"
    r"gmbh|gesellschaft\s+mit\s+beschr[aä]nkter\s+haftung|"
    r"ag|aktiengesellschaft|"
    r"sa|s\.a|sociedad\s+an[oó]nima|soci[eé]t[eé]\s+anonyme|"
    r"srl|s\.r\.l|"
    r"bv|b\.v|besloten\s+vennootschap|"
    r"nv|n\.v|naamloze\s+vennootschap|"
    r"plc|public\s+limited\s+company|"
    r"lp|limited\s+partnership|"
    r"llp|limited\s+liability\s+partnership|"
    r"se|societas\s+europaea|"
    r"pty|proprietary|"
    r"pvt|private|"
    r"co|company|"
    r"oy|oyj|ab|as|asa|"
    r"sarl|s\.a\.r\.l|"
    r"spa|s\.p\.a|"
    r"kk|kabushiki\s+kaisha|"
    r"pte|"
    r"holding|holdings|group"
    r")\.?\b",
    re.IGNORECASE,
)

_WHITESPACE = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9\s]")


def normalize_company_name(name: str) -> str:
    """Normalise a company name for comparison.

    Steps:
        1. Unicode NFKD normalisation and diacritic removal
        2. Lowercase
        3. Remove legal suffixes (Ltd, Inc, Corp, GmbH, SA, etc.)
        4. Remove punctuation
        5. Collapse whitespace and strip
    """
    if not name:
        return ""
    # Unicode normalisation
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_text = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    lowered = ascii_text.lower()
    # Remove suffixes
    cleaned = _COMPANY_SUFFIXES.sub("", lowered)
    # Remove remaining punctuation
    cleaned = _NON_ALNUM.sub(" ", cleaned)
    # Collapse whitespace
    cleaned = _WHITESPACE.sub(" ", cleaned).strip()
    return cleaned


# ---------------------------------------------------------------------------
# Deterministic cross-reference
# ---------------------------------------------------------------------------


@dataclass
class MappingRecord:
    """A single row from the GLEIF <-> OpenCorporates mapping CSV."""

    lei: str
    oc_url: str
    oc_jurisdiction: str
    oc_company_number: str
    gleif_name: str
    oc_name: str
    confidence: float = 1.0


class DeterministicCrossReference:
    """Apply the GLEIF <-> OpenCorporates mapping CSV to produce exact
    entity matches keyed on LEI and company number."""

    def __init__(self, mapping_csv: str | None = None):
        self._csv_path = mapping_csv or GLEIF_OC_MAPPING_CSV

    def load_mapping(self) -> pd.DataFrame:
        """Load and parse the mapping CSV.

        Expected columns: lei, oc_url, oc_jurisdiction, oc_company_number,
        gleif_legal_name, oc_name
        """
        path = Path(self._csv_path)
        if not path.exists():
            logger.warning("GLEIF-OC mapping CSV not found at %s", path)
            return pd.DataFrame()

        df = pd.read_csv(
            path,
            dtype=str,
            usecols=[
                "lei",
                "oc_url",
                "oc_jurisdiction",
                "oc_company_number",
                "gleif_legal_name",
                "oc_name",
            ],
        )
        df.columns = [
            "lei",
            "oc_url",
            "oc_jurisdiction",
            "oc_company_number",
            "gleif_name",
            "oc_name",
        ]
        df["gleif_name_norm"] = df["gleif_name"].apply(normalize_company_name)
        df["oc_name_norm"] = df["oc_name"].apply(normalize_company_name)
        df["confidence"] = 1.0
        logger.info("Loaded %d GLEIF-OC mappings", len(df))
        return df

    def apply(self, entities_df: pd.DataFrame) -> pd.DataFrame:
        """Join entity records against the mapping table.

        *entities_df* must have columns: ``id``, ``name``, ``lei``,
        ``registration_number``, ``jurisdiction``, ``source``.

        Returns a DataFrame of matched pairs with columns:
        ``id_left``, ``id_right``, ``match_key``, ``confidence``.
        """
        mapping = self.load_mapping()
        if mapping.empty or entities_df.empty:
            return pd.DataFrame(
                columns=["id_left", "id_right", "match_key", "confidence"]
            )

        # Split entities by source
        gleif = entities_df[entities_df["source"] == "gleif"].copy()
        oc = entities_df[entities_df["source"] == "opencorporates"].copy()

        matches: list[dict] = []

        # Join on LEI
        if not gleif.empty and "lei" in gleif.columns:
            lei_merge = gleif.merge(
                mapping[["lei", "oc_company_number", "oc_jurisdiction"]],
                on="lei",
                how="inner",
            )
            for _, row in lei_merge.iterrows():
                oc_match = oc[
                    (oc["registration_number"] == row["oc_company_number"])
                    & (oc["jurisdiction"] == row["oc_jurisdiction"])
                ]
                for _, oc_row in oc_match.iterrows():
                    matches.append(
                        {
                            "id_left": row["id"],
                            "id_right": oc_row["id"],
                            "match_key": f"LEI:{row['lei']}",
                            "confidence": 1.0,
                        }
                    )

        # Join on registration number + jurisdiction
        if not gleif.empty and not oc.empty:
            reg_merge = gleif.merge(
                oc,
                left_on=["registration_number", "jurisdiction"],
                right_on=["registration_number", "jurisdiction"],
                suffixes=("_gleif", "_oc"),
                how="inner",
            )
            for _, row in reg_merge.iterrows():
                pair = {
                    "id_left": row["id_gleif"],
                    "id_right": row["id_oc"],
                    "match_key": f"REG:{row['registration_number']}@{row['jurisdiction']}",
                    "confidence": 0.98,
                }
                matches.append(pair)

        result = pd.DataFrame(matches).drop_duplicates(
            subset=["id_left", "id_right"]
        )
        logger.info("Deterministic cross-reference produced %d matches", len(result))
        return result


# ---------------------------------------------------------------------------
# Probabilistic linkage with Splink
# ---------------------------------------------------------------------------


class SplinkCorporateLinker:
    """Probabilistic entity resolution using Splink with DuckDB backend.

    Blocking rules (5):
        1. Exact match on registration_number
        2. Exact match on LEI
        3. First 4 chars of normalised name + jurisdiction
        4. Exact match on postal_code + first 3 chars of name
        5. Exact match on jurisdiction + Soundex of normalised name

    Comparison functions (6):
        1. Jaro-Winkler on normalised name
        2. Exact match on registration_number
        3. Exact match on jurisdiction
        4. Exact match on LEI
        5. Exact match on postal_code
        6. Levenshtein on address
    """

    def __init__(
        self,
        auto_merge_threshold: float | None = None,
        review_threshold: float | None = None,
    ):
        self._auto_threshold = auto_merge_threshold or AUTO_MERGE_THRESHOLD
        self._review_threshold = review_threshold or REVIEW_THRESHOLD

    def _build_settings(self):
        """Build Splink settings using SettingsCreator."""
        import splink.comparison_library as cl
        from splink import SettingsCreator

        return SettingsCreator(
            link_type="link_and_dedupe",
            unique_id_column_name="id",
            blocking_rules_to_generate_predictions=[
                # Rule 1: Exact registration number
                "l.registration_number = r.registration_number",
                # Rule 2: Exact LEI
                "l.lei = r.lei",
                # Rule 3: First 4 chars of normalised name + jurisdiction
                (
                    "substr(l.name_norm, 1, 4) = substr(r.name_norm, 1, 4) "
                    "AND l.jurisdiction = r.jurisdiction"
                ),
                # Rule 4: Postal code + first 3 chars of name
                (
                    "l.postal_code = r.postal_code "
                    "AND substr(l.name_norm, 1, 3) = substr(r.name_norm, 1, 3)"
                ),
                # Rule 5: Jurisdiction + Soundex of name
                (
                    "l.jurisdiction = r.jurisdiction "
                    "AND soundex(l.name_norm) = soundex(r.name_norm)"
                ),
            ],
            comparisons=[
                # Comparison 1: Jaro-Winkler on normalised name
                cl.JaroWinklerAtThresholds(
                    "name_norm",
                    score_threshold_or_thresholds=[0.92, 0.85, 0.70],
                ),
                # Comparison 2: Exact match on registration number
                cl.ExactMatch("registration_number"),
                # Comparison 3: Exact match on jurisdiction
                cl.ExactMatch("jurisdiction"),
                # Comparison 4: Exact match on LEI
                cl.ExactMatch("lei"),
                # Comparison 5: Exact match on postal code
                cl.ExactMatch("postal_code"),
                # Comparison 6: Levenshtein on address
                cl.LevenshteinAtThresholds(
                    "address",
                    distance_threshold_or_thresholds=[1, 3, 5],
                ),
            ],
            retain_intermediate_calculation_columns=False,
            retain_matching_columns=True,
            max_iterations=20,
            em_convergence=0.0001,
        )

    def link(self, entities_df: pd.DataFrame) -> pd.DataFrame:
        """Run probabilistic linkage and return scored pairs.

        *entities_df* must include columns: ``id``, ``name``, ``name_norm``,
        ``registration_number``, ``jurisdiction``, ``lei``, ``postal_code``,
        ``address``, ``source``.

        Returns DataFrame with columns including ``id_l``, ``id_r``,
        ``match_weight``, ``match_probability``.
        """
        from splink import Linker

        settings = self._build_settings()
        linker = Linker(entities_df, settings, database_api="duckdb")

        # Estimate u-probabilities from random sampling
        linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

        # Estimate m-probabilities via EM on two blocking rules
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.registration_number = r.registration_number",
            fix_u_probabilities=True,
        )
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.name_norm = r.name_norm AND l.jurisdiction = r.jurisdiction",
            fix_u_probabilities=True,
        )

        # Predict
        predictions = linker.inference.predict(
            threshold_match_probability=self._review_threshold
        )
        pairs_df = predictions.as_pandas_dataframe()
        logger.info("Splink produced %d candidate pairs", len(pairs_df))
        return pairs_df

    def partition(
        self, pairs_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split pairs into auto-merge and human-review sets."""
        auto = pairs_df[
            pairs_df["match_probability"] >= self._auto_threshold
        ].copy()
        review = pairs_df[
            (pairs_df["match_probability"] >= self._review_threshold)
            & (pairs_df["match_probability"] < self._auto_threshold)
        ].copy()
        logger.info(
            "Partitioned: %d auto-merge, %d for review", len(auto), len(review)
        )
        return auto, review


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _prepare_entities(driver: Any) -> pd.DataFrame:
    """Pull company records from Memgraph for linkage."""
    query = """
    MATCH (c:Company)
    OPTIONAL MATCH (c)-[:REGISTERED_AT]->(a:Address)
    RETURN c.id AS id,
           c.name AS name,
           c.registration_number AS registration_number,
           c.jurisdiction AS jurisdiction,
           c.lei AS lei,
           a.postal_code AS postal_code,
           a.full_address AS address,
           c.source AS source
    """
    with driver.session() as session:
        records = session.run(query).data()

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Normalise names
    df["name_norm"] = df["name"].apply(normalize_company_name)

    # Fill nulls to avoid Splink errors
    for col in ["registration_number", "jurisdiction", "lei", "postal_code", "address"]:
        if col in df.columns:
            df[col] = df[col].fillna("")

    return df


def run_resolution(
    memgraph_uri: str | None = None,
    gleif_oc_csv: str | None = None,
    auto_merge_threshold: float | None = None,
    review_threshold: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full entity resolution pipeline.

    Returns:
        (auto_merge_df, review_df) -- DataFrames of high-confidence and
        uncertain matches respectively.
    """
    uri = memgraph_uri or MEMGRAPH_URI
    auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
    driver = GraphDatabase.driver(uri, auth=auth)

    try:
        entities_df = _prepare_entities(driver)
        if entities_df.empty:
            logger.warning("No entities found in graph for resolution")
            empty = pd.DataFrame()
            return empty, empty

        logger.info("Loaded %d entities for resolution", len(entities_df))

        # Stage 1: Deterministic cross-reference
        det = DeterministicCrossReference(mapping_csv=gleif_oc_csv)
        det_matches = det.apply(entities_df)

        # Stage 2: Probabilistic linkage
        linker = SplinkCorporateLinker(
            auto_merge_threshold=auto_merge_threshold,
            review_threshold=review_threshold,
        )
        prob_pairs = linker.link(entities_df)
        auto_prob, review_prob = linker.partition(prob_pairs)

        # Combine deterministic (always auto-merge) with probabilistic
        if not det_matches.empty:
            det_auto = det_matches.rename(
                columns={"id_left": "id_l", "id_right": "id_r"}
            )
            det_auto["match_probability"] = det_matches["confidence"]
            auto_merge_df = pd.concat(
                [auto_prob, det_auto], ignore_index=True
            ).drop_duplicates(subset=["id_l", "id_r"])
        else:
            auto_merge_df = auto_prob

        review_df = review_prob

        logger.info(
            "Resolution complete: %d auto-merge, %d review",
            len(auto_merge_df),
            len(review_df),
        )
        return auto_merge_df, review_df

    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Graph merge
# ---------------------------------------------------------------------------

_CREATE_SAME_AS_QUERY = """
UNWIND $pairs AS pair
MATCH (a:Company {id: pair.id_l})
MATCH (b:Company {id: pair.id_r})
MERGE (a)-[r:SAME_AS]-(b)
SET r.confidence = pair.confidence,
    r.match_key = pair.match_key,
    r.resolved_at = $resolved_at
RETURN count(r) AS edges_created
"""


def apply_merges_to_graph(
    auto_merge_df: pd.DataFrame,
    memgraph_uri: str | None = None,
    batch_size: int = 500,
) -> int:
    """Write SAME_AS edges to Memgraph for all auto-merged pairs.

    Parameters:
        auto_merge_df: DataFrame with at least ``id_l``, ``id_r``, and
            ``match_probability`` columns.
        memgraph_uri: Bolt URI override.
        batch_size: Number of pairs per UNWIND batch.

    Returns:
        Total number of SAME_AS edges created.
    """
    if auto_merge_df.empty:
        logger.info("No merges to apply")
        return 0

    uri = memgraph_uri or MEMGRAPH_URI
    auth = (MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_USER else None
    driver = GraphDatabase.driver(uri, auth=auth)

    resolved_at = pd.Timestamp.now(tz="UTC").isoformat()
    total_created = 0

    try:
        # Prepare pairs as list of dicts
        pairs = []
        for _, row in auto_merge_df.iterrows():
            pairs.append(
                {
                    "id_l": row.get("id_l", row.get("id_left", "")),
                    "id_r": row.get("id_r", row.get("id_right", "")),
                    "confidence": float(
                        row.get("match_probability", row.get("confidence", 0.0))
                    ),
                    "match_key": row.get("match_key", "splink"),
                }
            )

        # Batch UNWIND
        with driver.session() as session:
            for i in range(0, len(pairs), batch_size):
                batch = pairs[i : i + batch_size]
                result = session.run(
                    _CREATE_SAME_AS_QUERY,
                    pairs=batch,
                    resolved_at=resolved_at,
                ).single()
                created = result["edges_created"] if result else 0
                total_created += created
                logger.info(
                    "Batch %d-%d: created %d SAME_AS edges",
                    i,
                    i + len(batch),
                    created,
                )

    finally:
        driver.close()

    logger.info("Total SAME_AS edges created: %d", total_created)
    return total_created
