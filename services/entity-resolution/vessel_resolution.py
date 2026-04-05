"""Vessel entity resolution using Splink probabilistic record linkage.

Resolves vessel identities across AIS data, OFAC SDN, and OpenSanctions
using IMO, MMSI, vessel name, flag state, and vessel type as matching fields.

Reference: https://github.com/moj-analytical-services/splink
"""

import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger("mda.entity_resolution.vessel")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://mda:mda@localhost:5432/mda")


def load_vessel_records_from_postgres() -> pd.DataFrame:
    """Load vessel records from all source staging tables."""
    import asyncpg
    import asyncio

    async def _fetch():
        conn = await asyncpg.connect(POSTGRES_DSN)

        # AIS-derived vessels
        ais_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (mmsi)
                concat('ais_', mmsi) AS unique_id,
                'ais' AS source,
                imo,
                mmsi,
                vessel_name AS name,
                NULL AS flag_state,
                vessel_type::text AS vessel_type,
                NULL::real AS gross_tonnage
            FROM ais_positions
            WHERE mmsi IS NOT NULL
            ORDER BY mmsi, valid_time DESC
            """
        )

        # OFAC staging
        ofac_rows = await conn.fetch(
            """
            SELECT
                concat('ofac_', entity_id) AS unique_id,
                'ofac' AS source,
                imo, mmsi, name,
                flag_state, vessel_type,
                NULL::real AS gross_tonnage
            FROM ofac_vessels_staging
            """
        )

        # OpenSanctions staging
        os_rows = await conn.fetch(
            """
            SELECT
                concat('os_', entity_id) AS unique_id,
                'opensanctions' AS source,
                imo, mmsi, name,
                flag_state, vessel_type,
                gross_tonnage
            FROM opensanctions_vessels_staging
            """
        )

        await conn.close()
        return ais_rows, ofac_rows, os_rows

    ais_rows, ofac_rows, os_rows = asyncio.run(_fetch())

    frames = []
    for rows, label in [(ais_rows, "ais"), (ofac_rows, "ofac"), (os_rows, "opensanctions")]:
        if rows:
            df = pd.DataFrame([dict(r) for r in rows])
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    all_vessels = pd.concat(frames, ignore_index=True)

    # Clean name for comparison
    all_vessels["name_cleaned"] = (
        all_vessels["name"]
        .fillna("")
        .str.upper()
        .str.replace(r"[^A-Z0-9 ]", "", regex=True)
        .str.strip()
    )

    return all_vessels


def run_vessel_resolution(min_confidence: float = 0.85, output_path: str | None = None) -> pd.DataFrame:
    """Run Splink vessel entity resolution.

    Returns DataFrame with cluster assignments.
    """
    try:
        import splink.duckdb.comparison_library as cl
        import splink.duckdb.blocking_rule_library as brl
        from splink.duckdb.linker import DuckDBLinker
    except ImportError:
        logger.error("Splink not installed. Run: pip install splink")
        raise

    all_vessels = load_vessel_records_from_postgres()
    if all_vessels.empty:
        logger.warning("No vessel records found for resolution")
        return pd.DataFrame()

    logger.info("Loaded %d vessel records for resolution", len(all_vessels))

    conn = duckdb.connect()

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "unique_id",
        "blocking_rules_to_generate_predictions": [
            brl.exact_match_rule("imo"),
            brl.exact_match_rule("mmsi"),
            brl.block_on("name_cleaned"),
        ],
        "comparisons": [
            cl.exact_match("imo", term_frequency_adjustments=False),
            cl.exact_match("mmsi", term_frequency_adjustments=False),
            cl.jaro_winkler_at_thresholds("name_cleaned", [0.95, 0.88, 0.80], term_frequency_adjustments=True),
            cl.exact_match("flag_state"),
            cl.exact_match("vessel_type"),
        ],
        "max_iterations": 25,
        "em_convergence": 0.0001,
    }

    linker = DuckDBLinker(all_vessels, settings, connection=conn)

    # Train u-probabilities
    linker.estimate_u_using_random_sampling(max_pairs=1_000_000)

    # Train m-probabilities using known matches (same IMO)
    linker.estimate_parameters_using_expectation_maximisation(
        brl.exact_match_rule("imo"), fix_u_probabilities=True
    )

    # Generate predictions
    df_predictions = linker.predict(threshold_match_probability=min_confidence)
    df_clusters = linker.cluster_pairwise_predictions_at_threshold(
        df_predictions, threshold_match_probability=min_confidence
    )

    df_result = df_clusters.as_pandas_dataframe()
    logger.info("Resolved %d vessel records into %d clusters", len(df_result), df_result["cluster_id"].nunique())

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df_result.to_parquet(output_path)
        logger.info("Saved resolution results to %s", output_path)

    return df_result


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    output = sys.argv[1] if len(sys.argv) > 1 else "/data/entity_clusters/vessels_latest.parquet"
    run_vessel_resolution(output_path=output)
