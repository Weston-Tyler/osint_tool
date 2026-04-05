"""GDELT BigQuery connector for historical bulk analysis.

Provides structured access to the full GDELT dataset in Google BigQuery
for large-scale analytical queries, Granger causality time-series
construction, and conflict escalation indicator extraction.

Tables:
    - ``gdelt-bq.gdeltv2.events`` -- GDELT 2.0 Event table
    - ``gdelt-bq.gdeltv2.gkg`` -- Global Knowledge Graph
    - ``gdelt-bq.gdeltv2.eventmentions`` -- Event mentions with tone/source

Requires:
    - ``GBQ_PROJECT_ID`` environment variable
    - Google Cloud credentials (ADC or ``GOOGLE_APPLICATION_CREDENTIALS``)
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery
import pyarrow  # noqa: F401  -- required by bigquery for arrow-based results

logger = logging.getLogger("mda.worker.gdelt.bigquery")

GBQ_PROJECT_ID = os.getenv("GBQ_PROJECT_ID")

# GDELT BigQuery table references
TABLE_EVENTS = "gdelt-bq.gdeltv2.events"
TABLE_GKG = "gdelt-bq.gdeltv2.gkg"
TABLE_EVENTMENTIONS = "gdelt-bq.gdeltv2.eventmentions"

# CAMEO verbal conflict codes (01-09 are verbal cooperation/conflict)
# and material conflict codes (10-20)
CAMEO_VERBAL_CONFLICT = ["10", "11", "12", "13", "14"]
CAMEO_MATERIAL_CONFLICT = ["15", "16", "17", "18", "19", "20"]

# MDA countries of interest
MDA_COUNTRIES = {
    "US", "MX", "CO", "VE", "EC", "PA", "GT", "HN", "SV", "NI", "CR",
    "CU", "JM", "PE", "BO", "BR", "BZ", "DO", "HT", "TT",
}

# Humanitarian monitoring countries for food security
FOOD_SECURITY_COUNTRIES = {
    "YE", "SO", "SS", "SD", "ET", "AF", "CD", "HT", "NG", "ML",
    "BF", "NE", "TD", "MG", "MZ", "ZW", "SY", "MM",
}

# Pre-built MDA query templates
MDA_QUERIES: dict[str, str] = {
    "cartel_activity": f"""
        SELECT
            GlobalEventID,
            SQLDATE,
            Actor1Name,
            Actor1CountryCode,
            Actor2Name,
            Actor2CountryCode,
            EventCode,
            EventBaseCode,
            EventRootCode,
            QuadClass,
            GoldsteinScale,
            NumMentions,
            NumSources,
            NumArticles,
            AvgTone,
            ActionGeo_FullName,
            ActionGeo_CountryCode,
            ActionGeo_Lat,
            ActionGeo_Long,
            SOURCEURL
        FROM `{TABLE_EVENTS}`
        WHERE ActionGeo_CountryCode IN ('MX', 'CO', 'GT', 'HN', 'SV', 'PA', 'NI', 'CR', 'BZ')
          AND EventRootCode IN ('17', '18', '19', '20')
          AND SQLDATE >= @start_date
          AND SQLDATE <= @end_date
        ORDER BY SQLDATE DESC
    """,
    "maritime_jiatf_south": f"""
        SELECT
            GlobalEventID,
            SQLDATE,
            Actor1Name,
            Actor1CountryCode,
            Actor2Name,
            Actor2CountryCode,
            EventCode,
            GoldsteinScale,
            AvgTone,
            ActionGeo_FullName,
            ActionGeo_Lat,
            ActionGeo_Long,
            SOURCEURL
        FROM `{TABLE_EVENTS}`
        WHERE ActionGeo_Lat BETWEEN 0 AND 25
          AND ActionGeo_Long BETWEEN -105 AND -60
          AND EventRootCode IN ('17', '18', '19', '20')
          AND SQLDATE >= @start_date
          AND SQLDATE <= @end_date
        ORDER BY SQLDATE DESC
    """,
    "sanctions_global": f"""
        SELECT
            GlobalEventID,
            SQLDATE,
            Actor1Name,
            Actor1CountryCode,
            Actor2Name,
            Actor2CountryCode,
            EventCode,
            EventBaseCode,
            GoldsteinScale,
            AvgTone,
            NumMentions,
            ActionGeo_FullName,
            ActionGeo_CountryCode,
            SOURCEURL
        FROM `{TABLE_EVENTS}`
        WHERE EventBaseCode IN ('163', '164', '165', '166', '172', '173')
          AND SQLDATE >= @start_date
          AND SQLDATE <= @end_date
        ORDER BY SQLDATE DESC
    """,
    "food_security": f"""
        SELECT
            DATE,
            SourceCommonName,
            DocumentIdentifier,
            V2Themes,
            V2Locations,
            V2Tone,
            V2Persons,
            V2Organizations
        FROM `{TABLE_GKG}`
        WHERE (V2Themes LIKE '%FOOD_SECURITY%' OR V2Themes LIKE '%TAX_FAMINE%')
          AND DATE >= @start_date
          AND DATE <= @end_date
        ORDER BY DATE DESC
        LIMIT 10000
    """,
}


class GDELTBigQueryConnector:
    """Connector to GDELT data in Google BigQuery for historical analysis."""

    def __init__(self, project_id: str | None = None) -> None:
        """Initialize the BigQuery connector.

        Args:
            project_id: Google Cloud project ID. Falls back to
                ``GBQ_PROJECT_ID`` environment variable.

        Raises:
            ValueError: If no project ID is provided or available in env.
        """
        self._project_id = project_id or GBQ_PROJECT_ID
        if not self._project_id:
            raise ValueError(
                "BigQuery project ID required. Set GBQ_PROJECT_ID env var "
                "or pass project_id parameter."
            )
        self._client = bigquery.Client(project=self._project_id)
        logger.info(
            "BigQuery connector initialized for project: %s", self._project_id
        )

    # ------------------------------------------------------------------
    # Raw query execution
    # ------------------------------------------------------------------

    def query_events(
        self, sql: str, params: list[bigquery.ScalarQueryParameter] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a raw SQL query against GDELT BigQuery tables.

        Args:
            sql: SQL query string. Can reference GDELT tables directly.
            params: Optional list of BigQuery query parameters for
                parameterized queries.

        Returns:
            List of row dicts.
        """
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params

        logger.info("Executing BigQuery query (%d chars)", len(sql))
        query_job = self._client.query(sql, job_config=job_config)
        rows = query_job.result()

        results: list[dict[str, Any]] = []
        for row in rows:
            results.append(dict(row))

        logger.info("BigQuery query returned %d rows", len(results))
        return results

    # ------------------------------------------------------------------
    # Filtered event queries
    # ------------------------------------------------------------------

    def query_events_by_region(
        self,
        country_codes: list[str],
        cameo_codes: list[str],
        start_date: str,
        end_date: str,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        """Query GDELT events filtered by country and CAMEO codes.

        Args:
            country_codes: List of ISO country codes to filter on
                (matched against ActionGeo_CountryCode).
            cameo_codes: List of CAMEO EventRootCode values to filter.
            start_date: Start date in ``YYYYMMDD`` format.
            end_date: End date in ``YYYYMMDD`` format.
            limit: Maximum rows to return.

        Returns:
            List of event dicts.
        """
        countries_str = ", ".join(f"'{c}'" for c in country_codes)
        cameo_str = ", ".join(f"'{c}'" for c in cameo_codes)

        sql = f"""
            SELECT
                GlobalEventID,
                SQLDATE,
                Actor1Name,
                Actor1CountryCode,
                Actor2Name,
                Actor2CountryCode,
                EventCode,
                EventBaseCode,
                EventRootCode,
                QuadClass,
                GoldsteinScale,
                NumMentions,
                NumSources,
                NumArticles,
                AvgTone,
                ActionGeo_FullName,
                ActionGeo_CountryCode,
                ActionGeo_Lat,
                ActionGeo_Long,
                SOURCEURL
            FROM `{TABLE_EVENTS}`
            WHERE ActionGeo_CountryCode IN ({countries_str})
              AND EventRootCode IN ({cameo_str})
              AND SQLDATE >= @start_date
              AND SQLDATE <= @end_date
            ORDER BY SQLDATE DESC
            LIMIT {limit}
        """

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        return self.query_events(sql, params=params)

    def query_gkg_themes(
        self,
        themes: list[str],
        start_date: str,
        end_date: str,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        """Query GKG records filtered by theme codes.

        Args:
            themes: List of GKG theme codes to search for (uses LIKE
                matching so partial matches work).
            start_date: Start date in ``YYYYMMDD`` format.
            end_date: End date in ``YYYYMMDD`` format.
            limit: Maximum rows to return.

        Returns:
            List of GKG record dicts.
        """
        theme_conditions = " OR ".join(
            f"V2Themes LIKE '%{theme}%'" for theme in themes
        )

        sql = f"""
            SELECT
                GKGRECORDID,
                DATE,
                SourceCommonName,
                DocumentIdentifier,
                V2Themes,
                V2Locations,
                V2Tone,
                V2Persons,
                V2Organizations,
                AllNames,
                Amounts
            FROM `{TABLE_GKG}`
            WHERE ({theme_conditions})
              AND DATE >= @start_date
              AND DATE <= @end_date
            ORDER BY DATE DESC
            LIMIT {limit}
        """

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        return self.query_events(sql, params=params)

    # ------------------------------------------------------------------
    # Granger causality time-series builder
    # ------------------------------------------------------------------

    def build_granger_time_series(
        self,
        event_type_a: str,
        event_type_b: str,
        country: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """Build paired daily time series for Granger causality testing.

        Constructs two daily event-count series directly from BigQuery,
        avoiding local DB dependency. These series can be fed to
        ``statsmodels.tsa.stattools.grangercausalitytests``.

        Args:
            event_type_a: CAMEO EventRootCode for the potential cause
                (e.g. ``"14"`` for protests).
            event_type_b: CAMEO EventRootCode for the potential effect
                (e.g. ``"19"`` for fight).
            country: ISO country code for ActionGeo_CountryCode.
            start_date: Start date ``YYYYMMDD``.
            end_date: End date ``YYYYMMDD``.

        Returns:
            Dict with keys ``"series_a"`` and ``"series_b"``, each a list
            of ``{"date": str, "count": int}`` dicts sorted by date.
        """
        sql = f"""
            WITH daily_a AS (
                SELECT
                    SQLDATE AS date,
                    COUNT(*) AS event_count
                FROM `{TABLE_EVENTS}`
                WHERE EventRootCode = @event_type_a
                  AND ActionGeo_CountryCode = @country
                  AND SQLDATE >= @start_date
                  AND SQLDATE <= @end_date
                GROUP BY SQLDATE
            ),
            daily_b AS (
                SELECT
                    SQLDATE AS date,
                    COUNT(*) AS event_count
                FROM `{TABLE_EVENTS}`
                WHERE EventRootCode = @event_type_b
                  AND ActionGeo_CountryCode = @country
                  AND SQLDATE >= @start_date
                  AND SQLDATE <= @end_date
                GROUP BY SQLDATE
            ),
            all_dates AS (
                SELECT DISTINCT date FROM daily_a
                UNION DISTINCT
                SELECT DISTINCT date FROM daily_b
            )
            SELECT
                d.date,
                COALESCE(a.event_count, 0) AS count_a,
                COALESCE(b.event_count, 0) AS count_b
            FROM all_dates d
            LEFT JOIN daily_a a ON d.date = a.date
            LEFT JOIN daily_b b ON d.date = b.date
            ORDER BY d.date ASC
        """

        params = [
            bigquery.ScalarQueryParameter("event_type_a", "STRING", event_type_a),
            bigquery.ScalarQueryParameter("event_type_b", "STRING", event_type_b),
            bigquery.ScalarQueryParameter("country", "STRING", country),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        rows = self.query_events(sql, params=params)

        series_a: list[dict[str, Any]] = []
        series_b: list[dict[str, Any]] = []
        for row in rows:
            series_a.append({"date": row["date"], "count": row["count_a"]})
            series_b.append({"date": row["date"], "count": row["count_b"]})

        logger.info(
            "Built Granger time series: %d days, event_a=%s, event_b=%s, "
            "country=%s",
            len(rows),
            event_type_a,
            event_type_b,
            country,
        )

        return {"series_a": series_a, "series_b": series_b}

    # ------------------------------------------------------------------
    # Aggregate queries
    # ------------------------------------------------------------------

    def get_event_volume_by_country(
        self, start_date: str, end_date: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Get aggregate event counts per country for a date range.

        Args:
            start_date: Start date ``YYYYMMDD``.
            end_date: End date ``YYYYMMDD``.
            limit: Maximum countries to return (ordered by event count desc).

        Returns:
            List of ``{"country": str, "event_count": int, "avg_tone": float}``
            dicts.
        """
        sql = f"""
            SELECT
                ActionGeo_CountryCode AS country,
                COUNT(*) AS event_count,
                AVG(AvgTone) AS avg_tone,
                AVG(GoldsteinScale) AS avg_goldstein,
                SUM(NumMentions) AS total_mentions
            FROM `{TABLE_EVENTS}`
            WHERE SQLDATE >= @start_date
              AND SQLDATE <= @end_date
              AND ActionGeo_CountryCode IS NOT NULL
              AND ActionGeo_CountryCode != ''
            GROUP BY ActionGeo_CountryCode
            ORDER BY event_count DESC
            LIMIT {limit}
        """

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        return self.query_events(sql, params=params)

    def get_conflict_escalation_indicators(
        self, country: str, lookback_days: int = 90
    ) -> dict[str, Any]:
        """Query for CAMEO escalation sequences (verbal -> material conflict).

        Analyzes whether verbal conflict events are escalating into material
        conflict events over the lookback period, a key indicator for
        instability forecasting.

        Args:
            country: ISO country code.
            lookback_days: Number of days to look back from today.

        Returns:
            Dict containing:
            - ``daily_series``: daily verbal/material conflict counts
            - ``verbal_total``: total verbal conflict events
            - ``material_total``: total material conflict events
            - ``escalation_ratio``: material / (verbal + 1)
            - ``trend_direction``: "escalating", "stable", or "de-escalating"
        """
        sql = f"""
            WITH daily_counts AS (
                SELECT
                    SQLDATE AS date,
                    COUNTIF(EventRootCode IN ('10', '11', '12', '13', '14'))
                        AS verbal_conflict,
                    COUNTIF(EventRootCode IN ('15', '16', '17', '18', '19', '20'))
                        AS material_conflict
                FROM `{TABLE_EVENTS}`
                WHERE ActionGeo_CountryCode = @country
                  AND EventRootCode IN (
                      '10', '11', '12', '13', '14',
                      '15', '16', '17', '18', '19', '20'
                  )
                  AND SQLDATE >= FORMAT_DATE(
                      '%Y%m%d',
                      DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
                  )
                GROUP BY SQLDATE
                ORDER BY SQLDATE ASC
            )
            SELECT
                date,
                verbal_conflict,
                material_conflict
            FROM daily_counts
            ORDER BY date ASC
        """

        params = [
            bigquery.ScalarQueryParameter("country", "STRING", country),
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
        ]

        rows = self.query_events(sql, params=params)

        verbal_total = sum(r.get("verbal_conflict", 0) for r in rows)
        material_total = sum(r.get("material_conflict", 0) for r in rows)
        escalation_ratio = material_total / (verbal_total + 1)

        # Determine trend: compare first-half vs second-half material ratio
        if len(rows) >= 4:
            midpoint = len(rows) // 2
            first_half = rows[:midpoint]
            second_half = rows[midpoint:]

            first_material = sum(r.get("material_conflict", 0) for r in first_half)
            first_verbal = sum(r.get("verbal_conflict", 0) for r in first_half)
            second_material = sum(r.get("material_conflict", 0) for r in second_half)
            second_verbal = sum(r.get("verbal_conflict", 0) for r in second_half)

            first_ratio = first_material / (first_verbal + 1)
            second_ratio = second_material / (second_verbal + 1)

            if second_ratio > first_ratio * 1.25:
                trend = "escalating"
            elif second_ratio < first_ratio * 0.75:
                trend = "de-escalating"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        result = {
            "country": country,
            "lookback_days": lookback_days,
            "daily_series": rows,
            "verbal_total": verbal_total,
            "material_total": material_total,
            "escalation_ratio": round(escalation_ratio, 4),
            "trend_direction": trend,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Conflict escalation for %s: verbal=%d, material=%d, "
            "ratio=%.3f, trend=%s",
            country,
            verbal_total,
            material_total,
            escalation_ratio,
            trend,
        )

        return result

    # ------------------------------------------------------------------
    # Pre-built MDA queries
    # ------------------------------------------------------------------

    def run_mda_query(
        self, query_name: str, start_date: str, end_date: str
    ) -> list[dict[str, Any]]:
        """Execute a pre-built MDA query template.

        Args:
            query_name: One of ``cartel_activity``, ``maritime_jiatf_south``,
                ``sanctions_global``, ``food_security``.
            start_date: Start date ``YYYYMMDD``.
            end_date: End date ``YYYYMMDD``.

        Returns:
            List of row dicts.

        Raises:
            ValueError: If the query name is not recognized.
        """
        sql = MDA_QUERIES.get(query_name)
        if sql is None:
            available = ", ".join(MDA_QUERIES.keys())
            raise ValueError(
                f"Unknown MDA query '{query_name}'. Available: {available}"
            )

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        logger.info("Running MDA query: %s (%s to %s)", query_name, start_date, end_date)
        return self.query_events(sql, params=params)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_to_parquet(self, query: str, output_path: str) -> str:
        """Export BigQuery query results to a local Parquet file.

        The Parquet file can then be uploaded to MinIO or other object
        storage for downstream processing.

        Args:
            query: SQL query to execute.
            output_path: Local file path for the output Parquet file.

        Returns:
            The output file path.
        """
        logger.info("Exporting query results to Parquet: %s", output_path)

        query_job = self._client.query(query)
        arrow_table = query_job.result().to_arrow()

        import pyarrow.parquet as pq

        pq.write_table(arrow_table, output_path)

        file_size = os.path.getsize(output_path)
        logger.info(
            "Exported %d rows to %s (%.2f MB)",
            arrow_table.num_rows,
            output_path,
            file_size / (1024 * 1024),
        )

        return output_path

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the BigQuery client."""
        self._client.close()
        logger.info("BigQuery connector closed")


def main() -> None:
    """CLI convenience -- run a sample MDA query."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    connector = GDELTBigQueryConnector()
    try:
        # Example: cartel activity in the last 30 days
        # Dates would normally be computed dynamically
        from datetime import timedelta

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=30)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        logger.info("Running sample MDA queries for %s to %s", start_str, end_str)

        # Run pre-built queries
        for query_name in MDA_QUERIES:
            try:
                results = connector.run_mda_query(query_name, start_str, end_str)
                logger.info("MDA query [%s]: %d rows", query_name, len(results))
            except Exception:
                logger.exception("Error running MDA query: %s", query_name)

        # Run conflict escalation for Mexico
        try:
            escalation = connector.get_conflict_escalation_indicators("MX", 90)
            logger.info(
                "Mexico conflict escalation: trend=%s, ratio=%.3f",
                escalation["trend_direction"],
                escalation["escalation_ratio"],
            )
        except Exception:
            logger.exception("Error running conflict escalation query")

        # Run event volume by country
        try:
            volumes = connector.get_event_volume_by_country(start_str, end_str)
            for vol in volumes[:10]:
                logger.info(
                    "  %s: %d events, avg_tone=%.2f",
                    vol["country"],
                    vol["event_count"],
                    vol.get("avg_tone", 0),
                )
        except Exception:
            logger.exception("Error running event volume query")

    finally:
        connector.close()


if __name__ == "__main__":
    main()
