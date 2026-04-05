"""Smoke tests for all data source ingesters.

Validates that each ingester:
1. Imports without error
2. Has the expected classes/functions
3. Normalizers produce valid output shapes
4. API URL constants are reachable (HEAD request)

Run: python -m pytest tests/smoke/test_all_ingesters.py -v
"""

import importlib
import re
from datetime import datetime, timezone

import pytest
import requests


# ── Helper: check URL reachability ───────────────────────────

def _url_reachable(url: str, timeout: int = 10) -> bool:
    """HEAD request to check if a URL is reachable."""
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        return resp.status_code < 500
    except Exception:
        return False


# ============================================================
# 1. AIS Workers
# ============================================================

class TestAISWorkers:
    def test_noaa_imports(self):
        mod = importlib.import_module("workers.ais.noaa_ais_ingester")
        assert hasattr(mod, "ingest_ais_geoparquet")
        assert hasattr(mod, "ingest_all")

    def test_gfw_imports(self):
        mod = importlib.import_module("workers.ais.gfw_ingester")
        assert hasattr(mod, "fetch_vessel_events")
        assert hasattr(mod, "normalize_gfw_encounter")

    def test_aishub_imports(self):
        mod = importlib.import_module("workers.ais.aishub_ingester")
        assert hasattr(mod, "normalize_aishub_record")
        assert hasattr(mod, "run_polling_loop")

    def test_aishub_normalization(self):
        from workers.ais.aishub_ingester import normalize_aishub_record
        raw = {"MMSI": "123456789", "LATITUDE": 5.0, "LONGITUDE": -76.0,
               "SOG": 120, "COG": 1800, "HEADING": 245, "NAVSTAT": 0,
               "NAME": "TEST VESSEL", "TYPE": 70, "IMO": "1234567", "TIME": "2026-01-01"}
        result = normalize_aishub_record(raw)
        assert result["mmsi"] == "123456789"
        assert result["speed_kts"] == 12.0  # SOG/10
        assert result["source"] == "aishub"


# ============================================================
# 2. Sanctions Workers
# ============================================================

class TestSanctionsWorkers:
    def test_ofac_imports(self):
        mod = importlib.import_module("workers.sanctions.ofac_sdn_ingester")
        assert hasattr(mod, "parse_ofac_vessel")
        assert hasattr(mod, "parse_ofac_individual")

    def test_opensanctions_imports(self):
        mod = importlib.import_module("workers.sanctions.opensanctions_ingester")
        assert hasattr(mod, "ftm_to_vessel")
        assert hasattr(mod, "ftm_to_person")

    def test_ofac_url_reachable(self):
        assert _url_reachable("https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.json")


# ============================================================
# 3. GDELT Workers
# ============================================================

class TestGDELTWorkers:
    def test_event_ingester_imports(self):
        mod = importlib.import_module("workers.gdelt.gdelt_ingester")
        assert hasattr(mod, "normalize_gdelt_event")
        assert hasattr(mod, "is_mda_relevant")

    def test_gkg_ingester_imports(self):
        mod = importlib.import_module("workers.gdelt.gkg_ingester")
        assert callable(getattr(mod, "ingest_latest", None)) or hasattr(mod, "GKGIngester")

    def test_doc_api_imports(self):
        mod = importlib.import_module("workers.gdelt.doc_api")
        assert hasattr(mod, "GDELTDocAPI") or callable(getattr(mod, "search_articles", None))

    def test_geo_api_imports(self):
        mod = importlib.import_module("workers.gdelt.geo_api")
        assert hasattr(mod, "GDELTGeoAPI") or callable(getattr(mod, "search_point_data", None))

    def test_tv_api_imports(self):
        mod = importlib.import_module("workers.gdelt.tv_api")
        assert hasattr(mod, "GDELTTvAPI") or callable(getattr(mod, "search_clips", None))

    def test_gdelt_master_url_reachable(self):
        assert _url_reachable("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt")


# ============================================================
# 4. UAS Workers
# ============================================================

class TestUASWorkers:
    def test_faa_imports(self):
        mod = importlib.import_module("workers.uas.faa_uas_ingester")
        assert hasattr(mod, "classify_faa_uas")
        assert hasattr(mod, "normalize_faa_sighting")

    def test_remote_id_imports(self):
        mod = importlib.import_module("workers.uas.remote_id_receiver")
        assert hasattr(mod, "RemoteIDParser")

    def test_faa_classification(self):
        from workers.uas.faa_uas_ingester import classify_faa_uas
        assert classify_faa_uas("DJI Phantom 4") == "SMALL_COMMERCIAL"
        assert classify_faa_uas("fixed wing craft") == "FIXED_WING"
        assert classify_faa_uas("") == "UNKNOWN"


# ============================================================
# 5. ACLED Workers
# ============================================================

class TestACLEDWorkers:
    def test_acled_imports(self):
        mod = importlib.import_module("workers.acled.acled_ingester")
        assert hasattr(mod, "ACLEDIngester") or callable(getattr(mod, "fetch_events", None))


# ============================================================
# 6. Economic Workers
# ============================================================

class TestEconomicWorkers:
    def test_fred_imports(self):
        mod = importlib.import_module("workers.economic.fred_ingester")
        assert hasattr(mod, "FREDIngester") or callable(getattr(mod, "fetch_series", None))

    def test_worldbank_imports(self):
        mod = importlib.import_module("workers.economic.worldbank_ingester")
        assert hasattr(mod, "WorldBankIngester") or callable(getattr(mod, "fetch_indicator", None))


# ============================================================
# 7. Financial Workers
# ============================================================

class TestFinancialWorkers:
    def test_icij_imports(self):
        mod = importlib.import_module("workers.financial.icij_ingester")
        assert hasattr(mod, "ICIJIngester") or callable(getattr(mod, "load_entities", None))

    def test_sanctions_aggregator_imports(self):
        mod = importlib.import_module("workers.financial.sanctions_aggregator")
        assert hasattr(mod, "SanctionsAggregator") or callable(getattr(mod, "fetch_eu_sanctions", None))


# ============================================================
# 8. Humanitarian Workers
# ============================================================

class TestHumanitarianWorkers:
    def test_emdat_imports(self):
        mod = importlib.import_module("workers.humanitarian.emdat_ingester")
        assert callable(getattr(mod, "normalize_disaster"))

    def test_emdat_normalization(self):
        from workers.humanitarian.emdat_ingester import normalize_disaster
        raw = {"Dis No": "2024-001", "Disaster Type": "Flood", "ISO": "MX",
               "Country": "Mexico", "Start Year": "2024", "Start Month": "6",
               "Total Deaths": "50", "Total Affected": "100000",
               "Total Damage ('000 US$)": "5000"}
        result = normalize_disaster(raw)
        assert result is not None
        assert result["event_type"] == "NATURAL_DISASTER_FLOOD"
        assert result["metadata"]["deaths"] == 50

    def test_iom_dtm_imports(self):
        mod = importlib.import_module("workers.humanitarian.iom_dtm_ingester")
        assert callable(getattr(mod, "normalize_dtm_record"))

    def test_reliefweb_url_reachable(self):
        assert _url_reachable("https://api.reliefweb.int/v1/reports?limit=1")


# ============================================================
# 9. Geospatial Workers
# ============================================================

class TestGeospatialWorkers:
    def test_nasa_firms_imports(self):
        mod = importlib.import_module("workers.geospatial.nasa_firms_ingester")
        assert callable(getattr(mod, "fetch_fires"))

    def test_adsb_imports(self):
        mod = importlib.import_module("workers.geospatial.adsb_ingester")
        assert hasattr(mod, "ADSBIngester") or callable(getattr(mod, "fetch_aircraft_near_point", None))


# ============================================================
# 10. Crypto Workers
# ============================================================

class TestCryptoWorkers:
    def test_coingecko_imports(self):
        mod = importlib.import_module("workers.crypto.coingecko_ingester")
        assert callable(getattr(mod, "fetch_prices"))
        assert callable(getattr(mod, "detect_anomaly"))

    def test_coingecko_anomaly_detection(self):
        from workers.crypto.coingecko_ingester import detect_anomaly
        # Create synthetic price series with a spike
        prices = [{"token_id": "bitcoin", "ticker": "BTC",
                    "price_usd": 50000 + i * 10, "timestamp": f"2026-01-{i+1:02d}"}
                   for i in range(40)]
        # Inject spike
        prices[35]["price_usd"] = 80000
        anomalies = detect_anomaly(prices)
        assert len(anomalies) >= 1
        assert any(a["direction"] == "spike" for a in anomalies)

    def test_coingecko_api_reachable(self):
        assert _url_reachable("https://api.coingecko.com/api/v3/ping")


# ============================================================
# 11. Maritime Workers
# ============================================================

class TestMaritimeWorkers:
    def test_maritime_ingester_imports(self):
        mod = importlib.import_module("workers.maritime.maritime_ingester")
        assert callable(getattr(mod, "ingest_world_port_index"))
        assert callable(getattr(mod, "fetch_comtrade_flows"))

    def test_comtrade_api_reachable(self):
        assert _url_reachable("https://comtradeapi.un.org/public/v1/preview/C/A/HS")


# ============================================================
# 12. Causal Extraction Engine
# ============================================================

class TestCausalExtraction:
    def test_rule_based_imports(self):
        mod = importlib.import_module("services.causal_extractor.rule_based")
        assert hasattr(mod, "RuleBasedCausalExtractor") or hasattr(mod, "CAUSAL_PATTERNS_EN")

    def test_llm_extractor_imports(self):
        mod = importlib.import_module("services.causal_extractor.llm_extractor")
        assert hasattr(mod, "LLMCausalExtractor") or hasattr(mod, "CausalTripletLLMOutput")

    def test_granger_imports(self):
        mod = importlib.import_module("services.causal_extractor.granger_analyzer")
        assert hasattr(mod, "GrangerCausalityAnalyzer") or callable(getattr(mod, "run_granger_test", None))


# ============================================================
# 13. WorldFish
# ============================================================

class TestWorldFish:
    def test_seed_extractor_imports(self):
        mod = importlib.import_module("worldfish.seed_extractor")
        assert hasattr(mod, "SimulationSeed") or hasattr(mod, "OBISeedExtractor")

    def test_persona_generator_imports(self):
        mod = importlib.import_module("worldfish.persona_generator")
        assert hasattr(mod, "AgentPersona") or hasattr(mod, "OBIAgentPersonaGenerator")

    def test_environments_imports(self):
        mod = importlib.import_module("worldfish.environments")
        assert hasattr(mod, "ActionType")
        assert hasattr(mod, "WorldState")
        assert hasattr(mod, "MaritimeDomainEnvironment")

    def test_prediction_imports(self):
        mod = importlib.import_module("worldfish.prediction")
        assert hasattr(mod, "CausalPrediction")

    def test_prediction_cot_xml(self):
        from worldfish.prediction import CausalPrediction
        pred = CausalPrediction(
            predicted_event_type="PORT_CLOSURE",
            predicted_event_description="Predicted port closure",
            predicted_location_lat=19.05,
            predicted_location_lon=-104.32,
            predicted_location_region="Manzanillo, Mexico",
            confidence=0.75,
            domain="maritime",
            trigger_event_id="test_trigger_001",
        )
        xml = pred.to_atak_cot_xml()
        assert "<?xml" in xml
        assert "WF-" in xml
        assert "maritime" in xml.lower() or "a-h-G" in xml

    def test_action_types(self):
        from worldfish.environments import ActionType
        assert ActionType.MARITIME_TRANSIT.value == "maritime_transit"
        assert ActionType.ATTACK.value == "attack"
        assert len(ActionType) >= 15

    def test_world_state_defaults(self):
        from worldfish.environments import WorldState
        ws = WorldState()
        assert ws.step == 0
        assert 0 <= ws.violence_level <= 1
        assert 0 <= ws.enforcement_pressure <= 1


# ============================================================
# 14. Market Causal
# ============================================================

class TestMarketCausal:
    def test_tracker_imports(self):
        mod = importlib.import_module("services.market_causal.tracker")
        assert hasattr(mod, "MarketCausalTracker") or hasattr(mod, "TRACKED_INSTRUMENTS")

    def test_early_warning_imports(self):
        mod = importlib.import_module("services.market_causal.early_warning")
        assert hasattr(mod, "MarketEarlyWarningSystem") or hasattr(mod, "MarketPressureScore")

    def test_butterfly_imports(self):
        mod = importlib.import_module("services.market_causal.butterfly")
        assert hasattr(mod, "ButterflyEffectEngine") or hasattr(mod, "BUTTERFLY_QUERIES")


# ============================================================
# 15. Services
# ============================================================

class TestServices:
    def test_nlp_pipeline_imports(self):
        mod = importlib.import_module("services.nlp.ner_pipeline")
        assert hasattr(mod, "extract_regex_entities")

    def test_causal_graph_learner_imports(self):
        mod = importlib.import_module("services.causal_graph_learner.learner")
        assert hasattr(mod, "CausalGraphLearner")

    def test_intel_products_imports(self):
        mod = importlib.import_module("services.intel_products.causal_chain_report")
        assert hasattr(mod, "CausalChainReport")

    def test_cot_publisher_imports(self):
        mod = importlib.import_module("services.cot_publisher.cot_publisher")  # note: cot-publisher dir
        assert hasattr(mod, "build_vessel_cot")

    def test_stix_exporter_imports(self):
        mod = importlib.import_module("services.stix_export.stix_exporter")  # note: stix-export dir
        assert hasattr(mod, "vessel_to_stix_indicator")
