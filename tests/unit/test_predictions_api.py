"""Unit tests for the /v1/analytics/predictions endpoint and its query builder.

Exercises the router in isolation (no gqlalchemy / live Memgraph) via an injected
fake state.memgraph and a lightweight request stand-in.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

from api.routers.analytics import (
    build_predictions_query,
    decayed_confidence,
    enrich_with_decay,
    get_predictions,
)


class FakeMemgraph:
    def __init__(self, rows):
        self.rows = rows
        self.last = None

    def execute_and_fetch(self, cypher, params=None):
        self.last = (cypher, params)
        return list(self.rows)


def _request(mg):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(memgraph=mg)))


class TestQueryBuilder:
    def test_no_filters(self):
        cypher, params = build_predictions_query()
        assert "PredictedEvent" in cypher
        assert "p.confidence >= $min_confidence" in cypher
        assert "p.domain = $domain" not in cypher
        assert "p.predicted_event_type = $event_type" not in cypher
        assert params == {"min_confidence": 0.0}

    def test_all_filters(self):
        cypher, params = build_predictions_query(
            domain="maritime", event_type="INTERDICTION_OPERATION", min_confidence=0.4, limit=25
        )
        assert "p.domain = $domain" in cypher
        assert "p.predicted_event_type = $event_type" in cypher
        assert params == {"min_confidence": 0.4, "domain": "maritime", "event_type": "INTERDICTION_OPERATION"}
        assert "LIMIT 25" in cypher

    def test_limit_inlined_and_bounded(self):
        cypher_hi, _ = build_predictions_query(limit=99999)
        assert "LIMIT 1000" in cypher_hi  # clamped high
        assert "$limit" not in cypher_hi  # never parameterized
        cypher_lo, _ = build_predictions_query(limit=0)
        assert "LIMIT 1" in cypher_lo  # clamped low

    def test_orders_by_confidence_desc(self):
        cypher, _ = build_predictions_query()
        assert "ORDER BY p.confidence DESC" in cypher


class TestEndpoint:
    async def test_returns_shaped_response(self):
        rows = [
            {"prediction_id": "p1", "confidence": 0.8, "domain": "maritime"},
            {"prediction_id": "p2", "confidence": 0.5, "domain": "maritime"},
        ]
        mg = FakeMemgraph(rows)
        resp = await get_predictions(
            _request(mg), domain="maritime", event_type=None, min_confidence=0.3, limit=50
        )
        assert resp["count"] == 2
        assert resp["predictions"][0]["prediction_id"] == "p1"
        assert resp["filters"] == {
            "domain": "maritime", "event_type": None, "min_confidence": 0.3, "limit": 50
        }
        # the query was actually parameterized with the filters
        assert mg.last[1]["domain"] == "maritime"
        assert mg.last[1]["min_confidence"] == 0.3

    async def test_empty_result(self):
        mg = FakeMemgraph([])
        resp = await get_predictions(
            _request(mg), domain=None, event_type=None, min_confidence=0.0, limit=100
        )
        assert resp["count"] == 0
        assert resp["predictions"] == []

    async def test_response_carries_decay_fields(self):
        rows = [{"prediction_id": "p1", "confidence": 0.8, "generated_at": "2026-07-13T00:00:00Z"}]
        resp = await get_predictions(
            _request(FakeMemgraph(rows)), domain=None, event_type=None, min_confidence=0.0, limit=100
        )
        assert "current_confidence" in resp["predictions"][0]
        assert "age_days" in resp["predictions"][0]


class TestDecay:
    def test_no_decay_at_generation(self):
        now = datetime(2026, 7, 13, tzinfo=timezone.utc)
        current, age = decayed_confidence(0.8, "2026-07-13T00:00:00Z", now)
        assert current == 0.8
        assert age == 0.0

    def test_decays_over_time(self):
        now = datetime(2026, 7, 23, tzinfo=timezone.utc)  # 10 days later
        current, age = decayed_confidence(0.8, "2026-07-13T00:00:00+00:00", now)
        assert age == 10.0
        assert current < 0.8
        assert abs(current - 0.8 * (0.98 ** 10)) < 1e-4

    def test_missing_generated_at(self):
        now = datetime(2026, 7, 13, tzinfo=timezone.utc)
        current, age = decayed_confidence(0.8, "", now)
        assert current == 0.8
        assert age is None

    def test_unparseable_generated_at(self):
        now = datetime(2026, 7, 13, tzinfo=timezone.utc)
        current, age = decayed_confidence(0.5, "not-a-date", now)
        assert current == 0.5
        assert age is None

    def test_enrich_ranks_fresh_over_stale(self):
        now = datetime(2026, 7, 23, tzinfo=timezone.utc)
        rows = [
            {"prediction_id": "stale_high", "confidence": 0.9, "generated_at": "2026-06-13T00:00:00Z"},
            {"prediction_id": "fresh_mid", "confidence": 0.6, "generated_at": "2026-07-23T00:00:00Z"},
        ]
        out = enrich_with_decay(rows, now)
        # stale_high decays below fresh_mid, so the fresher prediction ranks first
        assert out[0]["prediction_id"] == "fresh_mid"
        assert all("current_confidence" in r and "age_days" in r for r in out)
