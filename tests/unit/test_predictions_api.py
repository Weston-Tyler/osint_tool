"""Unit tests for the /v1/analytics/predictions endpoint and its query builder.

Exercises the router in isolation (no gqlalchemy / live Memgraph) via an injected
fake state.memgraph and a lightweight request stand-in.
"""

from types import SimpleNamespace

from api.routers.analytics import build_predictions_query, get_predictions


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
