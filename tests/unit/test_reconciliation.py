"""Unit tests for the pure predicted-vs-realized reconciliation logic."""

from processors.graph.reconciliation import (
    build_interdiction_event_cypher,
    build_realized_by_cypher,
    haversine_km,
    match_prediction,
    type_matches,
)


def _prediction(**over):
    p = {
        "prediction_id": "p1",
        "predicted_event_type": "INTERDICTION_OPERATION",
        "predicted_lat": 12.5, "predicted_lon": -90.3, "predicted_region": "Eastern Pacific",
        "timeframe_max_days": 40,
        "generated_at": "2026-07-13T00:00:00Z",
        "confidence": 0.5,
    }
    p.update(over)
    return p


def _realized(**over):
    r = {
        "event_id": "evt-real-1", "event_type": "INTERDICTION_OPERATION",
        "lat": 12.6, "lon": -90.2, "region": "Eastern Pacific",
        "occurred_at": "2026-07-25T00:00:00Z",
    }
    r.update(over)
    return r


class TestTypeMatch:
    def test_exact(self):
        assert type_matches("INTERDICTION_OPERATION", "INTERDICTION_OPERATION")

    def test_alias(self):
        assert type_matches("INTERDICTION_OPERATION", "SEIZURE")
        assert type_matches("MARITIME_VIOLENT_INCIDENT", "ARMED_CLASH")

    def test_no_match(self):
        assert not type_matches("INTERDICTION_OPERATION", "PORT_VISIT")
        assert not type_matches(None, "X")


class TestHaversine:
    def test_zero(self):
        assert haversine_km(12.5, -90.3, 12.5, -90.3) == 0.0

    def test_known_distance(self):
        # ~1 degree of latitude ~= 111 km
        assert 100 < haversine_km(0.0, 0.0, 1.0, 0.0) < 120


class TestMatchPrediction:
    def test_match(self):
        m = match_prediction(_prediction(), _realized())
        assert m is not None
        assert m["distance_km"] is not None and m["distance_km"] < 250
        assert m["lead_time_days"] == 12.0
        assert m["confidence_at_prediction"] == 0.5

    def test_wrong_type(self):
        assert match_prediction(_prediction(), _realized(event_type="PORT_VISIT")) is None

    def test_too_far(self):
        assert match_prediction(_prediction(), _realized(lat=40.0, lon=10.0)) is None

    def test_before_generation(self):
        assert match_prediction(_prediction(), _realized(occurred_at="2026-07-01T00:00:00Z")) is None

    def test_past_window(self):
        # 40-day window + 7 grace; a realization 100 days later does not count
        assert match_prediction(_prediction(), _realized(occurred_at="2026-10-25T00:00:00Z")) is None

    def test_region_fallback_when_no_coords(self):
        pred = _prediction(predicted_lat=None, predicted_lon=None)
        assert match_prediction(pred, _realized(lat=None, lon=None)) is not None
        assert match_prediction(pred, _realized(lat=None, lon=None, region="Caribbean")) is None


class TestCypherBuilders:
    def test_interdiction_event(self):
        built = build_interdiction_event_cypher(_realized())
        assert built is not None
        cypher, params = built
        assert ":Event" in cypher and params["eid"] == "evt-real-1"

    def test_interdiction_no_event_id(self):
        assert build_interdiction_event_cypher({"event_type": "INTERDICTION_OPERATION"}) is None

    def test_realized_by(self):
        match = {"distance_km": 12.0, "lead_time_days": 12.0, "confidence_at_prediction": 0.5}
        cypher, params = build_realized_by_cypher("p1", "evt-real-1", match)
        assert "REALIZED_BY" in cypher and "p.realized = true" in cypher
        assert params["pid"] == "p1" and params["eid"] == "evt-real-1"
        assert params["distance_km"] == 12.0
