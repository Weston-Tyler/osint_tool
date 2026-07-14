"""Unit tests for graph_consumer interdiction ingestion + reconciliation wiring.

Uses an injected fake Memgraph that serves candidate predictions from
execute_and_fetch and records write queries from execute — no live infra.
"""

from processors.graph.graph_consumer import GraphProcessor


class FakeMemgraph:
    def __init__(self, fetch_rows=None):
        self.executes = []
        self._fetch_rows = fetch_rows or []

    def execute(self, cypher, params=None):
        self.executes.append((cypher, params))

    def execute_and_fetch(self, cypher, params=None):
        return list(self._fetch_rows)


_CANDIDATE = {
    "prediction_id": "p1",
    "predicted_event_type": "INTERDICTION_OPERATION",
    "predicted_lat": 12.5, "predicted_lon": -90.3, "predicted_region": "Eastern Pacific",
    "timeframe_max_days": 40, "generated_at": "2026-07-13T00:00:00Z", "confidence": 0.5,
}

_INTERDICTION = {
    "event_id": "evt-real-1", "event_type": "INTERDICTION_OPERATION",
    "lat": 12.6, "lon": -90.2, "region": "Eastern Pacific",
    "occurred_at": "2026-07-25T00:00:00Z",
}


def test_interdiction_upserted_and_matching_prediction_linked():
    mg = FakeMemgraph(fetch_rows=[_CANDIDATE])
    gp = GraphProcessor(memgraph=mg)
    gp.process_message("mda.interdictions.new", _INTERDICTION)
    joined = " ".join(c for c, _ in mg.executes)
    assert ":Event" in joined  # realized event upserted
    assert "REALIZED_BY" in joined  # matching prediction linked
    link = [p for c, p in mg.executes if "REALIZED_BY" in c][0]
    assert link["pid"] == "p1" and link["eid"] == "evt-real-1"


def test_no_link_when_type_mismatch():
    mg = FakeMemgraph(fetch_rows=[_CANDIDATE])
    gp = GraphProcessor(memgraph=mg)
    gp.process_message("mda.interdictions.new", {**_INTERDICTION, "event_type": "PORT_VISIT"})
    joined = " ".join(c for c, _ in mg.executes)
    assert ":Event" in joined  # event still upserted
    assert "REALIZED_BY" not in joined  # but nothing matched


def test_interdiction_without_event_id_is_skipped():
    mg = FakeMemgraph(fetch_rows=[_CANDIDATE])
    gp = GraphProcessor(memgraph=mg)
    gp.process_message("mda.interdictions.new", {"event_type": "INTERDICTION_OPERATION"})
    assert mg.executes == []
