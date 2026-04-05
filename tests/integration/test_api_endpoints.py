"""Integration tests for FastAPI endpoints.

These tests use TestClient (no real DB connections) to verify routing,
parameter validation, and response structure. For full integration testing
with live services, use pytest-docker fixtures.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_memgraph():
    mg = MagicMock()
    mg.execute_and_fetch.return_value = []
    return mg


@pytest.fixture
def mock_pool():
    pool = AsyncMock()
    conn = AsyncMock()
    conn.fetch.return_value = []
    conn.fetchval.return_value = 1
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool


@pytest.fixture
def client(mock_memgraph, mock_pool):
    """Create test client with mocked dependencies."""
    from fastapi.testclient import TestClient

    # Patch lifespan to inject mocks
    with patch("api.main.asyncpg") as mock_asyncpg, \
         patch("api.main.Memgraph") as MockMG, \
         patch("api.routers.websocket.kafka_alert_consumer", new_callable=AsyncMock):
        MockMG.return_value = mock_memgraph
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        from api.main import app
        app.state.memgraph = mock_memgraph
        app.state.postgres_pool = mock_pool

        with TestClient(app) as c:
            yield c


class TestHealthEndpoint:
    def test_health_ok(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = [{"n": 1}]
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("ok", "degraded")
        assert "services" in data


class TestVesselEndpoints:
    def test_search_vessels_empty(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/vessels")
        assert resp.status_code == 200
        data = resp.json()
        assert data["vessels"] == []
        assert data["count"] == 0

    def test_search_vessels_with_filters(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/vessels?flag_state=PA&min_risk=5.0&sanctioned_only=true")
        assert resp.status_code == 200

    def test_vessel_not_found(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/vessels/9999999")
        assert resp.status_code == 404

    def test_vessel_limit_validation(self, client):
        resp = client.get("/v1/vessels?limit=1000")
        # Should be rejected since max is 500
        assert resp.status_code == 422


class TestUASEndpoints:
    def test_get_detections(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/uas/detections")
        assert resp.status_code == 200
        data = resp.json()
        assert "detections" in data

    def test_get_detections_with_filters(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/uas/detections?since_hours=48&min_confidence=0.8")
        assert resp.status_code == 200

    def test_get_sensors(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/uas/sensors")
        assert resp.status_code == 200


class TestAnalyticsEndpoints:
    def test_sanctioned_network(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/analytics/sanctioned-network")
        assert resp.status_code == 200

    def test_risk_summary(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/analytics/risk-summary")
        assert resp.status_code == 200

    def test_ais_gaps_summary(self, client, mock_memgraph):
        mock_memgraph.execute_and_fetch.return_value = []
        resp = client.get("/v1/analytics/ais-gaps-summary?days=7")
        assert resp.status_code == 200
