"""Load testing for MDA API using Locust.

Run: locust -f tests/load/locustfile.py --host=http://localhost:8000

Targets:
  - /health: p95 < 50ms
  - /v1/vessels: p95 < 200ms
  - /v1/vessels/{id}: p95 < 500ms
  - /v1/uas/detections: p95 < 200ms
  - /v1/analytics/risk-summary: p95 < 1000ms
"""

import random

from locust import HttpUser, between, task


# Sample vessel identifiers for testing
SAMPLE_IMOS = ["9123456", "7654321", "8888888", "1234567", "9876543"]
SAMPLE_MMSIS = ["123456789", "987654321", "111222333", "444555666", "777888999"]
FLAG_STATES = ["PA", "LR", "MH", "US", "SG", "KP", "IR", "GB", "CN", "JP"]


class MDAViewerUser(HttpUser):
    """Simulates a read-only dashboard user browsing vessel data."""

    weight = 5
    wait_time = between(1, 3)

    @task(10)
    def health_check(self):
        self.client.get("/health")

    @task(20)
    def search_vessels(self):
        self.client.get("/v1/vessels?limit=50")

    @task(15)
    def search_vessels_filtered(self):
        flag = random.choice(FLAG_STATES)
        self.client.get(f"/v1/vessels?flag_state={flag}&limit=20")

    @task(10)
    def search_sanctioned(self):
        self.client.get("/v1/vessels?sanctioned_only=true&limit=100")

    @task(5)
    def search_high_risk(self):
        self.client.get("/v1/vessels?min_risk=7.0&limit=50")


class MDAOperatorUser(HttpUser):
    """Simulates an operator querying specific vessels and UAS detections."""

    weight = 3
    wait_time = between(2, 5)

    @task(10)
    def get_vessel_detail(self):
        identifier = random.choice(SAMPLE_IMOS + SAMPLE_MMSIS)
        with self.client.get(f"/v1/vessels/{identifier}", catch_response=True) as resp:
            if resp.status_code == 404:
                resp.success()  # Expected for sample data

    @task(8)
    def get_vessel_track(self):
        identifier = random.choice(SAMPLE_IMOS)
        with self.client.get(f"/v1/vessels/{identifier}/track?days=7", catch_response=True) as resp:
            if resp.status_code == 404:
                resp.success()

    @task(10)
    def get_uas_detections(self):
        self.client.get("/v1/uas/detections?since_hours=24")

    @task(5)
    def get_uas_sensors(self):
        self.client.get("/v1/uas/sensors")

    @task(5)
    def get_flight_paths(self):
        self.client.get("/v1/uas/flight-paths?since_hours=48")


class MDAAnalystUser(HttpUser):
    """Simulates an analyst running complex network queries."""

    weight = 1
    wait_time = between(5, 15)

    @task(5)
    def sanctioned_network(self):
        self.client.get("/v1/analytics/sanctioned-network")

    @task(5)
    def risk_summary(self):
        self.client.get("/v1/analytics/risk-summary")

    @task(3)
    def ais_gaps_summary(self):
        self.client.get("/v1/analytics/ais-gaps-summary?days=30")

    @task(2)
    def interdiction_heatmap(self):
        self.client.get("/v1/analytics/interdiction-heatmap?months=12&clusters=10")

    @task(3)
    def vessel_network_traversal(self):
        imo = random.choice(SAMPLE_IMOS)
        with self.client.get(f"/v1/vessels/{imo}/network?hops=2", catch_response=True) as resp:
            if resp.status_code == 404:
                resp.success()

    @task(5)
    def metrics(self):
        self.client.get("/metrics/summary")
