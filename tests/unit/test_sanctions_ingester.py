"""Unit tests for sanctions ingestion parsing."""

import pytest

from workers.sanctions.ofac_sdn_ingester import parse_ofac_vessel, parse_ofac_individual, parse_ofac_entity


class TestOFACParsing:
    def test_parse_vessel(self):
        entry = {
            "uid": 12345,
            "sdnType": "Vessel",
            "lastName": "EVER FORWARD",
            "ids": [
                {"idType": "IMO Number", "idNumber": "IMO 9123456"},
                {"idType": "MMSI", "idNumber": "123456789"},
            ],
            "programs": [{"program": "IRAN"}],
            "akas": [{"lastName": "PACIFIC STAR"}],
            "remarks": "Flag: Panama",
        }
        result = parse_ofac_vessel(entry)
        assert result is not None
        assert result["imo"] == "9123456"
        assert result["mmsi"] == "123456789"
        assert result["name"] == "EVER FORWARD"
        assert result["sanctions_status"] == "SANCTIONED"
        assert "IRAN" in result["ofac_programs"]
        assert "PACIFIC STAR" in result["aliases"]

    def test_parse_non_vessel_returns_none(self):
        entry = {"uid": 99999, "sdnType": "Individual", "lastName": "Smith"}
        assert parse_ofac_vessel(entry) is None

    def test_parse_individual(self):
        entry = {
            "uid": 54321,
            "sdnType": "Individual",
            "firstName": "Joaquin",
            "lastName": "GUZMAN LOERA",
            "dateOfBirth": "1957-04-04",
            "placeOfBirth": "Mexico",
            "nationality": "MX",
            "programs": [{"program": "SDNT"}],
            "akas": [{"firstName": "El Chapo", "lastName": "GUZMAN"}],
            "remarks": "",
        }
        result = parse_ofac_individual(entry)
        assert result is not None
        assert result["name_full"] == "Joaquin GUZMAN LOERA"
        assert "El Chapo GUZMAN" in result["aliases"]

    def test_parse_entity(self):
        entry = {
            "uid": 67890,
            "sdnType": "Entity",
            "lastName": "Sinaloa Cartel Front Company LLC",
            "programs": [{"program": "SDNTK"}],
            "akas": [],
            "remarks": "",
        }
        result = parse_ofac_entity(entry)
        assert result is not None
        assert result["entity_type"] == "organization"
