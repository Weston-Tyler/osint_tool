"""Unit tests for sanctions ingestion parsing.

OFAC's standalone SDN ingester was retired in favour of OpenSanctions (which merges
the OFAC SDN list with 40+ others). These tests cover the OpenSanctions FollowTheMoney
(FTM) → MDA converters that replaced the old ``parse_ofac_*`` functions.
"""

import json

from workers.sanctions.opensanctions_ingester import (
    FTM_CONVERTERS,
    ftm_to_organization,
    ftm_to_person,
    ftm_to_vessel,
    stream_entities,
)


class TestFTMVessel:
    def test_parse_vessel(self):
        entity = {
            "id": "vessel-ever-forward",
            "schema": "Vessel",
            "datasets": ["us_ofac_sdn"],
            "properties": {
                "name": ["EVER FORWARD", "PACIFIC STAR"],
                "imoNumber": ["9123456"],
                "mmsi": ["123456789"],
                "flag": ["Panama"],
                "weakAlias": ["EVERFORWARD"],
            },
        }
        result = ftm_to_vessel(entity)
        assert result["entity_type"] == "vessel"
        assert result["imo"] == "9123456"
        assert result["mmsi"] == "123456789"
        assert result["name"] == "EVER FORWARD"
        assert result["flag_state"] == "Panama"
        assert result["sanctions_status"] == "SANCTIONED"
        assert "us_ofac_sdn" in result["sanctions_datasets"]
        # aliases = remaining names + weak aliases
        assert "PACIFIC STAR" in result["aliases"]
        assert "EVERFORWARD" in result["aliases"]

    def test_missing_properties_default_to_none(self):
        result = ftm_to_vessel({"id": "bare", "schema": "Vessel", "properties": {}})
        assert result["imo"] is None
        assert result["mmsi"] is None
        assert result["name"] is None
        assert result["aliases"] == []


class TestFTMPerson:
    def test_parse_person(self):
        entity = {
            "id": "person-guzman",
            "schema": "Person",
            "datasets": ["us_ofac_sdn"],
            "properties": {
                "name": ["Joaquin GUZMAN LOERA", "El Chapo"],
                "birthDate": ["1957-04-04"],
                "nationality": ["mx"],
                "birthPlace": ["Mexico"],
            },
        }
        result = ftm_to_person(entity)
        assert result["entity_type"] == "person"
        assert result["name_full"] == "Joaquin GUZMAN LOERA"
        assert "El Chapo" in result["aliases"]
        assert result["dob"] == "1957-04-04"
        assert result["nationality"] == "mx"
        assert result["place_of_birth"] == "Mexico"


class TestFTMOrganization:
    def test_parse_organization(self):
        entity = {
            "id": "org-front",
            "schema": "Company",
            "datasets": ["us_ofac_sdn"],
            "properties": {
                "name": ["Sinaloa Cartel Front Company LLC"],
                "jurisdiction": ["mx"],
            },
        }
        result = ftm_to_organization(entity)
        assert result["entity_type"] == "organization"
        assert result["name"] == "Sinaloa Cartel Front Company LLC"
        assert result["jurisdiction"] == "mx"
        assert result["sanctions_status"] == "SANCTIONED"

    def test_converter_registry_covers_company_and_legalentity(self):
        # Company/LegalEntity both map to the organization converter
        assert FTM_CONVERTERS["Company"] is ftm_to_organization
        assert FTM_CONVERTERS["LegalEntity"] is ftm_to_organization
        assert set(FTM_CONVERTERS) == {"Vessel", "Person", "Organization", "Company", "LegalEntity"}


class TestStreamEntities:
    def test_schema_filter(self, tmp_path):
        f = tmp_path / "entities.ftm.json"
        f.write_text(
            "\n".join(
                json.dumps(e)
                for e in [
                    {"id": "v1", "schema": "Vessel", "properties": {}},
                    {"id": "p1", "schema": "Person", "properties": {}},
                    {"id": "s1", "schema": "Sanction", "properties": {}},
                ]
            )
        )
        ids = [e["id"] for e in stream_entities(f, schema_filter=["Vessel", "Person"])]
        assert ids == ["v1", "p1"]

    def test_no_filter_yields_all(self, tmp_path):
        f = tmp_path / "entities.ftm.json"
        f.write_text(json.dumps({"id": "x", "schema": "Anything", "properties": {}}))
        assert [e["id"] for e in stream_entities(f)] == ["x"]
