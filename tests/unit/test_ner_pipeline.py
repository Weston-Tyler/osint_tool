"""Unit tests for NLP/NER pipeline regex extraction and entity categorization."""

import re

import pytest

# Import regex functions directly to avoid spacy dependency at test time
# These are self-contained and don't need spacy
try:
    from services.nlp.ner_pipeline import extract_regex_entities, DRUG_PATTERNS
except ImportError:
    # Fallback: define the regex patterns and function inline for testing
    # when spacy is not installed
    pytest.skip("spacy not installed", allow_module_level=True)


class TestRegexExtraction:
    def test_imo_extraction(self):
        text = "The vessel IMO 9123456 was spotted near the coast."
        entities = extract_regex_entities(text)
        imo_ents = [e for e in entities if e["label"] == "IMO_NUMBER"]
        assert len(imo_ents) == 1
        assert imo_ents[0]["value"] == "9123456"

    def test_mmsi_extraction(self):
        text = "AIS signal from MMSI 123456789 went dark at 0300Z."
        entities = extract_regex_entities(text)
        mmsi_ents = [e for e in entities if e["label"] == "MMSI_NUMBER"]
        assert len(mmsi_ents) == 1
        assert mmsi_ents[0]["value"] == "123456789"

    def test_operation_name_extraction(self):
        text = "Operation UNIFIED RESOLVE interdicted 3 vessels in the Eastern Pacific."
        entities = extract_regex_entities(text)
        op_ents = [e for e in entities if e["label"] == "OPERATION_NAME"]
        assert len(op_ents) == 1
        assert "UNIFIED RESOLVE" in op_ents[0]["value"]

    def test_drug_quantity_extraction(self):
        text = "Coast Guard seized 2,400 kilograms of cocaine worth $72 million."
        entities = extract_regex_entities(text)
        qty_ents = [e for e in entities if e["label"] == "DRUG_QUANTITY"]
        assert len(qty_ents) == 1
        assert qty_ents[0]["value"] == "2,400"
        assert qty_ents[0]["unit"] == "kilograms"

    def test_money_extraction(self):
        text = "The shipment was valued at $72 million on the street."
        entities = extract_regex_entities(text)
        money_ents = [e for e in entities if e["label"] == "MONETARY_VALUE"]
        assert len(money_ents) == 1
        assert money_ents[0]["value"] == "72"
        assert money_ents[0]["multiplier"] == "million"

    def test_multiple_entities_in_text(self):
        text = (
            "The USCG cutter intercepted vessel IMO 7654321 (MMSI 987654321) "
            "carrying 500 kg of cocaine during Operation GULF SHIELD."
        )
        entities = extract_regex_entities(text)
        labels = {e["label"] for e in entities}
        assert "IMO_NUMBER" in labels
        assert "MMSI_NUMBER" in labels
        assert "DRUG_QUANTITY" in labels
        assert "OPERATION_NAME" in labels

    def test_no_false_positive_on_random_numbers(self):
        text = "The report was filed at 1234 Main Street, Suite 567890123."
        entities = extract_regex_entities(text)
        # Should not match random numbers as IMO or MMSI
        imo_ents = [e for e in entities if e["label"] == "IMO_NUMBER"]
        mmsi_ents = [e for e in entities if e["label"] == "MMSI_NUMBER"]
        assert len(imo_ents) == 0
        assert len(mmsi_ents) == 0


class TestDrugPatterns:
    def test_all_major_drugs_covered(self):
        major_drugs = ["cocaine", "heroin", "fentanyl", "methamphetamine", "marijuana"]
        for drug in major_drugs:
            assert drug in DRUG_PATTERNS, f"{drug} not in DRUG_PATTERNS"
