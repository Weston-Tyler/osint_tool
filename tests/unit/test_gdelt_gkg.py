"""Unit tests for GDELT GKG theme/entity parsing and DOC API query building."""

import pytest


class TestGKGThemeFiltering:
    """Test MDA-relevant GKG theme code filtering."""

    MDA_THEMES = {
        "TAX_FAMINE", "FOOD_SECURITY", "DRUG_TRADE", "DRUG_TRAFFICKING",
        "MARITIME_INCIDENT", "MARITIME_PIRACY", "CARTEL", "SMUGGLING",
        "CRIME_ILLICIT", "MILITARY_DRONE", "DISPLACEMENT", "REFUGEES",
        "SANCTIONS", "BLOCKADE", "OIL_SPILL", "PROTEST", "RIOT",
        "WMD", "HUMAN_TRAFFICKING", "KIDNAPPING", "EXTORTION",
        "ASSASSINATION", "POLITICAL_TURMOIL", "NATURAL_DISASTER",
    }

    def _is_mda_relevant(self, themes: list[str]) -> bool:
        """Check if any theme in the list matches MDA themes (prefix match)."""
        for theme in themes:
            theme_upper = theme.upper().strip()
            for mda_theme in self.MDA_THEMES:
                if theme_upper.startswith(mda_theme):
                    return True
        return False

    def test_direct_match(self):
        assert self._is_mda_relevant(["DRUG_TRADE"]) is True

    def test_prefix_match(self):
        assert self._is_mda_relevant(["TAX_FAMINE_HUNGER"]) is True
        assert self._is_mda_relevant(["FOOD_SECURITY_AID"]) is True

    def test_no_match(self):
        assert self._is_mda_relevant(["SPORTS", "ENTERTAINMENT", "TECHNOLOGY"]) is False

    def test_mixed_themes(self):
        assert self._is_mda_relevant(["SPORTS", "CARTEL", "WEATHER"]) is True

    def test_empty(self):
        assert self._is_mda_relevant([]) is False

    def test_maritime_piracy(self):
        assert self._is_mda_relevant(["MARITIME_PIRACY_ATTACK"]) is True


class TestGKGLocationParsing:
    """Test GKG V2Locations field parsing."""

    def _parse_location(self, loc_str: str) -> dict | None:
        """Parse a single GKG V2Location entry (# delimited)."""
        parts = loc_str.split("#")
        if len(parts) < 7:
            return None
        try:
            return {
                "location_type": int(parts[0]) if parts[0] else 0,
                "name": parts[1],
                "country_code": parts[2],
                "adm1": parts[3],
                "lat": float(parts[4]) if parts[4] else None,
                "lon": float(parts[5]) if parts[5] else None,
                "feature_id": parts[6],
            }
        except (ValueError, IndexError):
            return None

    def test_valid_location(self):
        loc = self._parse_location("4#Manzanillo, Colima, Mexico#MX#MX06#19.0513#-104.316#MX06")
        assert loc is not None
        assert loc["name"] == "Manzanillo, Colima, Mexico"
        assert loc["country_code"] == "MX"
        assert abs(loc["lat"] - 19.0513) < 0.001

    def test_missing_coords(self):
        loc = self._parse_location("1#Mexico##MX00###-2441792")
        assert loc is not None
        assert loc["lat"] is None

    def test_too_short(self):
        assert self._parse_location("1#Mexico") is None


class TestGKGToneParsing:
    """Test V2Tone field parsing."""

    def _parse_tone(self, tone_str: str) -> dict:
        parts = tone_str.split(",")
        return {
            "tone": float(parts[0]) if len(parts) > 0 and parts[0] else 0.0,
            "positive": float(parts[1]) if len(parts) > 1 and parts[1] else 0.0,
            "negative": float(parts[2]) if len(parts) > 2 and parts[2] else 0.0,
            "polarity": float(parts[3]) if len(parts) > 3 and parts[3] else 0.0,
            "activity_density": float(parts[4]) if len(parts) > 4 and parts[4] else 0.0,
            "self_group_density": float(parts[5]) if len(parts) > 5 and parts[5] else 0.0,
            "word_count": int(float(parts[6])) if len(parts) > 6 and parts[6] else 0,
        }

    def test_full_tone(self):
        tone = self._parse_tone("-3.45,2.1,5.55,7.65,18.2,3.4,1250")
        assert tone["tone"] == -3.45
        assert tone["positive"] == 2.1
        assert tone["negative"] == 5.55
        assert tone["word_count"] == 1250

    def test_partial_tone(self):
        tone = self._parse_tone("-1.5,1.0,2.5")
        assert tone["tone"] == -1.5
        assert tone["word_count"] == 0


class TestDocAPIQueryBuilding:
    """Test DOC API query construction for MDA domains."""

    MDA_QUERIES = {
        "cartel_mexico": '"cartel OR narco OR trafficking" sourcecountry:MX',
        "maritime_piracy": '"maritime piracy OR vessel seizure" theme:MARITIME',
        "sanctions": '"sanctions evasion OR flag state" theme:SANCTIONS',
        "fentanyl": '"fentanyl OR cocaine seizure"',
        "border_drone": '"drone border OR UAS incursion"',
    }

    def test_query_templates_exist(self):
        for domain, query in self.MDA_QUERIES.items():
            assert len(query) > 10, f"Query for {domain} is too short"
            assert '"' in query or "theme:" in query, f"Query for {domain} has no search terms"

    def test_no_empty_queries(self):
        for domain, query in self.MDA_QUERIES.items():
            assert query.strip(), f"Empty query for {domain}"
