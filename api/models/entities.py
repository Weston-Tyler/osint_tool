"""Entity models for vessels, persons, organizations, and related types."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class VesselSummary(BaseModel):
    """Summary view of a vessel entity."""

    imo: Optional[str] = None
    mmsi: Optional[str] = None
    name: Optional[str] = None
    flag_state: Optional[str] = None
    vessel_type: Optional[str] = None
    risk_score: float = Field(0.0, ge=0.0, le=10.0)
    sanctions_status: Optional[str] = None
    ais_status: Optional[str] = None
    last_ais_position_lat: Optional[float] = None
    last_ais_position_lon: Optional[float] = None
    last_ais_timestamp: Optional[datetime] = None


class PersonSummary(BaseModel):
    """Summary view of a person entity."""

    entity_id: str
    name_full: Optional[str] = None
    aliases: list[str] = []
    nationality: Optional[str] = None
    status: Optional[str] = None
    role: Optional[str] = None
    sanctions_status: Optional[str] = None
    primary_org_id: Optional[str] = None


class OrganizationSummary(BaseModel):
    """Summary view of an organization entity."""

    entity_id: str
    name: Optional[str] = None
    aliases: list[str] = []
    org_type: Optional[str] = None
    threat_level: Optional[str] = None
    primary_country: Optional[str] = None
    sanctions_status: Optional[str] = None
    active: bool = True


class InterdictionEventSummary(BaseModel):
    """Summary view of an interdiction event."""

    event_id: str
    event_type: Optional[str] = None
    event_date: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    lead_agency: Optional[str] = None
    cargo_type: Optional[str] = None
    cargo_quantity_kg: Optional[float] = None
    cargo_estimated_value_usd: Optional[float] = None
    persons_arrested: int = 0
    vessel_disposition: Optional[str] = None


class AlertEvent(BaseModel):
    """Composite alert event from any detection pipeline."""

    alert_id: str
    alert_type: str
    severity: str = "MEDIUM"
    message: str = ""
    timestamp: datetime
    source: str = ""
    subject_mmsi: Optional[str] = None
    subject_imo: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    metadata: dict = {}
