"""AIS position and gap event validation models."""

import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class AISPositionMessage(BaseModel):
    """Validated AIS position record from any source."""

    source: str
    mmsi: str = Field(..., min_length=9, max_length=9)
    imo: Optional[str] = None
    timestamp: datetime
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    speed_kts: float = Field(0.0, ge=0, le=102.2)
    course: float = Field(0.0, ge=0, lt=360)
    heading: int = Field(511, ge=0, le=511)  # 511 = not available
    nav_status: int = Field(15, ge=0, le=15)  # 15 = not defined
    vessel_name: Optional[str] = None
    vessel_type: Optional[int] = None
    length_m: Optional[float] = Field(None, ge=0, le=500)
    draft_m: Optional[float] = Field(None, ge=0, le=30)

    @field_validator("mmsi")
    @classmethod
    def validate_mmsi(cls, v: str) -> str:
        if not re.match(r"^\d{9}$", v):
            raise ValueError(f"Invalid MMSI format: {v}")
        return v

    @field_validator("imo")
    @classmethod
    def validate_imo(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        # Some sources include "IMO " prefix
        v = re.sub(r"^IMO\s*", "", str(v)).strip()
        if not re.match(r"^\d{7}$", v):
            return None
        return v


class AISGapEvent(BaseModel):
    """Detected AIS gap event."""

    event_id: str
    vessel_mmsi: str
    gap_start_time: datetime
    gap_end_time: datetime
    gap_duration_hours: float = Field(..., ge=0)
    last_position_lat: float = Field(..., ge=-90, le=90)
    last_position_lon: float = Field(..., ge=-180, le=180)
    last_speed_kts: float = Field(0.0, ge=0)
    last_nav_status: int = Field(15, ge=0, le=15)
    resume_position_lat: float = Field(..., ge=-90, le=90)
    resume_position_lon: float = Field(..., ge=-180, le=180)
    resume_speed_kts: float = Field(0.0, ge=0)
    displacement_km: float = Field(0.0, ge=0)
    implied_speed_kts: float = Field(0.0, ge=0)
    probable_cause: str
    risk_flag: bool = False
    severity: str = "MEDIUM"
    source: str = "ais_gap_detector_v1"


class STSDetectionEvent(BaseModel):
    """Detected ship-to-ship transfer event."""

    event_id: str
    event_type: str = "STS_PROBABLE"
    vessel_a_mmsi: str
    vessel_b_mmsi: str
    start_time: datetime
    end_time: datetime
    duration_min: float = Field(..., ge=0)
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    source: str = "sts_detector_v1"
    confidence: float = Field(0.7, ge=0.0, le=1.0)
    risk_flag: bool = True
