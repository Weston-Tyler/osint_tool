"""UAS detection and tracking validation models."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class UASDetectionMessage(BaseModel):
    """Validated UAS detection event."""

    event_id: str
    source: str
    detection_timestamp: datetime
    detection_lat: float = Field(..., ge=-90, le=90)
    detection_lon: float = Field(..., ge=-180, le=180)
    detection_alt_m: float = Field(0.0, ge=0, le=15000)
    sensor_type: str
    detection_method: str
    detection_confidence: float = Field(..., ge=0.0, le=1.0)
    rf_frequency_mhz: Optional[float] = None
    uas_classification: str = "UNKNOWN"
    location_context: Optional[str] = None
    sensor_node_id: Optional[str] = None
    remote_id_broadcast: bool = False
    remote_id_uas_id: Optional[str] = None


class RemoteIDMessage(BaseModel):
    """Open Drone ID Remote ID broadcast message."""

    received_at: datetime
    rssi_dbm: int = 0
    source: str = "remote_id_wifi"
    msg_type: str
    uas_id: Optional[str] = None
    operator_id: Optional[str] = None
    lat: Optional[float] = Field(None, ge=-90, le=90)
    lon: Optional[float] = Field(None, ge=-180, le=180)
    altitude_pressure_m: Optional[float] = None
    altitude_geodetic_m: Optional[float] = None
    speed_horizontal_mps: Optional[float] = Field(None, ge=0)
    heading: Optional[int] = Field(None, ge=0, lt=360)
    op_status: Optional[int] = None
    ua_type: Optional[int] = None
    id_type: Optional[int] = None
    operator_id_type: Optional[int] = None


class FlightPathSummary(BaseModel):
    """Summary of a UAS flight path."""

    path_id: str
    drone_id: str
    start_time: datetime
    end_time: datetime
    duration_min: float
    max_altitude_m: float
    total_distance_m: float
    crossed_border: bool = False
    inferred_mission: str = "UNKNOWN"
    mission_confidence: float = Field(0.5, ge=0.0, le=1.0)
