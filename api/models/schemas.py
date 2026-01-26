"""Pydantic schemas for API request/response validation."""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
from enum import Enum


class EventType(str, Enum):
    """Event type codes."""
    DEATH = "D"
    INJURY = "I"
    MALFUNCTION = "M"
    OTHER = "O"


class EventFilters(BaseModel):
    """Filters for querying events."""
    manufacturers: Optional[list[str]] = Field(None, description="Filter by manufacturer names")
    product_codes: Optional[list[str]] = Field(None, description="Filter by FDA product codes")
    event_types: Optional[list[str]] = Field(None, description="Filter by event types (D, I, M, O)")
    date_from: Optional[date] = Field(None, description="Start date for date range")
    date_to: Optional[date] = Field(None, description="End date for date range")
    search_text: Optional[str] = Field(None, description="Search text in event narratives")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(50, ge=1, le=1000, description="Results per page")


class PaginationInfo(BaseModel):
    """Pagination metadata."""
    page: int
    page_size: int
    total: int
    total_pages: int


class EventSummary(BaseModel):
    """Summary information for an event in list view."""
    mdr_report_key: str
    report_number: Optional[str] = None
    date_received: Optional[str] = None
    date_of_event: Optional[str] = None
    event_type: Optional[str] = None
    manufacturer: Optional[str] = None
    product_code: Optional[str] = None
    manufacturer_name: Optional[str] = None


class DeviceInfo(BaseModel):
    """Device information."""
    brand_name: Optional[str] = None
    generic_name: Optional[str] = None
    model_number: Optional[str] = None
    manufacturer: Optional[str] = None
    product_code: Optional[str] = None


class NarrativeText(BaseModel):
    """Event narrative text."""
    type: Optional[str] = None
    text: Optional[str] = None


class PatientOutcomes(BaseModel):
    """Patient outcome flags."""
    death: bool = False
    life_threatening: bool = False
    hospitalization: bool = False
    disability: bool = False
    other: bool = False


class PatientInfo(BaseModel):
    """Patient information."""
    sequence: Optional[int] = None
    age: Optional[str] = None
    sex: Optional[str] = None
    outcomes: PatientOutcomes = PatientOutcomes()


class EventDetail(BaseModel):
    """Detailed event information."""
    mdr_report_key: str
    report_number: Optional[str] = None
    date_received: Optional[str] = None
    date_of_event: Optional[str] = None
    event_type: Optional[str] = None
    manufacturer: Optional[str] = None
    product_code: Optional[str] = None
    manufacturer_name: Optional[str] = None
    manufacturer_city: Optional[str] = None
    manufacturer_state: Optional[str] = None
    manufacturer_country: Optional[str] = None
    adverse_event_flag: Optional[str] = None
    product_problem_flag: Optional[str] = None
    devices: list[DeviceInfo] = []
    narratives: list[NarrativeText] = []
    patients: list[PatientInfo] = []


class EventListResponse(BaseModel):
    """Response for event list endpoint."""
    events: list[EventSummary]
    pagination: PaginationInfo


class StatsResponse(BaseModel):
    """Summary statistics response."""
    total: int
    deaths: int
    injuries: int
    malfunctions: int
    other: int


class TrendData(BaseModel):
    """Time series trend data point."""
    period: Optional[str] = None
    total: int
    deaths: int
    injuries: int
    malfunctions: int


class ManufacturerComparison(BaseModel):
    """Manufacturer comparison data."""
    manufacturer: str
    total: int
    deaths: int
    injuries: int
    malfunctions: int


class ManufacturerItem(BaseModel):
    """Manufacturer autocomplete item."""
    name: str
    count: int


class ProductCodeItem(BaseModel):
    """Product code autocomplete item."""
    code: str
    name: Optional[str] = None
    count: int


class DatabaseStatus(BaseModel):
    """Database status information."""
    total_events: int
    total_devices: int
    total_patients: int
    manufacturer_coverage_pct: float
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    last_refresh: Optional[str] = None


class IngestionLogEntry(BaseModel):
    """Ingestion log entry."""
    id: int
    file_name: str
    file_type: str
    records_loaded: int
    records_errors: int
    started_at: str
    completed_at: str
    status: str


class TextFrequencyResult(BaseModel):
    """Text frequency analysis result."""
    term: str
    count: int
    percentage: float
