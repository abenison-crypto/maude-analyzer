"""Pydantic schemas for advanced signal detection API."""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import date
from enum import Enum


class SignalMethod(str, Enum):
    """Signal detection methods."""
    ZSCORE = "zscore"
    PRR = "prr"
    ROR = "ror"
    EBGM = "ebgm"
    CUSUM = "cusum"
    YOY = "yoy"  # Year-over-year
    POP = "pop"  # Period-over-period
    ROLLING = "rolling"


class DrillDownLevel(str, Enum):
    """Hierarchical drill-down levels."""
    MANUFACTURER = "manufacturer"
    BRAND = "brand"
    GENERIC = "generic"
    MODEL = "model"


class TimeComparisonMode(str, Enum):
    """Time comparison modes."""
    LOOKBACK = "lookback"
    CUSTOM = "custom"
    YOY = "yoy"
    ROLLING = "rolling"


class ComparisonPopulation(str, Enum):
    """Comparison population for disproportionality methods."""
    ALL = "all"
    SAME_PRODUCT_CODE = "same_product_code"
    CUSTOM = "custom"


class TimePeriod(BaseModel):
    """Time period definition for custom range comparisons."""
    start_date: date
    end_date: date


class TimeComparisonConfig(BaseModel):
    """Configuration for time-based comparisons."""
    mode: TimeComparisonMode = TimeComparisonMode.LOOKBACK
    lookback_months: int = Field(default=12, ge=1, le=120)
    # For custom mode
    period_a: Optional[TimePeriod] = None
    period_b: Optional[TimePeriod] = None
    # For YoY mode
    current_year: Optional[int] = None
    comparison_year: Optional[int] = None
    quarter: Optional[int] = Field(default=None, ge=1, le=4)
    # For rolling mode
    rolling_window_months: int = Field(default=3, ge=1, le=24)
    # For z-score: specific month to compare (defaults to latest month if not set)
    comparison_month: Optional[date] = Field(
        default=None,
        description="Specific month to analyze for z-score (first day of month)"
    )


class ActiveEntityGroup(BaseModel):
    """Active entity group for query transformation."""
    id: str
    display_name: str
    members: list[str]
    entity_type: str = "manufacturer"


class SignalRequest(BaseModel):
    """Request schema for advanced signal detection."""
    # Detection methods to apply
    methods: list[SignalMethod] = Field(
        default=[SignalMethod.ZSCORE],
        description="Signal detection methods to apply"
    )

    # Time configuration
    time_config: TimeComparisonConfig = Field(default_factory=TimeComparisonConfig)

    # Drill-down level
    level: DrillDownLevel = Field(
        default=DrillDownLevel.MANUFACTURER,
        description="Aggregation level for signal detection"
    )
    parent_value: Optional[str] = Field(
        default=None,
        description="Parent entity value for drill-down (e.g., manufacturer name when level=brand)"
    )

    # Filters
    product_codes: Optional[list[str]] = None
    event_types: Optional[list[str]] = None

    # Comparison population for PRR/ROR/EBGM
    comparison_population: ComparisonPopulation = Field(
        default=ComparisonPopulation.ALL,
        description="Comparison population for disproportionality methods"
    )
    comparison_filters: Optional[dict] = Field(
        default=None,
        description="Custom filters for comparison population"
    )

    # Entity groups - active groups to apply during analysis
    active_groups: Optional[list[ActiveEntityGroup]] = Field(
        default=None,
        description="Active entity groups to apply. Group members are treated as a single entity."
    )

    # Date field for analysis
    date_field: Literal["date_received", "date_of_event"] = Field(
        default="date_received",
        description="Date field to use: date_received (when FDA got report) or date_of_event (when event occurred)"
    )

    # Thresholds and limits
    min_events: int = Field(default=10, ge=1, description="Minimum events to consider")
    limit: int = Field(default=20, ge=1, le=100, description="Maximum results to return")

    # Signal thresholds (optional overrides)
    zscore_high_threshold: float = Field(default=2.0, description="Z-score threshold for high signal")
    zscore_elevated_threshold: float = Field(default=1.0, description="Z-score threshold for elevated signal")
    prr_threshold: float = Field(default=2.0, description="PRR threshold for signal")
    ror_threshold: float = Field(default=2.0, description="ROR threshold for signal")
    change_pct_high: float = Field(default=100.0, description="Percentage change for high signal")
    change_pct_elevated: float = Field(default=50.0, description="Percentage change for elevated signal")


class MethodResult(BaseModel):
    """Result for a single detection method."""
    method: SignalMethod
    value: Optional[float] = None
    lower_ci: Optional[float] = None  # Lower confidence interval (for PRR/ROR)
    upper_ci: Optional[float] = None  # Upper confidence interval
    is_signal: bool = False
    signal_strength: Literal["high", "elevated", "normal"] = "normal"

    # Method-specific details
    details: Optional[dict] = None  # Extra info (e.g., CUSUM cumulative sum, rolling baseline)


class SignalResult(BaseModel):
    """Result for a single entity's signal analysis."""
    entity: str
    entity_level: DrillDownLevel

    # Event counts
    total_events: int
    deaths: int
    injuries: int
    malfunctions: int

    # Time-based counts
    current_period_events: Optional[int] = None
    comparison_period_events: Optional[int] = None
    change_pct: Optional[float] = None

    # Method results
    method_results: list[MethodResult] = []

    # Overall signal classification (based on all methods)
    signal_type: Literal["high", "elevated", "normal"] = "normal"

    # Drill-down indicator
    has_children: bool = False
    child_level: Optional[DrillDownLevel] = None


class TimeInfo(BaseModel):
    """Information about the time range used in analysis."""
    mode: TimeComparisonMode
    analysis_start: date
    analysis_end: date
    comparison_start: Optional[date] = None
    comparison_end: Optional[date] = None
    rolling_window: Optional[int] = None


class DataCompleteness(BaseModel):
    """Information about data completeness and reporting lag."""
    last_complete_month: str
    incomplete_months: list[str] = []
    estimated_lag_months: int = 2


class MonthlyDataPoint(BaseModel):
    """Single point in a monthly time series."""
    month: str
    count: int


class CUSUMDataPoint(BaseModel):
    """Single point in a CUSUM series."""
    month: str
    cusum: float
    count: int


class SignalResponse(BaseModel):
    """Response schema for advanced signal detection."""
    # Request info
    level: DrillDownLevel
    parent_value: Optional[str] = None
    methods_applied: list[SignalMethod]

    # Time information
    time_info: TimeInfo

    # Results
    signals: list[SignalResult]
    total_entities_analyzed: int

    # Summary counts
    high_signal_count: int
    elevated_signal_count: int
    normal_count: int

    # Metadata
    data_note: Optional[str] = None
    data_completeness: Optional[DataCompleteness] = None


class DisproportionalityInput(BaseModel):
    """Input counts for disproportionality calculations (2x2 table)."""
    a: int  # Target events for entity
    b: int  # Other events for entity
    c: int  # Target events for others
    d: int  # Other events for others


class DisproportionalityResult(BaseModel):
    """Result of disproportionality calculation."""
    value: float
    lower_ci: float
    upper_ci: float
    is_signal: bool
    n: int  # Number of target events
