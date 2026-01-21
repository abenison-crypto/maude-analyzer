"""Analysis module for MAUDE data queries and exports."""

from .queries import (
    SearchQuery,
    SearchCondition,
    get_mdr_summary,
    get_manufacturer_comparison,
    get_trend_data,
    get_event_type_breakdown,
    get_record_detail,
    get_filter_options,
)
from .export import DataExporter
from .text_analysis import (
    analyze_text,
    search_narratives,
    get_term_frequency,
    get_keyword_trends,
    compare_term_frequency_by_manufacturer,
    ADVERSE_EVENT_TERMS,
)
from .signals import (
    SignalDetector,
    SafetySignal,
    SignalSeverity,
    SignalType,
    SignalDetectionResult,
    detect_signals,
    get_monthly_summary,
)
from .statistics import (
    ComparisonResult,
    TrendAnalysis,
    compare_proportions,
    compare_manufacturers,
    analyze_trend,
    get_summary_statistics,
    rank_manufacturers_by_metric,
    calculate_proportion_confidence_interval,
)
from .reports import (
    ReportConfig,
    generate_html_report,
    generate_manufacturer_report,
    save_report,
)

__all__ = [
    # Queries
    "SearchQuery",
    "SearchCondition",
    "get_mdr_summary",
    "get_manufacturer_comparison",
    "get_trend_data",
    "get_event_type_breakdown",
    "get_record_detail",
    "get_filter_options",
    # Export
    "DataExporter",
    # Text analysis
    "analyze_text",
    "search_narratives",
    "get_term_frequency",
    "get_keyword_trends",
    "compare_term_frequency_by_manufacturer",
    "ADVERSE_EVENT_TERMS",
    # Signals
    "SignalDetector",
    "SafetySignal",
    "SignalSeverity",
    "SignalType",
    "SignalDetectionResult",
    "detect_signals",
    "get_monthly_summary",
    # Statistics
    "ComparisonResult",
    "TrendAnalysis",
    "compare_proportions",
    "compare_manufacturers",
    "analyze_trend",
    "get_summary_statistics",
    "rank_manufacturers_by_metric",
    "calculate_proportion_confidence_interval",
    # Reports
    "ReportConfig",
    "generate_html_report",
    "generate_manufacturer_report",
    "save_report",
]

# Conditionally import cached functions (only when Streamlit is available)
try:
    from .cached import (
        cached_mdr_summary,
        cached_filter_options,
        cached_manufacturer_comparison,
        cached_trend_data,
        cached_event_type_breakdown,
        cached_dashboard_data,
        cached_product_codes_with_counts,
        cached_manufacturer_list,
    )
    __all__.extend([
        "cached_mdr_summary",
        "cached_filter_options",
        "cached_manufacturer_comparison",
        "cached_trend_data",
        "cached_event_type_breakdown",
        "cached_dashboard_data",
        "cached_product_codes_with_counts",
        "cached_manufacturer_list",
    ])
except ImportError:
    # Streamlit not available; cached functions won't be exported
    pass
