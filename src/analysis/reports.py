"""Report generation module for MAUDE Analyzer."""

import io
import base64
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, CHART_COLORS, MANUFACTURER_COLORS, EVENT_TYPES
from config.logging_config import get_logger
from src.database import get_connection
from src.analysis.queries import get_mdr_summary, get_manufacturer_comparison, get_trend_data
from src.analysis.statistics import get_summary_statistics, rank_manufacturers_by_metric
from src.analysis.signals import detect_signals, SignalSeverity

logger = get_logger("reports")


@dataclass
class ReportConfig:
    """Configuration for report generation."""

    title: str = "MAUDE Analysis Report"
    subtitle: Optional[str] = None
    include_executive_summary: bool = True
    include_trends: bool = True
    include_manufacturer_comparison: bool = True
    include_signals: bool = True
    include_data_tables: bool = True
    manufacturers: Optional[List[str]] = None
    product_codes: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


def generate_html_report(
    config: Optional[ReportConfig] = None,
    conn=None,
) -> str:
    """
    Generate a complete HTML report.

    Args:
        config: Report configuration.
        conn: Database connection.

    Returns:
        HTML string of the report.
    """
    report_config = config or ReportConfig()
    own_conn = conn is None

    if own_conn:
        conn = get_connection()

    try:
        # Gather data
        summary = get_summary_statistics(
            manufacturers=report_config.manufacturers,
            product_codes=report_config.product_codes,
            start_date=report_config.start_date,
            end_date=report_config.end_date,
            conn=conn,
        )

        mfr_comparison = get_manufacturer_comparison(
            manufacturers=report_config.manufacturers,
            start_date=report_config.start_date,
            end_date=report_config.end_date,
            conn=conn,
        )

        trend_data = get_trend_data(
            aggregation="monthly",
            manufacturers=report_config.manufacturers,
            product_codes=report_config.product_codes,
            start_date=report_config.start_date,
            end_date=report_config.end_date,
            conn=conn,
        )

        signals = detect_signals(
            manufacturers=report_config.manufacturers,
            product_codes=report_config.product_codes,
        )

        # Build HTML
        html_parts = [_get_html_header(report_config)]

        # Executive summary
        if report_config.include_executive_summary:
            html_parts.append(_generate_executive_summary(summary, signals))

        # Trends section
        if report_config.include_trends and not trend_data.empty:
            html_parts.append(_generate_trends_section(trend_data))

        # Manufacturer comparison
        if report_config.include_manufacturer_comparison and not mfr_comparison.empty:
            html_parts.append(_generate_manufacturer_section(mfr_comparison))

        # Signals section
        if report_config.include_signals and signals.signals:
            html_parts.append(_generate_signals_section(signals))

        # Data tables
        if report_config.include_data_tables:
            html_parts.append(_generate_data_tables(mfr_comparison))

        html_parts.append(_get_html_footer())

        return "\n".join(html_parts)

    finally:
        if own_conn:
            conn.close()


def _get_html_header(config: ReportConfig) -> str:
    """Generate HTML header with styles."""
    generated_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{config.title}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #fff;
        }}
        .header {{
            text-align: center;
            border-bottom: 2px solid #1f77b4;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        h1 {{
            color: #1f77b4;
            margin-bottom: 5px;
        }}
        h2 {{
            color: #2c3e50;
            border-bottom: 1px solid #ddd;
            padding-bottom: 10px;
            margin-top: 40px;
        }}
        h3 {{
            color: #34495e;
        }}
        .subtitle {{
            color: #666;
            font-size: 1.1em;
        }}
        .generated-date {{
            color: #888;
            font-size: 0.9em;
        }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin: 20px 0;
        }}
        .kpi-card {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            border-left: 4px solid #1f77b4;
        }}
        .kpi-value {{
            font-size: 2em;
            font-weight: bold;
            color: #2c3e50;
        }}
        .kpi-label {{
            color: #666;
            font-size: 0.9em;
        }}
        .kpi-card.death {{
            border-left-color: {CHART_COLORS['death']};
        }}
        .kpi-card.injury {{
            border-left-color: {CHART_COLORS['injury']};
        }}
        .signal-card {{
            background: #fff;
            border-radius: 8px;
            padding: 15px;
            margin: 10px 0;
            border-left: 4px solid #ddd;
        }}
        .signal-critical {{
            border-left-color: #d62728;
            background: #fef0f0;
        }}
        .signal-high {{
            border-left-color: #ff7f0e;
            background: #fff8f0;
        }}
        .signal-medium {{
            border-left-color: #ffc107;
            background: #fffef0;
        }}
        .signal-low {{
            border-left-color: #17a2b8;
            background: #f0f9ff;
        }}
        .signal-title {{
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .signal-severity {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.8em;
            font-weight: bold;
            text-transform: uppercase;
        }}
        .severity-critical {{ background: #d62728; color: white; }}
        .severity-high {{ background: #ff7f0e; color: white; }}
        .severity-medium {{ background: #ffc107; color: #333; }}
        .severity-low {{ background: #17a2b8; color: white; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .chart-container {{
            margin: 20px 0;
            text-align: center;
        }}
        .chart-container img {{
            max-width: 100%;
            height: auto;
        }}
        @media print {{
            body {{
                padding: 0;
            }}
            .page-break {{
                page-break-before: always;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{config.title}</h1>
        {'<p class="subtitle">' + config.subtitle + '</p>' if config.subtitle else ''}
        <p class="generated-date">Generated: {generated_date}</p>
    </div>
"""


def _get_html_footer() -> str:
    """Generate HTML footer."""
    return """
    <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; color: #888; font-size: 0.9em;">
        <p>Generated by MAUDE Analyzer | Data source: FDA MAUDE Database</p>
    </div>
</body>
</html>
"""


def _generate_executive_summary(summary: Dict[str, Any], signals) -> str:
    """Generate executive summary section."""
    critical_signals = sum(1 for s in signals.signals if s.severity == SignalSeverity.CRITICAL)
    high_signals = sum(1 for s in signals.signals if s.severity == SignalSeverity.HIGH)

    date_range = ""
    if summary.get("date_range", {}).get("min"):
        date_range = f"{summary['date_range']['min']} to {summary['date_range']['max']}"

    return f"""
    <h2>Executive Summary</h2>

    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-value">{summary.get('total_mdrs', 0):,}</div>
            <div class="kpi-label">Total MDRs</div>
        </div>
        <div class="kpi-card death">
            <div class="kpi-value">{summary.get('deaths', 0):,}</div>
            <div class="kpi-label">Deaths ({summary.get('death_rate', 0):.1f}%)</div>
        </div>
        <div class="kpi-card injury">
            <div class="kpi-value">{summary.get('injuries', 0):,}</div>
            <div class="kpi-label">Injuries ({summary.get('injury_rate', 0):.1f}%)</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">{summary.get('malfunctions', 0):,}</div>
            <div class="kpi-label">Malfunctions ({summary.get('malfunction_rate', 0):.1f}%)</div>
        </div>
    </div>

    <h3>Key Findings</h3>
    <ul>
        <li><strong>Data Period:</strong> {date_range or 'All available data'}</li>
        <li><strong>Manufacturers Analyzed:</strong> {summary.get('unique_manufacturers', 0)}</li>
        <li><strong>Product Codes:</strong> {summary.get('unique_products', 0)}</li>
        <li><strong>Safety Signals Detected:</strong> {len(signals.signals)}
            ({critical_signals} critical, {high_signals} high severity)</li>
    </ul>
"""


def _generate_trends_section(trend_data: pd.DataFrame) -> str:
    """Generate trends section with chart."""
    # Aggregate by period
    trend_agg = trend_data.groupby("period").agg({
        "total_mdrs": "sum",
        "deaths": "sum",
        "injuries": "sum",
        "malfunctions": "sum",
    }).reset_index()

    trend_agg["period"] = pd.to_datetime(trend_agg["period"].astype(str))

    # Create trend chart
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=trend_agg["period"],
        y=trend_agg["total_mdrs"],
        mode="lines+markers",
        name="Total MDRs",
        line=dict(color=CHART_COLORS["primary"], width=2),
    ))

    fig.add_trace(go.Scatter(
        x=trend_agg["period"],
        y=trend_agg["deaths"],
        mode="lines+markers",
        name="Deaths",
        line=dict(color=CHART_COLORS["death"], width=2),
    ))

    fig.update_layout(
        title="MDR Trends Over Time",
        xaxis_title="Date",
        yaxis_title="Count",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=50, r=50, t=50, b=50),
        height=400,
    )

    # Convert to base64 image
    img_bytes = fig.to_image(format="png", scale=2)
    img_base64 = base64.b64encode(img_bytes).decode()

    return f"""
    <h2>Trends Analysis</h2>
    <div class="chart-container">
        <img src="data:image/png;base64,{img_base64}" alt="MDR Trends">
    </div>

    <h3>Monthly Statistics</h3>
    <table>
        <thead>
            <tr>
                <th>Period</th>
                <th>Total MDRs</th>
                <th>Deaths</th>
                <th>Injuries</th>
                <th>Malfunctions</th>
            </tr>
        </thead>
        <tbody>
            {''.join(f'''
            <tr>
                <td>{row["period"].strftime("%Y-%m")}</td>
                <td>{row["total_mdrs"]:,}</td>
                <td>{row["deaths"]:,}</td>
                <td>{row["injuries"]:,}</td>
                <td>{row["malfunctions"]:,}</td>
            </tr>
            ''' for _, row in trend_agg.tail(12).iterrows())}
        </tbody>
    </table>
"""


def _generate_manufacturer_section(mfr_df: pd.DataFrame) -> str:
    """Generate manufacturer comparison section."""
    # Create comparison chart
    top_mfrs = mfr_df.nlargest(10, "total_mdrs")

    fig = px.bar(
        top_mfrs.sort_values("total_mdrs", ascending=True),
        x="total_mdrs",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
        color_discrete_map=MANUFACTURER_COLORS,
        labels={"total_mdrs": "Total MDRs", "manufacturer_clean": "Manufacturer"},
    )

    fig.update_layout(
        title="Top 10 Manufacturers by MDR Volume",
        showlegend=False,
        margin=dict(l=50, r=50, t=50, b=50),
        height=400,
    )

    img_bytes = fig.to_image(format="png", scale=2)
    img_base64 = base64.b64encode(img_bytes).decode()

    return f"""
    <div class="page-break"></div>
    <h2>Manufacturer Comparison</h2>
    <div class="chart-container">
        <img src="data:image/png;base64,{img_base64}" alt="Manufacturer Comparison">
    </div>
"""


def _generate_signals_section(signals) -> str:
    """Generate safety signals section."""
    if not signals.signals:
        return """
    <h2>Safety Signals</h2>
    <p>No significant safety signals detected for this period.</p>
"""

    signal_cards = []
    for signal in signals.signals[:10]:  # Limit to top 10
        severity_class = f"signal-{signal.severity.value}"
        severity_badge = f"severity-{signal.severity.value}"

        signal_cards.append(f"""
        <div class="signal-card {severity_class}">
            <div class="signal-title">
                <span class="signal-severity {severity_badge}">{signal.severity.value}</span>
                {signal.title}
            </div>
            <p>{signal.description}</p>
            {f'<p><small>Change: {signal.percent_change:.1f}%</small></p>' if signal.percent_change else ''}
        </div>
        """)

    return f"""
    <h2>Safety Signals</h2>
    <p>The following safety signals were detected based on statistical analysis of recent data:</p>
    {''.join(signal_cards)}
"""


def _generate_data_tables(mfr_df: pd.DataFrame) -> str:
    """Generate data tables section."""
    if mfr_df.empty:
        return ""

    return f"""
    <div class="page-break"></div>
    <h2>Detailed Data</h2>

    <h3>Manufacturer Summary</h3>
    <table>
        <thead>
            <tr>
                <th>Manufacturer</th>
                <th>Total MDRs</th>
                <th>Deaths</th>
                <th>Injuries</th>
                <th>Malfunctions</th>
                <th>Death Rate</th>
            </tr>
        </thead>
        <tbody>
            {''.join(f'''
            <tr>
                <td>{row["manufacturer_clean"]}</td>
                <td>{row["total_mdrs"]:,}</td>
                <td>{row["deaths"]:,}</td>
                <td>{row["injuries"]:,}</td>
                <td>{row["malfunctions"]:,}</td>
                <td>{row["death_rate"]:.2f}%</td>
            </tr>
            ''' for _, row in mfr_df.iterrows())}
        </tbody>
    </table>
"""


def generate_manufacturer_report(
    manufacturer: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> str:
    """
    Generate a focused report for a single manufacturer.

    Args:
        manufacturer: Manufacturer name.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        HTML report string.
    """
    report_config = ReportConfig(
        title=f"MAUDE Analysis: {manufacturer}",
        subtitle="Manufacturer Safety Profile",
        manufacturers=[manufacturer],
        start_date=start_date,
        end_date=end_date,
    )

    return generate_html_report(report_config, conn)


def save_report(
    html_content: str,
    output_path: Path,
) -> Path:
    """
    Save HTML report to file.

    Args:
        html_content: HTML report content.
        output_path: Path to save the report.

    Returns:
        Path to the saved file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(f"Report saved to: {output_path}")
    return output_path
