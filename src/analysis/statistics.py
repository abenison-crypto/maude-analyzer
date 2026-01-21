"""Statistical analysis functions for MAUDE data."""

import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection

logger = get_logger("statistics")


@dataclass
class ComparisonResult:
    """Result of a statistical comparison."""

    group_a: str
    group_b: str
    metric: str
    value_a: float
    value_b: float
    difference: float
    percent_difference: float
    ratio: float
    significant: bool
    p_value: Optional[float] = None
    confidence_interval: Optional[Tuple[float, float]] = None


@dataclass
class TrendAnalysis:
    """Result of trend analysis."""

    metric: str
    slope: float
    intercept: float
    r_squared: float
    trend_direction: str  # "increasing", "decreasing", "stable"
    percent_change: float
    data_points: int


def calculate_proportion_confidence_interval(
    successes: int,
    trials: int,
    confidence: float = 0.95,
) -> Tuple[float, float]:
    """
    Calculate confidence interval for a proportion using Wilson score interval.

    Args:
        successes: Number of successes (e.g., deaths).
        trials: Total number of trials (e.g., total MDRs).
        confidence: Confidence level (default 0.95 for 95% CI).

    Returns:
        Tuple of (lower_bound, upper_bound).
    """
    if trials == 0:
        return (0.0, 0.0)

    p = successes / trials
    n = trials

    # Z-score for confidence level
    z = 1.96 if confidence == 0.95 else 2.576 if confidence == 0.99 else 1.645

    # Wilson score interval
    denominator = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denominator
    spread = z * math.sqrt((p * (1 - p) + z**2 / (4 * n)) / n) / denominator

    lower = max(0, center - spread)
    upper = min(1, center + spread)

    return (lower * 100, upper * 100)  # Return as percentages


def compare_proportions(
    successes_a: int,
    trials_a: int,
    successes_b: int,
    trials_b: int,
) -> Dict[str, Any]:
    """
    Compare two proportions using a two-proportion z-test.

    Args:
        successes_a: Successes in group A.
        trials_a: Total trials in group A.
        successes_b: Successes in group B.
        trials_b: Total trials in group B.

    Returns:
        Dictionary with test results.
    """
    if trials_a == 0 or trials_b == 0:
        return {
            "p_a": 0,
            "p_b": 0,
            "difference": 0,
            "z_score": 0,
            "p_value": 1.0,
            "significant": False,
        }

    p_a = successes_a / trials_a
    p_b = successes_b / trials_b

    # Pooled proportion
    p_pooled = (successes_a + successes_b) / (trials_a + trials_b)

    # Standard error
    se = math.sqrt(p_pooled * (1 - p_pooled) * (1 / trials_a + 1 / trials_b))

    if se == 0:
        return {
            "p_a": p_a * 100,
            "p_b": p_b * 100,
            "difference": (p_a - p_b) * 100,
            "z_score": 0,
            "p_value": 1.0,
            "significant": False,
        }

    # Z-score
    z = (p_a - p_b) / se

    # P-value (two-tailed) using normal approximation
    p_value = 2 * (1 - _normal_cdf(abs(z)))

    return {
        "p_a": p_a * 100,
        "p_b": p_b * 100,
        "difference": (p_a - p_b) * 100,
        "z_score": z,
        "p_value": p_value,
        "significant": p_value < 0.05,
    }


def _normal_cdf(x: float) -> float:
    """Approximate the cumulative distribution function of standard normal."""
    return (1 + math.erf(x / math.sqrt(2))) / 2


def compare_manufacturers(
    manufacturer_a: str,
    manufacturer_b: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> Dict[str, ComparisonResult]:
    """
    Compare two manufacturers across multiple metrics.

    Args:
        manufacturer_a: First manufacturer name.
        manufacturer_b: Second manufacturer name.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        Dictionary of ComparisonResult by metric name.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        params = [manufacturer_a, manufacturer_b]
        date_filter = ""

        if start_date:
            date_filter += " AND date_received >= ?"
            params.append(start_date)
        if end_date:
            date_filter += " AND date_received <= ?"
            params.append(end_date)

        sql = f"""
            SELECT
                manufacturer_clean,
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions
            FROM master_events
            WHERE manufacturer_clean IN (?, ?){date_filter}
            GROUP BY manufacturer_clean
        """

        df = conn.execute(sql, params).fetchdf()

        if df.empty or len(df) < 2:
            return {}

        # Get data for each manufacturer
        data_a = df[df["manufacturer_clean"] == manufacturer_a].iloc[0]
        data_b = df[df["manufacturer_clean"] == manufacturer_b].iloc[0]

        results = {}

        # Total MDRs comparison
        results["total_mdrs"] = _create_comparison(
            manufacturer_a, manufacturer_b, "Total MDRs",
            data_a["total_mdrs"], data_b["total_mdrs"]
        )

        # Death rate comparison
        death_test = compare_proportions(
            int(data_a["deaths"]), int(data_a["total_mdrs"]),
            int(data_b["deaths"]), int(data_b["total_mdrs"]),
        )

        results["death_rate"] = ComparisonResult(
            group_a=manufacturer_a,
            group_b=manufacturer_b,
            metric="Death Rate (%)",
            value_a=death_test["p_a"],
            value_b=death_test["p_b"],
            difference=death_test["difference"],
            percent_difference=death_test["difference"],
            ratio=death_test["p_a"] / death_test["p_b"] if death_test["p_b"] > 0 else 0,
            significant=death_test["significant"],
            p_value=death_test["p_value"],
            confidence_interval=calculate_proportion_confidence_interval(
                int(data_a["deaths"]), int(data_a["total_mdrs"])
            ),
        )

        # Injury rate comparison
        injury_test = compare_proportions(
            int(data_a["injuries"]), int(data_a["total_mdrs"]),
            int(data_b["injuries"]), int(data_b["total_mdrs"]),
        )

        results["injury_rate"] = ComparisonResult(
            group_a=manufacturer_a,
            group_b=manufacturer_b,
            metric="Injury Rate (%)",
            value_a=injury_test["p_a"],
            value_b=injury_test["p_b"],
            difference=injury_test["difference"],
            percent_difference=injury_test["difference"],
            ratio=injury_test["p_a"] / injury_test["p_b"] if injury_test["p_b"] > 0 else 0,
            significant=injury_test["significant"],
            p_value=injury_test["p_value"],
        )

        # Malfunction rate comparison
        malf_test = compare_proportions(
            int(data_a["malfunctions"]), int(data_a["total_mdrs"]),
            int(data_b["malfunctions"]), int(data_b["total_mdrs"]),
        )

        results["malfunction_rate"] = ComparisonResult(
            group_a=manufacturer_a,
            group_b=manufacturer_b,
            metric="Malfunction Rate (%)",
            value_a=malf_test["p_a"],
            value_b=malf_test["p_b"],
            difference=malf_test["difference"],
            percent_difference=malf_test["difference"],
            ratio=malf_test["p_a"] / malf_test["p_b"] if malf_test["p_b"] > 0 else 0,
            significant=malf_test["significant"],
            p_value=malf_test["p_value"],
        )

        return results

    finally:
        if own_conn:
            conn.close()


def _create_comparison(
    group_a: str,
    group_b: str,
    metric: str,
    value_a: float,
    value_b: float,
) -> ComparisonResult:
    """Create a simple comparison result."""
    difference = value_a - value_b
    percent_diff = (difference / value_b * 100) if value_b > 0 else 0
    ratio = value_a / value_b if value_b > 0 else 0

    return ComparisonResult(
        group_a=group_a,
        group_b=group_b,
        metric=metric,
        value_a=value_a,
        value_b=value_b,
        difference=difference,
        percent_difference=percent_diff,
        ratio=ratio,
        significant=False,  # Not a statistical test for counts
    )


def analyze_trend(
    data: pd.DataFrame,
    x_column: str,
    y_column: str,
) -> TrendAnalysis:
    """
    Analyze trend using simple linear regression.

    Args:
        data: DataFrame with trend data.
        x_column: Column name for x-axis (typically time).
        y_column: Column name for y-axis (metric).

    Returns:
        TrendAnalysis with slope, direction, etc.
    """
    if data.empty or len(data) < 2:
        return TrendAnalysis(
            metric=y_column,
            slope=0,
            intercept=0,
            r_squared=0,
            trend_direction="stable",
            percent_change=0,
            data_points=len(data),
        )

    # Convert to numeric indices
    x = np.arange(len(data))
    y = data[y_column].values.astype(float)

    # Simple linear regression
    n = len(x)
    x_mean = np.mean(x)
    y_mean = np.mean(y)

    numerator = np.sum((x - x_mean) * (y - y_mean))
    denominator = np.sum((x - x_mean) ** 2)

    if denominator == 0:
        slope = 0
    else:
        slope = numerator / denominator

    intercept = y_mean - slope * x_mean

    # R-squared
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)

    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    # Determine trend direction
    if slope > 0.1 * y_mean / n:  # Significant positive trend
        direction = "increasing"
    elif slope < -0.1 * y_mean / n:  # Significant negative trend
        direction = "decreasing"
    else:
        direction = "stable"

    # Calculate percent change from first to last
    first_val = y[0] if y[0] > 0 else 1
    last_val = y[-1]
    percent_change = (last_val - first_val) / first_val * 100

    return TrendAnalysis(
        metric=y_column,
        slope=slope,
        intercept=intercept,
        r_squared=r_squared,
        trend_direction=direction,
        percent_change=percent_change,
        data_points=n,
    )


def get_summary_statistics(
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> Dict[str, Any]:
    """
    Get comprehensive summary statistics.

    Args:
        manufacturers: Filter by manufacturers.
        product_codes: Filter by product codes.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        Dictionary with summary statistics.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        params = []
        where_clauses = []

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"product_code IN ({placeholders})")
            params.extend(product_codes)

        if start_date:
            where_clauses.append("date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions,
                COUNT(DISTINCT manufacturer_clean) as unique_manufacturers,
                COUNT(DISTINCT product_code) as unique_products,
                MIN(date_received) as min_date,
                MAX(date_received) as max_date
            FROM master_events
            {where_sql}
        """

        result = conn.execute(sql, params).fetchone()

        total = result[0] or 0

        stats = {
            "total_mdrs": total,
            "deaths": result[1] or 0,
            "injuries": result[2] or 0,
            "malfunctions": result[3] or 0,
            "unique_manufacturers": result[4] or 0,
            "unique_products": result[5] or 0,
            "date_range": {
                "min": result[6],
                "max": result[7],
            },
        }

        # Calculate rates
        if total > 0:
            stats["death_rate"] = round(stats["deaths"] / total * 100, 2)
            stats["injury_rate"] = round(stats["injuries"] / total * 100, 2)
            stats["malfunction_rate"] = round(stats["malfunctions"] / total * 100, 2)

            # Confidence intervals for rates
            stats["death_rate_ci"] = calculate_proportion_confidence_interval(
                stats["deaths"], total
            )
            stats["injury_rate_ci"] = calculate_proportion_confidence_interval(
                stats["injuries"], total
            )
        else:
            stats["death_rate"] = 0
            stats["injury_rate"] = 0
            stats["malfunction_rate"] = 0

        return stats

    finally:
        if own_conn:
            conn.close()


def rank_manufacturers_by_metric(
    metric: str = "death_rate",
    min_mdrs: int = 10,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn=None,
) -> pd.DataFrame:
    """
    Rank manufacturers by a specific metric.

    Args:
        metric: Metric to rank by (death_rate, injury_rate, malfunction_rate, total_mdrs).
        min_mdrs: Minimum MDRs required for inclusion.
        start_date: Start date filter.
        end_date: End date filter.
        conn: Database connection.

    Returns:
        DataFrame with ranked manufacturers.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        params = []
        where_clauses = []

        if start_date:
            where_clauses.append("date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT
                manufacturer_clean,
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions,
                ROUND(COUNT(*) FILTER (WHERE event_type = 'D') * 100.0 / COUNT(*), 2) as death_rate,
                ROUND(COUNT(*) FILTER (WHERE event_type = 'IN') * 100.0 / COUNT(*), 2) as injury_rate,
                ROUND(COUNT(*) FILTER (WHERE event_type = 'M') * 100.0 / COUNT(*), 2) as malfunction_rate
            FROM master_events
            {where_sql}
            GROUP BY manufacturer_clean
            HAVING COUNT(*) >= {min_mdrs}
            ORDER BY {metric} DESC
        """

        df = conn.execute(sql, params).fetchdf()

        # Add rank
        df["rank"] = range(1, len(df) + 1)

        return df

    finally:
        if own_conn:
            conn.close()
