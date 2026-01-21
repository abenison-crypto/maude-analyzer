"""Signal detection module for identifying safety trends and anomalies."""

import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection

logger = get_logger("signals")


class SignalSeverity(Enum):
    """Severity levels for detected signals."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SignalType(Enum):
    """Types of safety signals."""
    VOLUME_SPIKE = "volume_spike"
    DEATH_INCREASE = "death_increase"
    NEW_FAILURE_MODE = "new_failure_mode"
    MANUFACTURER_TREND = "manufacturer_trend"
    PRODUCT_TREND = "product_trend"
    KEYWORD_EMERGENCE = "keyword_emergence"


@dataclass
class SafetySignal:
    """A detected safety signal."""

    signal_type: SignalType
    severity: SignalSeverity
    title: str
    description: str
    detected_at: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)

    # Context
    manufacturer: Optional[str] = None
    product_code: Optional[str] = None
    period_start: Optional[date] = None
    period_end: Optional[date] = None

    # Metrics
    current_value: Optional[float] = None
    baseline_value: Optional[float] = None
    percent_change: Optional[float] = None
    z_score: Optional[float] = None


@dataclass
class SignalDetectionResult:
    """Result of signal detection analysis."""

    signals: List[SafetySignal] = field(default_factory=list)
    analysis_period: Tuple[date, date] = None
    baseline_period: Tuple[date, date] = None
    total_mdrs_analyzed: int = 0


class SignalDetector:
    """Detects safety signals from MAUDE data."""

    def __init__(
        self,
        baseline_months: int = 12,
        analysis_months: int = 3,
        volume_threshold: float = 2.0,  # z-score threshold
        death_threshold: float = 1.5,
        conn=None,
    ):
        """
        Initialize the signal detector.

        Args:
            baseline_months: Months of data for baseline calculation.
            analysis_months: Recent months to analyze for signals.
            volume_threshold: Z-score threshold for volume spikes.
            death_threshold: Z-score threshold for death rate increases.
            conn: Database connection.
        """
        self.baseline_months = baseline_months
        self.analysis_months = analysis_months
        self.volume_threshold = volume_threshold
        self.death_threshold = death_threshold
        self._conn = conn

    def _get_connection(self):
        """Get database connection."""
        if self._conn:
            return self._conn
        return get_connection()

    def detect_all_signals(
        self,
        manufacturers: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
    ) -> SignalDetectionResult:
        """
        Run all signal detection algorithms.

        Args:
            manufacturers: Filter by manufacturers.
            product_codes: Filter by product codes.

        Returns:
            SignalDetectionResult with detected signals.
        """
        result = SignalDetectionResult()
        today = date.today()

        result.analysis_period = (
            today - timedelta(days=self.analysis_months * 30),
            today,
        )
        result.baseline_period = (
            today - timedelta(days=(self.baseline_months + self.analysis_months) * 30),
            today - timedelta(days=self.analysis_months * 30),
        )

        # Detect volume spikes
        volume_signals = self.detect_volume_spikes(manufacturers, product_codes)
        result.signals.extend(volume_signals)

        # Detect death rate increases
        death_signals = self.detect_death_rate_changes(manufacturers, product_codes)
        result.signals.extend(death_signals)

        # Detect manufacturer trends
        mfr_signals = self.detect_manufacturer_trends(manufacturers, product_codes)
        result.signals.extend(mfr_signals)

        # Sort by severity
        severity_order = {
            SignalSeverity.CRITICAL: 0,
            SignalSeverity.HIGH: 1,
            SignalSeverity.MEDIUM: 2,
            SignalSeverity.LOW: 3,
        }
        result.signals.sort(key=lambda s: severity_order[s.severity])

        return result

    def detect_volume_spikes(
        self,
        manufacturers: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
    ) -> List[SafetySignal]:
        """
        Detect unusual spikes in MDR volume.

        Args:
            manufacturers: Filter by manufacturers.
            product_codes: Filter by product codes.

        Returns:
            List of volume spike signals.
        """
        signals = []
        conn = self._get_connection()

        try:
            # Get monthly counts
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

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            sql = f"""
                SELECT
                    DATE_TRUNC('month', date_received) as month,
                    manufacturer_clean,
                    COUNT(*) as count
                FROM master_events
                {where_sql}
                GROUP BY DATE_TRUNC('month', date_received), manufacturer_clean
                ORDER BY month
            """

            df = conn.execute(sql, params).fetchdf()

            if df.empty:
                return signals

            # Analyze each manufacturer
            for manufacturer in df["manufacturer_clean"].unique():
                mfr_df = df[df["manufacturer_clean"] == manufacturer].copy()
                mfr_df = mfr_df.sort_values("month")

                if len(mfr_df) < self.baseline_months + 1:
                    continue

                # Calculate baseline stats (excluding recent months)
                baseline = mfr_df.iloc[:-self.analysis_months]["count"]
                recent = mfr_df.iloc[-self.analysis_months:]["count"]

                if len(baseline) < 3:
                    continue

                baseline_mean = baseline.mean()
                baseline_std = baseline.std()

                if baseline_std == 0:
                    continue

                # Check for spikes in recent months
                for idx, row in mfr_df.iloc[-self.analysis_months:].iterrows():
                    z_score = (row["count"] - baseline_mean) / baseline_std

                    if z_score >= self.volume_threshold:
                        severity = self._get_severity_from_zscore(z_score)

                        signals.append(SafetySignal(
                            signal_type=SignalType.VOLUME_SPIKE,
                            severity=severity,
                            title=f"Volume Spike: {manufacturer}",
                            description=(
                                f"MDR volume for {manufacturer} increased significantly. "
                                f"Recent: {row['count']:.0f}, Baseline avg: {baseline_mean:.1f}"
                            ),
                            manufacturer=manufacturer,
                            period_start=row["month"].date() if hasattr(row["month"], "date") else row["month"],
                            current_value=row["count"],
                            baseline_value=baseline_mean,
                            percent_change=((row["count"] - baseline_mean) / baseline_mean * 100) if baseline_mean > 0 else 0,
                            z_score=z_score,
                        ))

        except Exception as e:
            logger.error(f"Error detecting volume spikes: {e}")

        return signals

    def detect_death_rate_changes(
        self,
        manufacturers: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
    ) -> List[SafetySignal]:
        """
        Detect increases in death rates.

        Args:
            manufacturers: Filter by manufacturers.
            product_codes: Filter by product codes.

        Returns:
            List of death rate signals.
        """
        signals = []
        conn = self._get_connection()

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

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            sql = f"""
                SELECT
                    DATE_TRUNC('month', date_received) as month,
                    manufacturer_clean,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE event_type = 'D') as deaths
                FROM master_events
                {where_sql}
                GROUP BY DATE_TRUNC('month', date_received), manufacturer_clean
                ORDER BY month
            """

            df = conn.execute(sql, params).fetchdf()

            if df.empty:
                return signals

            df["death_rate"] = df["deaths"] / df["total"] * 100

            # Analyze each manufacturer
            for manufacturer in df["manufacturer_clean"].unique():
                mfr_df = df[df["manufacturer_clean"] == manufacturer].copy()
                mfr_df = mfr_df.sort_values("month")

                if len(mfr_df) < self.baseline_months + 1:
                    continue

                # Calculate baseline death rate
                baseline = mfr_df.iloc[:-self.analysis_months]["death_rate"]
                recent = mfr_df.iloc[-self.analysis_months:]["death_rate"]

                if len(baseline) < 3:
                    continue

                baseline_mean = baseline.mean()
                baseline_std = baseline.std()

                if baseline_std == 0 or baseline_mean == 0:
                    continue

                # Check recent death rates
                recent_mean = recent.mean()
                z_score = (recent_mean - baseline_mean) / baseline_std

                if z_score >= self.death_threshold:
                    severity = SignalSeverity.HIGH if z_score >= 2.5 else SignalSeverity.MEDIUM

                    # Death signals are always at least MEDIUM severity
                    if recent_mean > 5:  # >5% death rate
                        severity = SignalSeverity.CRITICAL

                    signals.append(SafetySignal(
                        signal_type=SignalType.DEATH_INCREASE,
                        severity=severity,
                        title=f"Death Rate Increase: {manufacturer}",
                        description=(
                            f"Death rate for {manufacturer} has increased. "
                            f"Recent: {recent_mean:.2f}%, Baseline: {baseline_mean:.2f}%"
                        ),
                        manufacturer=manufacturer,
                        current_value=recent_mean,
                        baseline_value=baseline_mean,
                        percent_change=((recent_mean - baseline_mean) / baseline_mean * 100) if baseline_mean > 0 else 0,
                        z_score=z_score,
                    ))

        except Exception as e:
            logger.error(f"Error detecting death rate changes: {e}")

        return signals

    def detect_manufacturer_trends(
        self,
        manufacturers: Optional[List[str]] = None,
        product_codes: Optional[List[str]] = None,
    ) -> List[SafetySignal]:
        """
        Detect trending issues for manufacturers.

        Args:
            manufacturers: Filter by manufacturers.
            product_codes: Filter by product codes.

        Returns:
            List of manufacturer trend signals.
        """
        signals = []
        conn = self._get_connection()

        try:
            # Calculate year-over-year change
            today = date.today()
            current_year_start = date(today.year, 1, 1)
            prev_year_start = date(today.year - 1, 1, 1)
            prev_year_end = date(today.year - 1, 12, 31)

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

            base_where = " AND ".join(where_clauses) if where_clauses else "1=1"

            sql = f"""
                WITH current_year AS (
                    SELECT manufacturer_clean, COUNT(*) as count
                    FROM master_events
                    WHERE date_received >= ? AND {base_where}
                    GROUP BY manufacturer_clean
                ),
                prev_year AS (
                    SELECT manufacturer_clean, COUNT(*) as count
                    FROM master_events
                    WHERE date_received >= ? AND date_received <= ? AND {base_where}
                    GROUP BY manufacturer_clean
                )
                SELECT
                    COALESCE(c.manufacturer_clean, p.manufacturer_clean) as manufacturer,
                    COALESCE(c.count, 0) as current_count,
                    COALESCE(p.count, 0) as prev_count
                FROM current_year c
                FULL OUTER JOIN prev_year p ON c.manufacturer_clean = p.manufacturer_clean
            """

            full_params = [current_year_start] + params + [prev_year_start, prev_year_end] + params
            df = conn.execute(sql, full_params).fetchdf()

            if df.empty:
                return signals

            # Calculate days elapsed in current year for normalization
            days_elapsed = (today - current_year_start).days + 1
            days_in_prev_year = 365

            for _, row in df.iterrows():
                if row["prev_count"] < 10:  # Skip low-volume manufacturers
                    continue

                # Normalize current year to full year equivalent
                current_normalized = row["current_count"] * (days_in_prev_year / days_elapsed)
                prev_count = row["prev_count"]

                if prev_count > 0:
                    yoy_change = (current_normalized - prev_count) / prev_count * 100

                    if yoy_change >= 50:  # 50%+ YoY increase
                        severity = SignalSeverity.HIGH if yoy_change >= 100 else SignalSeverity.MEDIUM

                        signals.append(SafetySignal(
                            signal_type=SignalType.MANUFACTURER_TREND,
                            severity=severity,
                            title=f"YoY Increase: {row['manufacturer']}",
                            description=(
                                f"MDRs for {row['manufacturer']} are trending "
                                f"{yoy_change:.0f}% higher than last year (annualized)."
                            ),
                            manufacturer=row["manufacturer"],
                            current_value=current_normalized,
                            baseline_value=prev_count,
                            percent_change=yoy_change,
                            data={
                                "current_ytd": row["current_count"],
                                "prev_year_total": prev_count,
                                "days_elapsed": days_elapsed,
                            }
                        ))

        except Exception as e:
            logger.error(f"Error detecting manufacturer trends: {e}")

        return signals

    def _get_severity_from_zscore(self, z_score: float) -> SignalSeverity:
        """Determine severity level from z-score."""
        if z_score >= 4:
            return SignalSeverity.CRITICAL
        elif z_score >= 3:
            return SignalSeverity.HIGH
        elif z_score >= 2:
            return SignalSeverity.MEDIUM
        else:
            return SignalSeverity.LOW


def get_monthly_summary(
    months: int = 12,
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    conn=None,
) -> pd.DataFrame:
    """
    Get monthly summary statistics for trend analysis.

    Args:
        months: Number of months to include.
        manufacturers: Filter by manufacturers.
        product_codes: Filter by product codes.
        conn: Database connection.

    Returns:
        DataFrame with monthly statistics.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        params = []
        where_clauses = [f"date_received >= CURRENT_DATE - INTERVAL '{months} months'"]

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"product_code IN ({placeholders})")
            params.extend(product_codes)

        where_sql = "WHERE " + " AND ".join(where_clauses)

        sql = f"""
            SELECT
                DATE_TRUNC('month', date_received) as month,
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions,
                COUNT(DISTINCT manufacturer_clean) as manufacturers
            FROM master_events
            {where_sql}
            GROUP BY DATE_TRUNC('month', date_received)
            ORDER BY month
        """

        return conn.execute(sql, params).fetchdf()

    finally:
        if own_conn:
            conn.close()


def detect_signals(
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
) -> SignalDetectionResult:
    """
    Convenience function to detect all safety signals.

    Args:
        manufacturers: Filter by manufacturers.
        product_codes: Filter by product codes.

    Returns:
        SignalDetectionResult with detected signals.
    """
    detector = SignalDetector()
    return detector.detect_all_signals(manufacturers, product_codes)
