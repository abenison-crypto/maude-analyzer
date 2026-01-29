"""Signal detection service for advanced safety signal analysis."""

import math
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional

from api.services.database import get_db
from api.models.signal_schemas import (
    SignalMethod,
    DrillDownLevel,
    TimeComparisonMode,
    TimeComparisonConfig,
    ComparisonPopulation,
    SignalRequest,
    SignalResponse,
    SignalResult,
    MethodResult,
    TimeInfo,
    DisproportionalityResult,
)


class SignalDetectionService:
    """Service for detecting safety signals using multiple methods."""

    # Column mappings for drill-down levels
    LEVEL_COLUMNS = {
        DrillDownLevel.MANUFACTURER: "m.manufacturer_clean",
        DrillDownLevel.BRAND: "d.brand_name",
        DrillDownLevel.GENERIC: "d.generic_name",
        DrillDownLevel.MODEL: "d.model_number",
    }

    # Parent filter columns
    PARENT_COLUMNS = {
        DrillDownLevel.BRAND: "m.manufacturer_clean",
        DrillDownLevel.GENERIC: "d.brand_name",
        DrillDownLevel.MODEL: "d.generic_name",
    }

    # Child levels
    CHILD_LEVELS = {
        DrillDownLevel.MANUFACTURER: DrillDownLevel.BRAND,
        DrillDownLevel.BRAND: DrillDownLevel.GENERIC,
        DrillDownLevel.GENERIC: DrillDownLevel.MODEL,
        DrillDownLevel.MODEL: None,
    }

    def __init__(self):
        self.db = get_db()

    def detect_signals(self, request: SignalRequest) -> SignalResponse:
        """Main entry point for signal detection."""
        # Determine time range
        time_info = self._resolve_time_range(request.time_config)

        # Get base entity data
        entities = self._get_entity_data(request, time_info)

        if not entities:
            return SignalResponse(
                level=request.level,
                parent_value=request.parent_value,
                methods_applied=request.methods,
                time_info=time_info,
                signals=[],
                total_entities_analyzed=0,
                high_signal_count=0,
                elevated_signal_count=0,
                normal_count=0,
                data_note="No data available for the specified criteria",
            )

        # Calculate signals for each method
        results = []
        for entity_data in entities:
            method_results = []

            for method in request.methods:
                result = self._calculate_method(method, entity_data, request, time_info)
                method_results.append(result)

            # Determine overall signal type (highest severity across all methods)
            signal_type = self._determine_overall_signal(method_results)

            # Check for children
            child_level = self.CHILD_LEVELS.get(request.level)
            has_children = child_level is not None and self._has_children(
                entity_data["entity"], request.level, child_level
            )

            results.append(SignalResult(
                entity=entity_data["entity"],
                entity_level=request.level,
                total_events=entity_data["total_events"],
                deaths=entity_data["deaths"],
                injuries=entity_data["injuries"],
                malfunctions=entity_data["malfunctions"],
                current_period_events=entity_data.get("current_period_events"),
                comparison_period_events=entity_data.get("comparison_period_events"),
                change_pct=entity_data.get("change_pct"),
                method_results=method_results,
                signal_type=signal_type,
                has_children=has_children,
                child_level=child_level,
            ))

        # Sort by signal severity and z-score
        results.sort(key=lambda x: (
            0 if x.signal_type == "high" else 1 if x.signal_type == "elevated" else 2,
            -(next((r.value or 0 for r in x.method_results if r.method == SignalMethod.ZSCORE), 0))
        ))

        # Limit results
        results = results[:request.limit]

        # Count signals
        high_count = sum(1 for r in results if r.signal_type == "high")
        elevated_count = sum(1 for r in results if r.signal_type == "elevated")
        normal_count = sum(1 for r in results if r.signal_type == "normal")

        return SignalResponse(
            level=request.level,
            parent_value=request.parent_value,
            methods_applied=request.methods,
            time_info=time_info,
            signals=results,
            total_entities_analyzed=len(entities),
            high_signal_count=high_count,
            elevated_signal_count=elevated_count,
            normal_count=normal_count,
        )

    def _resolve_time_range(self, config: TimeComparisonConfig) -> TimeInfo:
        """Resolve time configuration to concrete dates."""
        # Get max date from database
        result = self.db.fetch_one("""
            SELECT MAX(date_received) FROM master_events
            WHERE manufacturer_clean IS NOT NULL
        """)
        max_date = result[0] if result and result[0] else date.today()

        # Note: 2020-2021 have limited data due to FDA ingestion gap, but 2022+ has full data
        # We use the actual max date from events with manufacturer data (already queried above)

        if config.mode == TimeComparisonMode.LOOKBACK:
            end_date = max_date
            start_date = end_date - relativedelta(months=config.lookback_months)
            return TimeInfo(
                mode=config.mode,
                analysis_start=start_date,
                analysis_end=end_date,
            )

        elif config.mode == TimeComparisonMode.CUSTOM:
            if config.period_a and config.period_b:
                return TimeInfo(
                    mode=config.mode,
                    analysis_start=config.period_a.start_date,
                    analysis_end=config.period_a.end_date,
                    comparison_start=config.period_b.start_date,
                    comparison_end=config.period_b.end_date,
                )
            # Fallback to lookback if periods not specified
            end_date = max_date
            start_date = end_date - relativedelta(months=config.lookback_months)
            return TimeInfo(mode=config.mode, analysis_start=start_date, analysis_end=end_date)

        elif config.mode == TimeComparisonMode.YOY:
            current_year = config.current_year or max_date.year
            comparison_year = config.comparison_year or (current_year - 1)

            if config.quarter:
                q_start_month = (config.quarter - 1) * 3 + 1
                q_end_month = config.quarter * 3
                analysis_start = date(current_year, q_start_month, 1)
                analysis_end = date(current_year, q_end_month, 1) + relativedelta(months=1) - timedelta(days=1)
                comparison_start = date(comparison_year, q_start_month, 1)
                comparison_end = date(comparison_year, q_end_month, 1) + relativedelta(months=1) - timedelta(days=1)
            else:
                analysis_start = date(current_year, 1, 1)
                analysis_end = date(current_year, 12, 31)
                comparison_start = date(comparison_year, 1, 1)
                comparison_end = date(comparison_year, 12, 31)

            return TimeInfo(
                mode=config.mode,
                analysis_start=analysis_start,
                analysis_end=analysis_end,
                comparison_start=comparison_start,
                comparison_end=comparison_end,
            )

        elif config.mode == TimeComparisonMode.ROLLING:
            end_date = max_date
            start_date = end_date - relativedelta(months=config.lookback_months)
            return TimeInfo(
                mode=config.mode,
                analysis_start=start_date,
                analysis_end=end_date,
                rolling_window=config.rolling_window_months,
            )

        # Default fallback
        end_date = max_date
        start_date = end_date - relativedelta(months=12)
        return TimeInfo(mode=TimeComparisonMode.LOOKBACK, analysis_start=start_date, analysis_end=end_date)

    def _get_entity_data(self, request: SignalRequest, time_info: TimeInfo) -> list[dict]:
        """Get aggregated data for entities at the specified level."""
        level_col = self.LEVEL_COLUMNS[request.level]
        needs_device_join = request.level != DrillDownLevel.MANUFACTURER

        # Determine which month to use for comparison (latest or specified)
        comparison_month = request.time_config.comparison_month

        # Build conditions
        conditions = [
            f"m.date_received >= ?",
            f"m.date_received <= ?",
            f"{level_col} IS NOT NULL",
        ]
        params = [time_info.analysis_start.isoformat(), time_info.analysis_end.isoformat()]

        # Parent filter for drill-down
        if request.parent_value and request.level != DrillDownLevel.MANUFACTURER:
            parent_col = self.PARENT_COLUMNS.get(request.level)
            if parent_col:
                conditions.append(f"{parent_col} = ?")
                params.append(request.parent_value)

        # Product code filter
        if request.product_codes:
            placeholders = ", ".join(["?" for _ in request.product_codes])
            conditions.append(f"m.product_code IN ({placeholders})")
            params.extend(request.product_codes)

        # Event type filter
        if request.event_types:
            from api.constants.columns import EVENT_TYPE_FILTER_MAPPING
            db_types = [EVENT_TYPE_FILTER_MAPPING.get(t, t) for t in request.event_types]
            placeholders = ", ".join(["?" for _ in db_types])
            conditions.append(f"m.event_type IN ({placeholders})")
            params.extend(db_types)

        where_clause = " AND ".join(conditions)

        # Build query with monthly breakdown for z-score calculation
        # If comparison_month is specified, use it; otherwise use max_month
        comparison_month_condition = (
            f"mc.month = DATE_TRUNC('month', DATE '{comparison_month.isoformat()}')"
            if comparison_month
            else "mc.month = es.max_month"
        )

        if needs_device_join:
            query = f"""
                WITH monthly_counts AS (
                    SELECT
                        {level_col} as entity,
                        DATE_TRUNC('month', m.date_received) as month,
                        COUNT(DISTINCT m.mdr_report_key) as event_count,
                        COUNT(DISTINCT CASE WHEN m.event_type = 'D' THEN m.mdr_report_key END) as death_count,
                        COUNT(DISTINCT CASE WHEN m.event_type = 'IN' THEN m.mdr_report_key END) as injury_count,
                        COUNT(DISTINCT CASE WHEN m.event_type = 'M' THEN m.mdr_report_key END) as malfunction_count
                    FROM master_events m
                    JOIN devices d ON d.mdr_report_key = m.mdr_report_key
                    WHERE {where_clause}
                    GROUP BY {level_col}, DATE_TRUNC('month', m.date_received)
                ),
                entity_stats AS (
                    SELECT
                        entity,
                        SUM(event_count) as total_events,
                        SUM(death_count) as deaths,
                        SUM(injury_count) as injuries,
                        SUM(malfunction_count) as malfunctions,
                        AVG(event_count) as avg_monthly,
                        STDDEV_SAMP(event_count) as std_monthly,
                        MAX(month) as max_month
                    FROM monthly_counts
                    GROUP BY entity
                    HAVING SUM(event_count) >= ?
                ),
                latest_counts AS (
                    SELECT mc.entity, mc.event_count as latest_month_events
                    FROM monthly_counts mc
                    JOIN entity_stats es ON mc.entity = es.entity AND {comparison_month_condition}
                )
                SELECT
                    es.entity,
                    es.total_events,
                    es.deaths,
                    es.injuries,
                    es.malfunctions,
                    ROUND(es.avg_monthly, 2) as avg_monthly,
                    ROUND(COALESCE(es.std_monthly, 0), 2) as std_monthly,
                    COALESCE(lc.latest_month_events, 0) as latest_month_events
                FROM entity_stats es
                LEFT JOIN latest_counts lc ON es.entity = lc.entity
                ORDER BY es.total_events DESC
            """
        else:
            query = f"""
                WITH monthly_counts AS (
                    SELECT
                        m.manufacturer_clean as entity,
                        DATE_TRUNC('month', m.date_received) as month,
                        COUNT(DISTINCT m.mdr_report_key) as event_count,
                        COUNT(DISTINCT CASE WHEN m.event_type = 'D' THEN m.mdr_report_key END) as death_count,
                        COUNT(DISTINCT CASE WHEN m.event_type = 'IN' THEN m.mdr_report_key END) as injury_count,
                        COUNT(DISTINCT CASE WHEN m.event_type = 'M' THEN m.mdr_report_key END) as malfunction_count
                    FROM master_events m
                    WHERE {where_clause}
                    GROUP BY m.manufacturer_clean, DATE_TRUNC('month', m.date_received)
                ),
                entity_stats AS (
                    SELECT
                        entity,
                        SUM(event_count) as total_events,
                        SUM(death_count) as deaths,
                        SUM(injury_count) as injuries,
                        SUM(malfunction_count) as malfunctions,
                        AVG(event_count) as avg_monthly,
                        STDDEV_SAMP(event_count) as std_monthly,
                        MAX(month) as max_month
                    FROM monthly_counts
                    GROUP BY entity
                    HAVING SUM(event_count) >= ?
                ),
                latest_counts AS (
                    SELECT mc.entity, mc.event_count as latest_month_events
                    FROM monthly_counts mc
                    JOIN entity_stats es ON mc.entity = es.entity AND {comparison_month_condition}
                )
                SELECT
                    es.entity,
                    es.total_events,
                    es.deaths,
                    es.injuries,
                    es.malfunctions,
                    ROUND(es.avg_monthly, 2) as avg_monthly,
                    ROUND(COALESCE(es.std_monthly, 0), 2) as std_monthly,
                    COALESCE(lc.latest_month_events, 0) as latest_month_events
                FROM entity_stats es
                LEFT JOIN latest_counts lc ON es.entity = lc.entity
                ORDER BY es.total_events DESC
            """

        params.append(request.min_events)
        results = self.db.fetch_all(query, params)

        entities = []
        for row in results:
            entities.append({
                "entity": row[0],
                "total_events": int(row[1]) if row[1] else 0,
                "deaths": int(row[2]) if row[2] else 0,
                "injuries": int(row[3]) if row[3] else 0,
                "malfunctions": int(row[4]) if row[4] else 0,
                "avg_monthly": float(row[5]) if row[5] else 0.0,
                "std_monthly": float(row[6]) if row[6] else 0.0,
                "latest_month_events": int(row[7]) if row[7] else 0,
            })

        # Get comparison period data if needed
        if time_info.comparison_start and time_info.comparison_end:
            self._add_comparison_data(entities, request, time_info)

        return entities

    def _add_comparison_data(self, entities: list[dict], request: SignalRequest, time_info: TimeInfo):
        """Add comparison period event counts for YoY and custom comparisons."""
        if not time_info.comparison_start or not time_info.comparison_end:
            return

        level_col = self.LEVEL_COLUMNS[request.level]
        needs_device_join = request.level != DrillDownLevel.MANUFACTURER

        entity_names = [e["entity"] for e in entities]
        if not entity_names:
            return

        placeholders = ", ".join(["?" for _ in entity_names])

        if needs_device_join:
            query = f"""
                SELECT {level_col} as entity, COUNT(DISTINCT m.mdr_report_key) as events
                FROM master_events m
                JOIN devices d ON d.mdr_report_key = m.mdr_report_key
                WHERE m.date_received >= ? AND m.date_received <= ?
                AND {level_col} IN ({placeholders})
                GROUP BY {level_col}
            """
        else:
            query = f"""
                SELECT m.manufacturer_clean as entity, COUNT(DISTINCT m.mdr_report_key) as events
                FROM master_events m
                WHERE m.date_received >= ? AND m.date_received <= ?
                AND m.manufacturer_clean IN ({placeholders})
                GROUP BY m.manufacturer_clean
            """

        params = [time_info.comparison_start.isoformat(), time_info.comparison_end.isoformat()]
        params.extend(entity_names)

        results = self.db.fetch_all(query, params)
        comparison_map = {row[0]: int(row[1]) for row in results}

        for entity in entities:
            entity["current_period_events"] = entity["total_events"]
            comp_events = comparison_map.get(entity["entity"], 0)
            entity["comparison_period_events"] = comp_events
            if comp_events > 0:
                entity["change_pct"] = round((entity["total_events"] - comp_events) * 100.0 / comp_events, 1)
            else:
                entity["change_pct"] = None

    def _calculate_method(
        self, method: SignalMethod, entity_data: dict, request: SignalRequest, time_info: TimeInfo
    ) -> MethodResult:
        """Calculate signal for a specific method."""
        if method == SignalMethod.ZSCORE:
            return self._calculate_zscore(entity_data, request, time_info)
        elif method == SignalMethod.PRR:
            return self._calculate_prr(entity_data, request, time_info)
        elif method == SignalMethod.ROR:
            return self._calculate_ror(entity_data, request, time_info)
        elif method == SignalMethod.EBGM:
            return self._calculate_ebgm(entity_data, request, time_info)
        elif method == SignalMethod.CUSUM:
            return self._calculate_cusum(entity_data, request, time_info)
        elif method == SignalMethod.YOY:
            return self._calculate_yoy(entity_data, request)
        elif method == SignalMethod.POP:
            return self._calculate_pop(entity_data, request)
        elif method == SignalMethod.ROLLING:
            return self._calculate_rolling(entity_data, request, time_info)
        else:
            return MethodResult(method=method, value=None, is_signal=False)

    def _calculate_zscore(self, entity_data: dict, request: SignalRequest, time_info: TimeInfo = None) -> MethodResult:
        """Calculate Z-score: (latest - avg) / stddev."""
        avg = entity_data.get("avg_monthly", 0)
        std = entity_data.get("std_monthly", 0)
        latest = entity_data.get("latest_month_events", 0)

        if std > 0:
            z_score = round((latest - avg) / std, 2)
        else:
            z_score = 0.0

        if z_score > request.zscore_high_threshold:
            strength = "high"
            is_signal = True
        elif z_score > request.zscore_elevated_threshold:
            strength = "elevated"
            is_signal = True
        else:
            strength = "normal"
            is_signal = False

        # Get monthly series for visualization (full lookback period)
        monthly_series = None
        if time_info:
            monthly_data = self._get_monthly_series(entity_data["entity"], request, time_info)
            if monthly_data:
                monthly_series = [
                    {"month": m["month"].isoformat() if hasattr(m["month"], 'isoformat') else str(m["month"]), "count": m["count"]}
                    for m in monthly_data  # Return full lookback period
                ]

        return MethodResult(
            method=SignalMethod.ZSCORE,
            value=z_score,
            is_signal=is_signal,
            signal_strength=strength,
            details={
                "avg_monthly": avg,
                "std_monthly": std,
                "latest_month": latest,
                "monthly_series": monthly_series,
            },
        )

    def _calculate_prr(self, entity_data: dict, request: SignalRequest, time_info: TimeInfo) -> MethodResult:
        """Calculate Proportional Reporting Ratio with 95% CI."""
        disprop = self._get_disproportionality_counts(entity_data, request, time_info, target_type="D")

        if disprop["a"] < 3 or (disprop["a"] + disprop["b"]) == 0 or (disprop["c"] + disprop["d"]) == 0:
            return MethodResult(method=SignalMethod.PRR, value=None, is_signal=False, signal_strength="normal")

        a, b, c, d = disprop["a"], disprop["b"], disprop["c"], disprop["d"]

        # PRR = [a/(a+b)] / [c/(c+d)]
        p_entity = a / (a + b)
        p_others = c / (c + d) if (c + d) > 0 else 0

        if p_others == 0:
            return MethodResult(method=SignalMethod.PRR, value=None, is_signal=False, signal_strength="normal")

        prr = p_entity / p_others

        # 95% CI using log transformation
        # SE(log(PRR)) = sqrt(1/a - 1/(a+b) + 1/c - 1/(c+d))
        try:
            se_log = math.sqrt(1/a - 1/(a+b) + 1/c - 1/(c+d))
            log_prr = math.log(prr)
            lower_ci = math.exp(log_prr - 1.96 * se_log)
            upper_ci = math.exp(log_prr + 1.96 * se_log)
        except (ValueError, ZeroDivisionError):
            lower_ci = 0
            upper_ci = prr * 2

        # Signal: PRR >= 2.0 AND lower CI >= 1 AND n >= 3
        is_signal = prr >= request.prr_threshold and lower_ci >= 1 and a >= 3
        strength = "high" if is_signal and prr >= 3 else "elevated" if is_signal else "normal"

        return MethodResult(
            method=SignalMethod.PRR,
            value=round(prr, 2),
            lower_ci=round(lower_ci, 2),
            upper_ci=round(upper_ci, 2),
            is_signal=is_signal,
            signal_strength=strength,
            details={"a": a, "b": b, "c": c, "d": d},
        )

    def _calculate_ror(self, entity_data: dict, request: SignalRequest, time_info: TimeInfo) -> MethodResult:
        """Calculate Reporting Odds Ratio with 95% CI."""
        disprop = self._get_disproportionality_counts(entity_data, request, time_info, target_type="D")

        if disprop["a"] < 3 or disprop["b"] == 0 or disprop["c"] == 0 or disprop["d"] == 0:
            return MethodResult(method=SignalMethod.ROR, value=None, is_signal=False, signal_strength="normal")

        a, b, c, d = disprop["a"], disprop["b"], disprop["c"], disprop["d"]

        # ROR = (a*d) / (b*c)
        ror = (a * d) / (b * c)

        # 95% CI using log transformation
        # SE(log(ROR)) = sqrt(1/a + 1/b + 1/c + 1/d)
        try:
            se_log = math.sqrt(1/a + 1/b + 1/c + 1/d)
            log_ror = math.log(ror)
            lower_ci = math.exp(log_ror - 1.96 * se_log)
            upper_ci = math.exp(log_ror + 1.96 * se_log)
        except (ValueError, ZeroDivisionError):
            lower_ci = 0
            upper_ci = ror * 2

        is_signal = ror >= request.ror_threshold and lower_ci >= 1
        strength = "high" if is_signal and ror >= 3 else "elevated" if is_signal else "normal"

        return MethodResult(
            method=SignalMethod.ROR,
            value=round(ror, 2),
            lower_ci=round(lower_ci, 2),
            upper_ci=round(upper_ci, 2),
            is_signal=is_signal,
            signal_strength=strength,
            details={"a": a, "b": b, "c": c, "d": d},
        )

    def _calculate_ebgm(self, entity_data: dict, request: SignalRequest, time_info: TimeInfo) -> MethodResult:
        """Calculate Empirical Bayes Geometric Mean (simplified)."""
        disprop = self._get_disproportionality_counts(entity_data, request, time_info, target_type="D")

        a, b, c, d = disprop["a"], disprop["b"], disprop["c"], disprop["d"]
        total = a + b + c + d

        if total == 0 or (a + b) == 0 or (a + c) == 0:
            return MethodResult(method=SignalMethod.EBGM, value=None, is_signal=False, signal_strength="normal")

        # Expected count under independence
        E = (a + b) * (a + c) / total

        if E == 0:
            return MethodResult(method=SignalMethod.EBGM, value=None, is_signal=False, signal_strength="normal")

        # Observed/Expected ratio
        rr = a / E

        # Simplified EBGM using shrinkage toward 1
        # More sophisticated implementation would use full Bayesian mixture model
        # EBGM = (a + 0.5) / (E + 0.5)  -- simple shrinkage estimate
        ebgm = (a + 0.5) / (E + 0.5)

        # EB05 (5th percentile) approximation using Poisson uncertainty
        # For small counts, shrink more toward 1
        try:
            # Approximate lower bound using gamma-Poisson model
            from scipy import stats
            eb05 = stats.gamma.ppf(0.05, a + 0.5, scale=1/(E + 0.5))
        except ImportError:
            # Fallback without scipy
            eb05 = ebgm * 0.5 if a < 5 else ebgm * 0.7

        is_signal = ebgm >= 2.0 and eb05 >= 1.0
        strength = "high" if is_signal and ebgm >= 3 else "elevated" if is_signal else "normal"

        return MethodResult(
            method=SignalMethod.EBGM,
            value=round(ebgm, 2),
            lower_ci=round(eb05, 2),
            is_signal=is_signal,
            signal_strength=strength,
            details={"observed": a, "expected": round(E, 2), "rr": round(rr, 2)},
        )

    def _calculate_cusum(self, entity_data: dict, request: SignalRequest, time_info: TimeInfo) -> MethodResult:
        """Calculate CUSUM for drift detection."""
        # Get monthly time series for entity
        monthly_data = self._get_monthly_series(entity_data["entity"], request, time_info)

        if len(monthly_data) < 3:
            return MethodResult(method=SignalMethod.CUSUM, value=None, is_signal=False, signal_strength="normal")

        counts = [m["count"] for m in monthly_data]
        mean = sum(counts) / len(counts)
        std = (sum((c - mean) ** 2 for c in counts) / len(counts)) ** 0.5

        if std == 0:
            return MethodResult(method=SignalMethod.CUSUM, value=0, is_signal=False, signal_strength="normal")

        # Calculate CUSUM: cumulative sum of (x - mean) / std
        cusum = 0
        max_cusum = 0
        cusum_series = []
        for i, count in enumerate(counts):
            cusum = max(0, cusum + (count - mean) / std - 0.5)  # One-sided CUSUM with slack
            max_cusum = max(max_cusum, cusum)
            month_str = monthly_data[i]["month"].isoformat() if hasattr(monthly_data[i]["month"], 'isoformat') else str(monthly_data[i]["month"])
            cusum_series.append({
                "month": month_str,
                "cusum": round(cusum, 2),
                "count": count,
            })

        # Control limit: typically 3-5 sigma
        control_limit = 3.0
        is_signal = max_cusum > control_limit
        strength = "high" if max_cusum > 5 else "elevated" if is_signal else "normal"

        return MethodResult(
            method=SignalMethod.CUSUM,
            value=round(max_cusum, 2),
            is_signal=is_signal,
            signal_strength=strength,
            details={
                "mean": round(mean, 1),
                "std": round(std, 1),
                "control_limit": control_limit,
                "cusum_series": cusum_series,  # Return full lookback period
            },
        )

    def _calculate_yoy(self, entity_data: dict, request: SignalRequest) -> MethodResult:
        """Calculate year-over-year change."""
        change_pct = entity_data.get("change_pct")

        if change_pct is None:
            return MethodResult(method=SignalMethod.YOY, value=None, is_signal=False, signal_strength="normal")

        if change_pct > request.change_pct_high:
            strength = "high"
            is_signal = True
        elif change_pct > request.change_pct_elevated:
            strength = "elevated"
            is_signal = True
        else:
            strength = "normal"
            is_signal = False

        return MethodResult(
            method=SignalMethod.YOY,
            value=round(change_pct, 1),
            is_signal=is_signal,
            signal_strength=strength,
            details={
                "current_period": entity_data.get("current_period_events"),
                "comparison_period": entity_data.get("comparison_period_events"),
            },
        )

    def _calculate_pop(self, entity_data: dict, request: SignalRequest) -> MethodResult:
        """Calculate period-over-period change (same as YoY logic)."""
        # Uses the same comparison data as YoY
        return self._calculate_yoy(entity_data, request)

    def _calculate_rolling(self, entity_data: dict, request: SignalRequest, time_info: TimeInfo) -> MethodResult:
        """Calculate signal against rolling average baseline."""
        monthly_data = self._get_monthly_series(entity_data["entity"], request, time_info)
        window = time_info.rolling_window or 3

        if len(monthly_data) < window + 1:
            return MethodResult(method=SignalMethod.ROLLING, value=None, is_signal=False, signal_strength="normal")

        # Calculate rolling average excluding the last month
        baseline_counts = [m["count"] for m in monthly_data[:-1]][-window:]
        rolling_avg = sum(baseline_counts) / len(baseline_counts)
        rolling_std = (sum((c - rolling_avg) ** 2 for c in baseline_counts) / len(baseline_counts)) ** 0.5

        latest = monthly_data[-1]["count"]

        if rolling_std == 0:
            deviation = 0
        else:
            deviation = (latest - rolling_avg) / rolling_std

        if deviation > 2:
            strength = "high"
            is_signal = True
        elif deviation > 1:
            strength = "elevated"
            is_signal = True
        else:
            strength = "normal"
            is_signal = False

        # Build monthly series for visualization (full lookback period)
        monthly_series = [
            {"month": m["month"].isoformat() if hasattr(m["month"], 'isoformat') else str(m["month"]), "count": m["count"]}
            for m in monthly_data  # Return full lookback period
        ]

        return MethodResult(
            method=SignalMethod.ROLLING,
            value=round(deviation, 2),
            is_signal=is_signal,
            signal_strength=strength,
            details={
                "rolling_avg": round(rolling_avg, 1),
                "rolling_std": round(rolling_std, 1),
                "latest": latest,
                "window_months": window,
                "monthly_series": monthly_series,
            },
        )

    def _get_disproportionality_counts(
        self, entity_data: dict, request: SignalRequest, time_info: TimeInfo, target_type: str = "D"
    ) -> dict:
        """Get 2x2 table counts for disproportionality methods."""
        entity = entity_data["entity"]
        level_col = self.LEVEL_COLUMNS[request.level]
        needs_device_join = request.level != DrillDownLevel.MANUFACTURER

        # Build base query conditions
        conditions = ["m.date_received >= ?", "m.date_received <= ?"]
        params = [time_info.analysis_start.isoformat(), time_info.analysis_end.isoformat()]

        # Apply comparison population filters
        if request.comparison_population == ComparisonPopulation.SAME_PRODUCT_CODE and request.product_codes:
            placeholders = ", ".join(["?" for _ in request.product_codes])
            conditions.append(f"m.product_code IN ({placeholders})")
            params.extend(request.product_codes)

        where_clause = " AND ".join(conditions)

        if needs_device_join:
            query = f"""
                SELECT
                    CASE WHEN {level_col} = ? THEN 'entity' ELSE 'other' END as group_type,
                    CASE WHEN m.event_type = ? THEN 'target' ELSE 'other' END as event_type,
                    COUNT(DISTINCT m.mdr_report_key) as count
                FROM master_events m
                JOIN devices d ON d.mdr_report_key = m.mdr_report_key
                WHERE {where_clause}
                AND {level_col} IS NOT NULL
                GROUP BY 1, 2
            """
        else:
            query = f"""
                SELECT
                    CASE WHEN m.manufacturer_clean = ? THEN 'entity' ELSE 'other' END as group_type,
                    CASE WHEN m.event_type = ? THEN 'target' ELSE 'other' END as event_type,
                    COUNT(DISTINCT m.mdr_report_key) as count
                FROM master_events m
                WHERE {where_clause}
                AND m.manufacturer_clean IS NOT NULL
                GROUP BY 1, 2
            """

        params = [entity, target_type] + params

        results = self.db.fetch_all(query, params)

        # Parse into 2x2 table
        counts = {"a": 0, "b": 0, "c": 0, "d": 0}
        for row in results:
            group, event, count = row
            if group == "entity" and event == "target":
                counts["a"] = int(count)
            elif group == "entity" and event == "other":
                counts["b"] = int(count)
            elif group == "other" and event == "target":
                counts["c"] = int(count)
            elif group == "other" and event == "other":
                counts["d"] = int(count)

        return counts

    def _get_monthly_series(self, entity: str, request: SignalRequest, time_info: TimeInfo) -> list[dict]:
        """Get monthly event counts for an entity."""
        level_col = self.LEVEL_COLUMNS[request.level]
        needs_device_join = request.level != DrillDownLevel.MANUFACTURER

        if needs_device_join:
            query = f"""
                SELECT DATE_TRUNC('month', m.date_received) as month, COUNT(DISTINCT m.mdr_report_key) as count
                FROM master_events m
                JOIN devices d ON d.mdr_report_key = m.mdr_report_key
                WHERE m.date_received >= ? AND m.date_received <= ?
                AND {level_col} = ?
                GROUP BY 1
                ORDER BY 1
            """
        else:
            query = f"""
                SELECT DATE_TRUNC('month', m.date_received) as month, COUNT(DISTINCT m.mdr_report_key) as count
                FROM master_events m
                WHERE m.date_received >= ? AND m.date_received <= ?
                AND m.manufacturer_clean = ?
                GROUP BY 1
                ORDER BY 1
            """

        params = [time_info.analysis_start.isoformat(), time_info.analysis_end.isoformat(), entity]
        results = self.db.fetch_all(query, params)

        return [{"month": row[0], "count": int(row[1])} for row in results]

    def _has_children(self, entity: str, current_level: DrillDownLevel, child_level: DrillDownLevel) -> bool:
        """Check if entity has child records at the next drill-down level.

        Note: For performance, we assume all non-model level entities have children.
        The actual drill-down will show if there's data or not.
        """
        # Model level has no children
        if current_level == DrillDownLevel.MODEL:
            return False

        # For other levels, assume children exist to avoid expensive per-entity queries
        # The drill-down will show empty results if there are no children
        return True

    def _determine_overall_signal(self, method_results: list[MethodResult]) -> str:
        """Determine overall signal type from multiple method results."""
        if any(r.signal_strength == "high" for r in method_results if r.is_signal):
            return "high"
        elif any(r.signal_strength == "elevated" for r in method_results if r.is_signal):
            return "elevated"
        return "normal"
