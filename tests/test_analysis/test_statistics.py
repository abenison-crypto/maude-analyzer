"""Tests for statistics module."""

import pytest
import pandas as pd
from datetime import date


class TestStatisticsFunctions:
    """Tests for statistical functions."""

    def test_calculate_proportion_confidence_interval(self):
        """Test confidence interval calculation."""
        from src.analysis.statistics import calculate_proportion_confidence_interval

        # 10 successes out of 100 trials = 10%
        lower, upper = calculate_proportion_confidence_interval(10, 100)

        assert lower < 10  # Lower bound should be below proportion
        assert upper > 10  # Upper bound should be above proportion
        assert lower > 0
        assert upper < 100

    def test_calculate_proportion_confidence_interval_zero_trials(self):
        """Test CI with zero trials."""
        from src.analysis.statistics import calculate_proportion_confidence_interval

        lower, upper = calculate_proportion_confidence_interval(0, 0)

        assert lower == 0
        assert upper == 0

    def test_compare_proportions(self):
        """Test two-proportion comparison."""
        from src.analysis.statistics import compare_proportions

        # Significant difference: 50% vs 10%
        result = compare_proportions(50, 100, 10, 100)

        assert result["p_a"] == 50
        assert result["p_b"] == 10
        assert result["difference"] == 40
        assert result["significant"] == True

    def test_compare_proportions_no_difference(self):
        """Test comparison with no difference."""
        from src.analysis.statistics import compare_proportions

        result = compare_proportions(10, 100, 10, 100)

        assert result["difference"] == 0
        assert result["significant"] == False

    def test_compare_manufacturers(self, test_db):
        """Test manufacturer comparison."""
        from src.analysis.statistics import compare_manufacturers

        result = compare_manufacturers("Abbott", "Medtronic", conn=test_db)

        assert "total_mdrs" in result
        assert "death_rate" in result
        assert result["total_mdrs"].group_a == "Abbott"
        assert result["total_mdrs"].group_b == "Medtronic"

    def test_get_summary_statistics(self, test_db):
        """Test summary statistics."""
        from src.analysis.statistics import get_summary_statistics

        stats = get_summary_statistics(conn=test_db)

        assert stats["total_mdrs"] == 5
        assert stats["deaths"] == 1
        assert stats["injuries"] == 2
        assert stats["unique_manufacturers"] == 4
        assert "death_rate" in stats

    def test_rank_manufacturers_by_metric(self, test_db):
        """Test manufacturer ranking."""
        from src.analysis.statistics import rank_manufacturers_by_metric

        df = rank_manufacturers_by_metric(
            metric="total_mdrs",
            min_mdrs=1,
            conn=test_db
        )

        assert isinstance(df, pd.DataFrame)
        assert "rank" in df.columns
        assert "manufacturer_clean" in df.columns
        # Abbott should be first with 2 MDRs
        assert df.iloc[0]["manufacturer_clean"] == "Abbott"

    def test_analyze_trend(self):
        """Test trend analysis."""
        from src.analysis.statistics import analyze_trend

        # Create sample trend data
        df = pd.DataFrame({
            "period": pd.date_range("2024-01-01", periods=6, freq="M"),
            "count": [10, 12, 15, 18, 22, 25]  # Increasing trend
        })

        result = analyze_trend(df, "period", "count")

        assert result.trend_direction == "increasing"
        assert result.slope > 0
        assert result.data_points == 6

    def test_analyze_trend_decreasing(self):
        """Test decreasing trend analysis."""
        from src.analysis.statistics import analyze_trend

        df = pd.DataFrame({
            "period": pd.date_range("2024-01-01", periods=6, freq="M"),
            "count": [25, 22, 18, 15, 12, 10]  # Decreasing trend
        })

        result = analyze_trend(df, "period", "count")

        assert result.trend_direction == "decreasing"
        assert result.slope < 0
