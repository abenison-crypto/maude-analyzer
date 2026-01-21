"""Tests for signal detection module."""

import pytest
from datetime import date


class TestSignalSeverity:
    """Tests for SignalSeverity enum."""

    def test_severity_levels(self):
        """Test all severity levels exist."""
        from src.analysis.signals import SignalSeverity

        assert SignalSeverity.LOW.value == "low"
        assert SignalSeverity.MEDIUM.value == "medium"
        assert SignalSeverity.HIGH.value == "high"
        assert SignalSeverity.CRITICAL.value == "critical"


class TestSignalType:
    """Tests for SignalType enum."""

    def test_signal_types(self):
        """Test all signal types exist."""
        from src.analysis.signals import SignalType

        assert SignalType.VOLUME_SPIKE.value == "volume_spike"
        assert SignalType.DEATH_INCREASE.value == "death_increase"
        assert SignalType.MANUFACTURER_TREND.value == "manufacturer_trend"


class TestSafetySignal:
    """Tests for SafetySignal dataclass."""

    def test_signal_creation(self):
        """Test creating a safety signal."""
        from src.analysis.signals import SafetySignal, SignalType, SignalSeverity

        signal = SafetySignal(
            signal_type=SignalType.VOLUME_SPIKE,
            severity=SignalSeverity.HIGH,
            title="Test Signal",
            description="Test description",
            manufacturer="Abbott",
            current_value=100,
            baseline_value=50,
            percent_change=100.0,
        )

        assert signal.signal_type == SignalType.VOLUME_SPIKE
        assert signal.severity == SignalSeverity.HIGH
        assert signal.title == "Test Signal"
        assert signal.manufacturer == "Abbott"
        assert signal.percent_change == 100.0


class TestSignalDetector:
    """Tests for SignalDetector class."""

    def test_detector_initialization(self):
        """Test detector initialization with defaults."""
        from src.analysis.signals import SignalDetector

        detector = SignalDetector()

        assert detector.baseline_months == 12
        assert detector.analysis_months == 3
        assert detector.volume_threshold == 2.0

    def test_detector_custom_params(self):
        """Test detector with custom parameters."""
        from src.analysis.signals import SignalDetector

        detector = SignalDetector(
            baseline_months=6,
            analysis_months=1,
            volume_threshold=3.0,
        )

        assert detector.baseline_months == 6
        assert detector.analysis_months == 1
        assert detector.volume_threshold == 3.0

    def test_get_severity_from_zscore(self):
        """Test severity determination from z-score."""
        from src.analysis.signals import SignalDetector, SignalSeverity

        detector = SignalDetector()

        assert detector._get_severity_from_zscore(5.0) == SignalSeverity.CRITICAL
        assert detector._get_severity_from_zscore(3.5) == SignalSeverity.HIGH
        assert detector._get_severity_from_zscore(2.5) == SignalSeverity.MEDIUM
        assert detector._get_severity_from_zscore(1.5) == SignalSeverity.LOW


class TestSignalDetection:
    """Tests for signal detection functions."""

    def test_detect_signals_returns_result(self, test_db):
        """Test that detect_signals returns a result object."""
        from src.analysis.signals import detect_signals, SignalDetectionResult

        # Note: With limited test data, we may not detect signals
        result = detect_signals()

        assert isinstance(result, SignalDetectionResult)
        assert hasattr(result, "signals")
        assert isinstance(result.signals, list)

    def test_get_monthly_summary(self, test_db):
        """Test monthly summary retrieval."""
        from src.analysis.signals import get_monthly_summary
        import pandas as pd

        result = get_monthly_summary(months=12, conn=test_db)

        assert isinstance(result, pd.DataFrame)
        assert "month" in result.columns or "period" in result.columns or result.empty
