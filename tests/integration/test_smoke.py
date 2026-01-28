"""
Feature Smoke Tests for MAUDE Analyzer.

Quick tests to verify core application features work end-to-end.
Run these after deployments or major changes to catch regressions.

Usage:
    pytest tests/integration/test_smoke.py -v
    pytest tests/integration/test_smoke.py -v -k "home" (single test)
"""

import os
import sys
from pathlib import Path

import pytest

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Skip all tests if database not available
DB_PATH = PROJECT_ROOT / "data" / "maude.duckdb"
SKIP_REASON = "Database not available for smoke testing"


def db_available() -> bool:
    """Check if database file exists."""
    return DB_PATH.exists()


pytestmark = pytest.mark.skipif(not db_available(), reason=SKIP_REASON)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def api_client():
    """Create a test client for the API."""
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


# =============================================================================
# Home Page / Dashboard Tests
# =============================================================================

class TestHomeDashboard:
    """Smoke tests for home/dashboard functionality."""

    def test_stats_load(self, api_client):
        """Dashboard stats should load without errors."""
        response = api_client.get("/api/events/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] >= 0

    def test_recent_events_load(self, api_client):
        """Recent events list should load."""
        response = api_client.get("/api/events?page_size=10")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data

    def test_trends_load(self, api_client):
        """Trend data should load for charts."""
        response = api_client.get("/api/analytics/trends?group_by=month")
        assert response.status_code == 200


# =============================================================================
# Explore Page / Events List Tests
# =============================================================================

class TestExplorePage:
    """Smoke tests for explore/events listing functionality."""

    def test_events_paginated(self, api_client):
        """Events pagination should work."""
        # Page 1
        response1 = api_client.get("/api/events?page=1&page_size=5")
        assert response1.status_code == 200
        data1 = response1.json()

        # Page 2 (if exists)
        if data1["pagination"]["total_pages"] > 1:
            response2 = api_client.get("/api/events?page=2&page_size=5")
            assert response2.status_code == 200
            data2 = response2.json()
            # Should be mostly different events (allow small overlap due to sort instability)
            keys1 = {e["mdr_report_key"] for e in data1["events"]}
            keys2 = {e["mdr_report_key"] for e in data2["events"]}
            overlap = len(keys1.intersection(keys2))
            assert overlap < 3, f"Too much overlap between pages: {overlap} events"

    def test_manufacturer_filter(self, api_client):
        """Manufacturer filter should work."""
        # Get a manufacturer
        mfr_resp = api_client.get("/api/events/manufacturers?limit=1")
        if mfr_resp.json():
            mfr = mfr_resp.json()[0]["name"]
            response = api_client.get(f"/api/events?manufacturers={mfr}&page_size=3")
            assert response.status_code == 200

    def test_date_filter(self, api_client):
        """Date range filter should work."""
        response = api_client.get("/api/events?date_from=2020-01-01&page_size=3")
        assert response.status_code == 200

    def test_event_type_filter(self, api_client):
        """Event type filter should work."""
        for event_type in ["D", "I", "M", "O"]:
            response = api_client.get(f"/api/events?event_types={event_type}&page_size=3")
            assert response.status_code == 200

    def test_text_search(self, api_client):
        """Text search should work."""
        response = api_client.get("/api/events?search_text=pain&page_size=3")
        assert response.status_code == 200

    def test_combined_filters(self, api_client):
        """Multiple filters combined should work."""
        response = api_client.get(
            "/api/events?event_types=D&date_from=2020-01-01&page_size=3"
        )
        assert response.status_code == 200


# =============================================================================
# Event Detail Tests
# =============================================================================

class TestEventDetail:
    """Smoke tests for event detail view."""

    def test_event_detail_loads(self, api_client):
        """Event detail should load with all sections."""
        # Get an event
        list_resp = api_client.get("/api/events?page_size=1")
        events = list_resp.json().get("events", [])
        if not events:
            pytest.skip("No events available")

        mdr_key = events[0]["mdr_report_key"]
        response = api_client.get(f"/api/events/{mdr_key}")
        assert response.status_code == 200

        data = response.json()
        assert "devices" in data
        assert "narratives" in data
        assert "patients" in data

    def test_invalid_event_returns_404(self, api_client):
        """Non-existent event should return 404."""
        response = api_client.get("/api/events/INVALID_KEY_999999")
        assert response.status_code == 404


# =============================================================================
# Analytics Page Tests
# =============================================================================

class TestAnalyticsPage:
    """Smoke tests for analytics functionality."""

    def test_trends_by_day(self, api_client):
        """Daily trends should work."""
        response = api_client.get("/api/analytics/trends?group_by=day")
        assert response.status_code == 200

    def test_trends_by_month(self, api_client):
        """Monthly trends should work."""
        response = api_client.get("/api/analytics/trends?group_by=month")
        assert response.status_code == 200

    def test_trends_by_year(self, api_client):
        """Yearly trends should work."""
        response = api_client.get("/api/analytics/trends?group_by=year")
        assert response.status_code == 200

    def test_signal_detection(self, api_client):
        """Signal detection should work."""
        response = api_client.get("/api/analytics/signals")
        assert response.status_code == 200

    def test_manufacturer_compare(self, api_client):
        """Manufacturer comparison should work."""
        # Get two manufacturers
        mfr_resp = api_client.get("/api/events/manufacturers?limit=2")
        manufacturers = mfr_resp.json()
        if len(manufacturers) < 2:
            pytest.skip("Need at least 2 manufacturers")

        mfr1 = manufacturers[0]["name"]
        mfr2 = manufacturers[1]["name"]
        response = api_client.get(f"/api/analytics/compare?manufacturers={mfr1},{mfr2}")
        assert response.status_code == 200

    def test_text_frequency(self, api_client):
        """Text frequency analysis should work."""
        response = api_client.get("/api/analytics/text-frequency")
        assert response.status_code == 200

    def test_event_type_distribution(self, api_client):
        """Event type distribution should work."""
        response = api_client.get("/api/analytics/event-type-distribution")
        assert response.status_code == 200


# =============================================================================
# Admin Page Tests
# =============================================================================

class TestAdminPage:
    """Smoke tests for admin/data quality functionality."""

    def test_completeness_report(self, api_client):
        """Completeness report should load."""
        response = api_client.get("/api/data-quality/completeness")
        assert response.status_code == 200

    def test_file_status(self, api_client):
        """File status should load."""
        response = api_client.get("/api/data-quality/file-status")
        assert response.status_code == 200

    def test_coverage_report(self, api_client):
        """Coverage report should load."""
        response = api_client.get("/api/data-quality/coverage")
        assert response.status_code == 200

    def test_gaps_report(self, api_client):
        """Gaps report should load."""
        response = api_client.get("/api/data-quality/gaps")
        assert response.status_code == 200

    def test_summary_report(self, api_client):
        """Summary report should load."""
        response = api_client.get("/api/data-quality/summary")
        assert response.status_code == 200

    def test_schema_health(self, api_client):
        """Schema health should load."""
        response = api_client.get("/api/data-quality/schema-health")
        assert response.status_code == 200

    def test_column_coverage_detail(self, api_client):
        """Column coverage detail should load."""
        response = api_client.get("/api/data-quality/column-coverage/master_events")
        assert response.status_code == 200


# =============================================================================
# Export Tests
# =============================================================================

class TestExport:
    """Smoke tests for export functionality."""

    def test_csv_export(self, api_client):
        """CSV export should work."""
        response = api_client.get("/api/events/export?format=csv&max_records=10")
        assert response.status_code == 200
        assert "text/csv" in response.headers.get("content-type", "")

    def test_xlsx_export(self, api_client):
        """Excel export should work (if openpyxl available)."""
        response = api_client.get("/api/events/export?format=xlsx&max_records=10")
        # May be 400 if openpyxl not installed
        assert response.status_code in [200, 400]


# =============================================================================
# Autocomplete Tests
# =============================================================================

class TestAutocomplete:
    """Smoke tests for autocomplete functionality."""

    def test_manufacturer_autocomplete(self, api_client):
        """Manufacturer autocomplete should work."""
        response = api_client.get("/api/events/manufacturers?search=med&limit=5")
        assert response.status_code == 200

    def test_product_code_autocomplete(self, api_client):
        """Product code autocomplete should work."""
        response = api_client.get("/api/events/product-codes?search=g&limit=5")
        assert response.status_code == 200


# =============================================================================
# Performance Sanity Tests
# =============================================================================

class TestPerformanceSanity:
    """Basic performance sanity checks."""

    def test_events_list_response_time(self, api_client):
        """Events list should respond reasonably fast."""
        import time
        start = time.time()
        response = api_client.get("/api/events?page_size=50")
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should respond in under 5 seconds
        assert elapsed < 5.0, f"Events list too slow: {elapsed:.2f}s"

    def test_stats_response_time(self, api_client):
        """Stats should respond reasonably fast."""
        import time
        start = time.time()
        response = api_client.get("/api/events/stats")
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should respond in under 3 seconds
        assert elapsed < 3.0, f"Stats too slow: {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
