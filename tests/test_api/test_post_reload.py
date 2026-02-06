"""Post-reload validation tests.

Tests to verify the database was properly loaded and API endpoints
are returning correct data after the 128M+ record reload.
"""
import pytest
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


class TestStatsEndpoint:
    """Verify stats endpoint returns expected counts after reload."""

    def test_stats_returns_expected_total(self):
        """Total should be ~23.6M events."""
        response = client.get("/api/events/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] >= 23_000_000, f"Expected 23M+ events, got {data['total']}"

    def test_stats_has_all_categories(self):
        """Stats should include all event type categories."""
        response = client.get("/api/events/stats")
        assert response.status_code == 200
        data = response.json()
        required_keys = ["total", "deaths", "injuries", "malfunctions", "other"]
        for key in required_keys:
            assert key in data, f"Missing key: {key}"
            assert isinstance(data[key], int), f"{key} should be int"

    def test_stats_categories_sum_to_total(self):
        """Deaths + injuries + malfunctions + other should equal total (accounting for nulls)."""
        response = client.get("/api/events/stats")
        data = response.json()
        category_sum = data["deaths"] + data["injuries"] + data["malfunctions"] + data["other"]
        # Allow for some events with null event_type
        assert category_sum <= data["total"]
        # But most should be categorized (at least 90%)
        assert category_sum >= data["total"] * 0.9


class TestEventTypeConversion:
    """Verify I -> IN event type filter conversion works."""

    def test_injury_filter_returns_only_injuries(self):
        """Filtering by 'I' should return only injuries (converted to 'IN')."""
        response = client.get("/api/events/stats?event_types=I")
        assert response.status_code == 200
        data = response.json()
        # Should have injuries
        assert data["injuries"] > 0, "Expected injuries to be > 0"
        # Should NOT have deaths (proves filter is working)
        assert data["deaths"] == 0, "Filter by I should exclude deaths"
        assert data["malfunctions"] == 0, "Filter by I should exclude malfunctions"

    def test_death_filter_returns_only_deaths(self):
        """Filtering by 'D' should return only deaths."""
        response = client.get("/api/events/stats?event_types=D")
        assert response.status_code == 200
        data = response.json()
        assert data["deaths"] > 0
        assert data["injuries"] == 0
        assert data["malfunctions"] == 0

    def test_malfunction_filter_returns_only_malfunctions(self):
        """Filtering by 'M' should return only malfunctions."""
        response = client.get("/api/events/stats?event_types=M")
        assert response.status_code == 200
        data = response.json()
        assert data["malfunctions"] > 0
        assert data["deaths"] == 0
        assert data["injuries"] == 0


class TestPatientSexNormalization:
    """Verify patient sex values are normalized to M/F/U."""

    def test_event_detail_patient_sex_normalized(self):
        """Patient sex should only be M, F, U, or None."""
        # Get first few events
        events_response = client.get("/api/events?page_size=20")
        assert events_response.status_code == 200
        events = events_response.json()["events"]

        valid_sex_values = {"M", "F", "U", None}

        for event in events[:10]:
            detail_response = client.get(f"/api/events/{event['mdr_report_key']}")
            if detail_response.status_code == 200:
                detail = detail_response.json()
                for patient in detail.get("patients", []):
                    sex = patient.get("sex")
                    assert sex in valid_sex_values, f"Invalid sex value: {sex}"


class TestAdminStatus:
    """Verify admin status endpoint returns correct counts."""

    def test_admin_status_event_count(self):
        """Admin status should show 23M+ events."""
        response = client.get("/api/admin/status")
        assert response.status_code == 200
        data = response.json()
        assert data["total_events"] >= 23_000_000

    def test_admin_status_device_count(self):
        """Admin status should show 23M+ devices."""
        response = client.get("/api/admin/status")
        assert response.status_code == 200
        data = response.json()
        assert data["total_devices"] >= 23_000_000

    def test_admin_status_patient_count(self):
        """Admin status should show 22M+ patients."""
        response = client.get("/api/admin/status")
        assert response.status_code == 200
        data = response.json()
        assert data["total_patients"] >= 22_000_000


class TestEventsList:
    """Verify events list endpoint works correctly."""

    def test_events_list_returns_data(self):
        """Events list should return events with pagination."""
        response = client.get("/api/events?page_size=5")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data
        assert "pagination" in data
        assert len(data["events"]) == 5

    def test_events_have_required_fields(self):
        """Each event should have required fields."""
        response = client.get("/api/events?page_size=5")
        data = response.json()
        required_fields = ["mdr_report_key", "report_number", "date_received", "event_type"]
        for event in data["events"]:
            for field in required_fields:
                assert field in event, f"Missing field: {field}"


class TestEventDetail:
    """Verify event detail endpoint works correctly."""

    def test_event_detail_has_devices(self):
        """Event detail should include devices array."""
        events = client.get("/api/events?page_size=1").json()["events"]
        mdr_key = events[0]["mdr_report_key"]
        detail = client.get(f"/api/events/{mdr_key}").json()
        assert "devices" in detail
        assert isinstance(detail["devices"], list)

    def test_event_detail_has_narratives(self):
        """Event detail should include narratives array."""
        events = client.get("/api/events?page_size=1").json()["events"]
        mdr_key = events[0]["mdr_report_key"]
        detail = client.get(f"/api/events/{mdr_key}").json()
        assert "narratives" in detail
        assert isinstance(detail["narratives"], list)

    def test_event_detail_has_patients(self):
        """Event detail should include patients array."""
        events = client.get("/api/events?page_size=1").json()["events"]
        mdr_key = events[0]["mdr_report_key"]
        detail = client.get(f"/api/events/{mdr_key}").json()
        assert "patients" in detail
        assert isinstance(detail["patients"], list)


class TestAnalyticsTrends:
    """Verify analytics trends endpoint works correctly."""

    def test_trends_by_year_returns_data(self):
        """Trends by year should return yearly data."""
        response = client.get("/api/analytics/trends?group_by=year")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 20  # Should have 20+ years of data

    def test_trends_have_required_fields(self):
        """Each trend entry should have required fields."""
        response = client.get("/api/analytics/trends?group_by=year")
        data = response.json()
        required_fields = ["period", "total", "deaths", "injuries", "malfunctions"]
        for entry in data[:5]:
            for field in required_fields:
                assert field in entry, f"Missing field: {field}"
