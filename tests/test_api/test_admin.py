"""Tests for admin API endpoints."""

import pytest


class TestDatabaseStatus:
    """Tests for GET /api/admin/status endpoint."""

    def test_get_status(self, client):
        """Test getting database status."""
        response = client.get("/api/admin/status")
        assert response.status_code == 200

        data = response.json()
        assert "total_events" in data
        assert "total_devices" in data
        assert "total_patients" in data
        assert "manufacturer_coverage_pct" in data
        assert isinstance(data["total_events"], int)

    def test_status_has_date_range(self, client):
        """Test that status includes date range info."""
        response = client.get("/api/admin/status")
        assert response.status_code == 200

        data = response.json()
        assert "date_range_start" in data
        assert "date_range_end" in data


class TestDataQuality:
    """Tests for GET /api/admin/data-quality endpoint."""

    def test_get_data_quality(self, client):
        """Test getting data quality report."""
        response = client.get("/api/admin/data-quality")
        assert response.status_code == 200

        data = response.json()
        assert "field_completeness" in data
        assert "event_type_distribution" in data
        assert "orphan_analysis" in data

    def test_field_completeness_structure(self, client):
        """Test field completeness data structure."""
        response = client.get("/api/admin/data-quality")
        assert response.status_code == 200

        data = response.json()
        if data["field_completeness"]:
            field = data["field_completeness"][0]
            assert "field" in field
            assert "percentage" in field

    def test_orphan_analysis_structure(self, client):
        """Test orphan analysis data structure."""
        response = client.get("/api/admin/data-quality")
        assert response.status_code == 200

        data = response.json()
        orphan = data["orphan_analysis"]
        assert "orphaned_devices" in orphan or "events_without_devices" in orphan


class TestIngestionHistory:
    """Tests for GET /api/admin/history endpoint."""

    def test_get_history(self, client):
        """Test getting ingestion history."""
        response = client.get("/api/admin/history")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)

    def test_get_history_with_limit(self, client):
        """Test getting ingestion history with limit."""
        response = client.get("/api/admin/history", params={"limit": 10})
        assert response.status_code == 200

        data = response.json()
        assert len(data) <= 10

    def test_history_structure(self, client):
        """Test ingestion history data structure."""
        response = client.get("/api/admin/history", params={"limit": 1})
        assert response.status_code == 200

        data = response.json()
        if data:
            record = data[0]
            # These fields should be present if history exists
            assert "file_name" in record or "id" in record


class TestHealthCheck:
    """Tests for GET /health endpoint."""

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200

        data = response.json()
        assert "status" in data
        assert "database" in data

    def test_health_check_healthy(self, client):
        """Test that health check returns healthy status."""
        response = client.get("/health")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert data["database"] == "connected"


class TestRoot:
    """Tests for GET / endpoint."""

    def test_root(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200

        data = response.json()
        assert "name" in data
        assert "version" in data
        assert "endpoints" in data
