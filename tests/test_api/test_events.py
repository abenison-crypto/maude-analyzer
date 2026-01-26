"""Tests for events API endpoints."""

import pytest


class TestListEvents:
    """Tests for GET /api/events endpoint."""

    def test_list_events_default(self, client):
        """Test listing events with default parameters."""
        response = client.get("/api/events")
        assert response.status_code == 200

        data = response.json()
        assert "events" in data
        assert "pagination" in data
        assert isinstance(data["events"], list)
        assert data["pagination"]["page"] == 1

    def test_list_events_pagination(self, client):
        """Test pagination parameters."""
        response = client.get("/api/events", params={"page": 2, "page_size": 10})
        assert response.status_code == 200

        data = response.json()
        assert data["pagination"]["page"] == 2
        assert data["pagination"]["page_size"] == 10
        assert len(data["events"]) <= 10

    def test_list_events_filter_by_event_type(self, client):
        """Test filtering by event type."""
        response = client.get("/api/events", params={"event_types": "D"})
        assert response.status_code == 200

        data = response.json()
        # If there are results, verify they match the filter
        for event in data["events"]:
            assert event["event_type"] == "D"

    def test_list_events_filter_by_manufacturer(self, client, sample_manufacturer):
        """Test filtering by manufacturer."""
        if not sample_manufacturer:
            pytest.skip("No manufacturers in database")

        response = client.get("/api/events", params={"manufacturers": sample_manufacturer})
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data["events"], list)

    def test_list_events_filter_by_product_code(self, client, sample_product_code):
        """Test filtering by product code."""
        if not sample_product_code:
            pytest.skip("No product codes in database")

        response = client.get("/api/events", params={"product_codes": sample_product_code})
        assert response.status_code == 200

    def test_list_events_filter_by_date_range(self, client):
        """Test filtering by date range."""
        response = client.get(
            "/api/events",
            params={"date_from": "2023-01-01", "date_to": "2023-12-31"}
        )
        assert response.status_code == 200


class TestEventStats:
    """Tests for GET /api/events/stats endpoint."""

    def test_get_stats(self, client):
        """Test getting event statistics."""
        response = client.get("/api/events/stats")
        assert response.status_code == 200

        data = response.json()
        assert "total" in data
        assert "deaths" in data
        assert "injuries" in data
        assert "malfunctions" in data
        assert isinstance(data["total"], int)

    def test_get_stats_filtered(self, client):
        """Test getting filtered statistics."""
        response = client.get("/api/events/stats", params={"event_types": "D"})
        assert response.status_code == 200

        data = response.json()
        assert data["total"] >= 0


class TestEventDetail:
    """Tests for GET /api/events/{mdr_report_key} endpoint."""

    def test_get_event_detail(self, client, sample_event_key):
        """Test getting event detail."""
        if not sample_event_key:
            pytest.skip("No events in database")

        response = client.get(f"/api/events/{sample_event_key}")
        assert response.status_code == 200

        data = response.json()
        assert data["mdr_report_key"] == sample_event_key

    def test_get_event_not_found(self, client):
        """Test getting non-existent event."""
        response = client.get("/api/events/NONEXISTENT_KEY_12345")
        assert response.status_code == 404


class TestExport:
    """Tests for GET /api/events/export endpoint."""

    def test_export_csv(self, client):
        """Test CSV export."""
        response = client.get("/api/events/export", params={"format": "csv", "max_records": 10})
        assert response.status_code == 200
        assert "text/csv" in response.headers["content-type"]
        assert "attachment" in response.headers["content-disposition"]
        assert ".csv" in response.headers["content-disposition"]

    def test_export_excel(self, client):
        """Test Excel export."""
        response = client.get("/api/events/export", params={"format": "xlsx", "max_records": 10})
        # May return 400 if openpyxl not available
        if response.status_code == 200:
            assert "spreadsheetml" in response.headers["content-type"]
            assert ".xlsx" in response.headers["content-disposition"]
        else:
            assert response.status_code == 400

    def test_export_with_filters(self, client):
        """Test export with filters applied."""
        response = client.get(
            "/api/events/export",
            params={"event_types": "D", "max_records": 10}
        )
        assert response.status_code == 200


class TestManufacturers:
    """Tests for GET /api/events/manufacturers endpoint."""

    def test_list_manufacturers(self, client):
        """Test listing manufacturers."""
        response = client.get("/api/events/manufacturers")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "name" in data[0]
            assert "count" in data[0]

    def test_search_manufacturers(self, client):
        """Test searching manufacturers."""
        response = client.get("/api/events/manufacturers", params={"search": "med", "limit": 10})
        assert response.status_code == 200

        data = response.json()
        assert len(data) <= 10


class TestProductCodes:
    """Tests for GET /api/events/product-codes endpoint."""

    def test_list_product_codes(self, client):
        """Test listing product codes."""
        response = client.get("/api/events/product-codes")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "code" in data[0]
            assert "count" in data[0]

    def test_search_product_codes(self, client):
        """Test searching product codes."""
        response = client.get("/api/events/product-codes", params={"search": "A", "limit": 10})
        assert response.status_code == 200

        data = response.json()
        assert len(data) <= 10
