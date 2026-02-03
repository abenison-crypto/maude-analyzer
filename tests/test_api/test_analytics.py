"""Tests for analytics API endpoints."""

import pytest


class TestTrends:
    """Tests for GET /api/analytics/trends endpoint."""

    def test_get_trends_default(self, client):
        """Test getting trends with default parameters."""
        response = client.get("/api/analytics/trends")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "period" in data[0]
            assert "total" in data[0]
            assert "deaths" in data[0]
            assert "injuries" in data[0]
            assert "malfunctions" in data[0]

    def test_get_trends_by_month(self, client):
        """Test getting trends grouped by month."""
        response = client.get("/api/analytics/trends", params={"group_by": "month"})
        assert response.status_code == 200

    def test_get_trends_by_year(self, client):
        """Test getting trends grouped by year."""
        response = client.get("/api/analytics/trends", params={"group_by": "year"})
        assert response.status_code == 200

    def test_get_trends_invalid_group_by(self, client):
        """Test getting trends with invalid group_by."""
        response = client.get("/api/analytics/trends", params={"group_by": "invalid"})
        assert response.status_code == 400

    def test_get_trends_with_filters(self, client):
        """Test getting filtered trends."""
        response = client.get(
            "/api/analytics/trends",
            params={
                "date_from": "2023-01-01",
                "date_to": "2023-12-31",
                "event_types": "D,I"
            }
        )
        assert response.status_code == 200

    def test_get_trends_date_field_date_received(self, client):
        """Test getting trends using date_received field."""
        response = client.get(
            "/api/analytics/trends",
            params={"date_field": "date_received", "group_by": "year"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_trends_date_field_date_of_event(self, client):
        """Test getting trends using date_of_event field."""
        response = client.get(
            "/api/analytics/trends",
            params={"date_field": "date_of_event", "group_by": "year"}
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_trends_invalid_date_field(self, client):
        """Test getting trends with invalid date_field."""
        response = client.get(
            "/api/analytics/trends",
            params={"date_field": "invalid_field"}
        )
        assert response.status_code == 400


class TestCompareManufacturers:
    """Tests for GET /api/analytics/compare endpoint."""

    def test_compare_manufacturers(self, client, sample_manufacturer):
        """Test comparing manufacturers."""
        if not sample_manufacturer:
            pytest.skip("No manufacturers in database")

        response = client.get(
            "/api/analytics/compare",
            params={"manufacturers": sample_manufacturer}
        )
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)

    def test_compare_no_manufacturers(self, client):
        """Test compare without manufacturers parameter."""
        response = client.get("/api/analytics/compare")
        assert response.status_code == 422  # Missing required parameter

    def test_compare_with_dates(self, client, sample_manufacturer):
        """Test comparing manufacturers with date filter."""
        if not sample_manufacturer:
            pytest.skip("No manufacturers in database")

        response = client.get(
            "/api/analytics/compare",
            params={
                "manufacturers": sample_manufacturer,
                "date_from": "2023-01-01",
                "date_to": "2023-12-31"
            }
        )
        assert response.status_code == 200


class TestSafetySignals:
    """Tests for GET /api/analytics/signals endpoint."""

    def test_get_signals_default(self, client):
        """Test getting safety signals with default parameters."""
        response = client.get("/api/analytics/signals")
        assert response.status_code == 200

        data = response.json()
        assert "lookback_months" in data
        assert "signals" in data
        assert isinstance(data["signals"], list)

    def test_get_signals_custom_lookback(self, client):
        """Test getting signals with custom lookback period."""
        response = client.get("/api/analytics/signals", params={"lookback_months": 6})
        assert response.status_code == 200

        data = response.json()
        assert data["lookback_months"] == 6

    def test_get_signals_custom_threshold(self, client):
        """Test getting signals with custom threshold."""
        response = client.get("/api/analytics/signals", params={"min_threshold": 100})
        assert response.status_code == 200

    def test_signals_structure(self, client):
        """Test signal data structure."""
        response = client.get("/api/analytics/signals")
        assert response.status_code == 200

        data = response.json()
        if data["signals"]:
            signal = data["signals"][0]
            assert "manufacturer" in signal
            assert "avg_monthly" in signal
            assert "z_score" in signal
            assert "signal_type" in signal
            assert signal["signal_type"] in ["high", "elevated", "normal"]


class TestTextFrequency:
    """Tests for GET /api/analytics/text-frequency endpoint."""

    def test_get_text_frequency_default(self, client):
        """Test getting text frequency with default parameters."""
        response = client.get("/api/analytics/text-frequency")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "term" in data[0]
            assert "count" in data[0]
            assert "percentage" in data[0]

    def test_get_text_frequency_custom_params(self, client):
        """Test getting text frequency with custom parameters."""
        response = client.get(
            "/api/analytics/text-frequency",
            params={"min_word_length": 5, "top_n": 20, "sample_size": 500}
        )
        assert response.status_code == 200

        data = response.json()
        assert len(data) <= 20

    def test_get_text_frequency_with_filters(self, client):
        """Test getting text frequency with filters."""
        response = client.get(
            "/api/analytics/text-frequency",
            params={"event_types": "D"}
        )
        assert response.status_code == 200


class TestEventTypeDistribution:
    """Tests for GET /api/analytics/event-type-distribution endpoint."""

    def test_get_distribution(self, client):
        """Test getting event type distribution."""
        response = client.get("/api/analytics/event-type-distribution")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "type" in data[0]
            assert "count" in data[0]
            assert "percentage" in data[0]

    def test_get_distribution_filtered(self, client):
        """Test getting filtered distribution."""
        response = client.get(
            "/api/analytics/event-type-distribution",
            params={"date_from": "2023-01-01"}
        )
        assert response.status_code == 200
