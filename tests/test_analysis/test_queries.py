"""Tests for analysis queries module."""

import pytest
import pandas as pd
from datetime import date


class TestSearchQuery:
    """Tests for SearchQuery class."""

    def test_add_condition(self, test_db):
        """Test adding search conditions."""
        from src.analysis.queries import SearchQuery

        query = SearchQuery()
        query.add_condition("event_type", "equals", "IN")

        assert len(query.conditions) == 1
        assert query.conditions[0].field == "event_type"
        assert query.conditions[0].operator == "equals"
        assert query.conditions[0].value == "IN"

    def test_add_manufacturers(self, test_db):
        """Test adding manufacturer filter."""
        from src.analysis.queries import SearchQuery

        query = SearchQuery()
        query.add_manufacturers(["Abbott", "Medtronic"])

        assert len(query.conditions) == 1
        assert query.conditions[0].operator == "in"

    def test_add_date_range(self, test_db):
        """Test adding date range filter."""
        from src.analysis.queries import SearchQuery

        query = SearchQuery()
        query.add_date_range(
            "date_received",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31)
        )

        assert len(query.conditions) == 1
        assert query.conditions[0].operator == "between"

    def test_build_sql(self, test_db):
        """Test SQL generation."""
        from src.analysis.queries import SearchQuery

        query = SearchQuery()
        query.add_manufacturers(["Abbott"])

        sql, params = query.build_sql()

        assert "SELECT" in sql
        assert "FROM master_events" in sql
        assert "WHERE" in sql
        assert "Abbott" in params

    def test_execute(self, test_db):
        """Test query execution."""
        from src.analysis.queries import SearchQuery

        query = SearchQuery()
        query.add_manufacturers(["Abbott"])

        result = query.execute(test_db)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # Abbott has 2 records in test data

    def test_count(self, test_db):
        """Test count query."""
        from src.analysis.queries import SearchQuery

        query = SearchQuery()
        query.add_event_types(["IN"])

        count = query.count(test_db)

        assert count == 2  # 2 injury records in test data


class TestQueryFunctions:
    """Tests for standalone query functions."""

    def test_get_mdr_summary(self, test_db):
        """Test MDR summary statistics."""
        from src.analysis.queries import get_mdr_summary

        summary = get_mdr_summary(test_db)

        assert summary["total_mdrs"] == 5
        assert summary["deaths"] == 1
        assert summary["injuries"] == 2
        assert summary["malfunctions"] == 2
        assert summary["unique_manufacturers"] == 4

    def test_get_manufacturer_comparison(self, test_db):
        """Test manufacturer comparison."""
        from src.analysis.queries import get_manufacturer_comparison

        result = get_manufacturer_comparison(
            manufacturers=["Abbott", "Medtronic"],
            conn=test_db
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "manufacturer_clean" in result.columns
        assert "total_mdrs" in result.columns

    def test_get_trend_data(self, test_db):
        """Test trend data retrieval."""
        from src.analysis.queries import get_trend_data

        result = get_trend_data(
            aggregation="monthly",
            conn=test_db
        )

        assert isinstance(result, pd.DataFrame)
        assert "period" in result.columns
        assert "total_mdrs" in result.columns

    def test_get_event_type_breakdown(self, test_db):
        """Test event type breakdown."""
        from src.analysis.queries import get_event_type_breakdown

        result = get_event_type_breakdown(conn=test_db)

        assert isinstance(result, pd.DataFrame)
        assert "event_type" in result.columns
        assert "count" in result.columns

    def test_get_filter_options(self, test_db):
        """Test filter options retrieval."""
        from src.analysis.queries import get_filter_options

        options = get_filter_options(test_db)

        assert "manufacturers" in options
        assert "product_codes" in options
        assert "event_types" in options
        assert "Abbott" in options["manufacturers"]
        assert "GZB" in options["product_codes"]
