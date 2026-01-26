"""Pytest fixtures for API tests."""

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture(scope="module")
def client():
    """Create a TestClient for the FastAPI application."""
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def sample_manufacturer(client):
    """Get a sample manufacturer name for testing."""
    response = client.get("/api/events/manufacturers", params={"limit": 1})
    if response.status_code == 200 and response.json():
        return response.json()[0]["name"]
    return None


@pytest.fixture(scope="module")
def sample_product_code(client):
    """Get a sample product code for testing."""
    response = client.get("/api/events/product-codes", params={"limit": 1})
    if response.status_code == 200 and response.json():
        return response.json()[0]["code"]
    return None


@pytest.fixture(scope="module")
def sample_event_key(client):
    """Get a sample MDR report key for testing."""
    response = client.get("/api/events", params={"page_size": 1})
    if response.status_code == 200 and response.json().get("events"):
        return response.json()["events"][0]["mdr_report_key"]
    return None
