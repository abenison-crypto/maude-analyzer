"""Filter autocomplete API router.

Provides autocomplete endpoints for device-specific filter fields
like brand names, generic names, model numbers, and device manufacturers.
"""

from fastapi import APIRouter, Query
from typing import Optional

from api.services.database import get_db

router = APIRouter()


@router.get("/brand-names")
async def list_brand_names(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
):
    """Get list of device brand names for autocomplete."""
    db = get_db()

    if search:
        query = """
            SELECT brand_name, COUNT(*) as count
            FROM devices
            WHERE brand_name IS NOT NULL
            AND brand_name != ''
            AND LOWER(brand_name) LIKE ?
            GROUP BY brand_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [f"%{search.lower()}%", limit]
    else:
        query = """
            SELECT brand_name, COUNT(*) as count
            FROM devices
            WHERE brand_name IS NOT NULL
            AND brand_name != ''
            GROUP BY brand_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [limit]

    results = db.fetch_all(query, params)
    return [{"value": r[0], "label": r[0], "count": r[1]} for r in results]


@router.get("/generic-names")
async def list_generic_names(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
):
    """Get list of device generic names for autocomplete."""
    db = get_db()

    if search:
        query = """
            SELECT generic_name, COUNT(*) as count
            FROM devices
            WHERE generic_name IS NOT NULL
            AND generic_name != ''
            AND LOWER(generic_name) LIKE ?
            GROUP BY generic_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [f"%{search.lower()}%", limit]
    else:
        query = """
            SELECT generic_name, COUNT(*) as count
            FROM devices
            WHERE generic_name IS NOT NULL
            AND generic_name != ''
            GROUP BY generic_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [limit]

    results = db.fetch_all(query, params)
    return [{"value": r[0], "label": r[0], "count": r[1]} for r in results]


@router.get("/device-manufacturers")
async def list_device_manufacturers(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
):
    """Get list of device manufacturers for autocomplete."""
    db = get_db()

    if search:
        query = """
            SELECT manufacturer_d_name, COUNT(*) as count
            FROM devices
            WHERE manufacturer_d_name IS NOT NULL
            AND manufacturer_d_name != ''
            AND LOWER(manufacturer_d_name) LIKE ?
            GROUP BY manufacturer_d_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [f"%{search.lower()}%", limit]
    else:
        query = """
            SELECT manufacturer_d_name, COUNT(*) as count
            FROM devices
            WHERE manufacturer_d_name IS NOT NULL
            AND manufacturer_d_name != ''
            GROUP BY manufacturer_d_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [limit]

    results = db.fetch_all(query, params)
    return [{"value": r[0], "label": r[0], "count": r[1]} for r in results]


@router.get("/model-numbers")
async def list_model_numbers(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
):
    """Get list of device model numbers for autocomplete."""
    db = get_db()

    if search:
        query = """
            SELECT model_number, COUNT(*) as count
            FROM devices
            WHERE model_number IS NOT NULL
            AND model_number != ''
            AND LOWER(model_number) LIKE ?
            GROUP BY model_number
            ORDER BY count DESC
            LIMIT ?
        """
        params = [f"%{search.lower()}%", limit]
    else:
        query = """
            SELECT model_number, COUNT(*) as count
            FROM devices
            WHERE model_number IS NOT NULL
            AND model_number != ''
            GROUP BY model_number
            ORDER BY count DESC
            LIMIT ?
        """
        params = [limit]

    results = db.fetch_all(query, params)
    return [{"value": r[0], "label": r[0], "count": r[1]} for r in results]


@router.get("/device-product-codes")
async def list_device_product_codes(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
):
    """Get list of device product codes for autocomplete."""
    db = get_db()

    if search:
        query = """
            SELECT d.device_report_product_code, pc.device_name, COUNT(*) as count
            FROM devices d
            LEFT JOIN product_codes pc ON d.device_report_product_code = pc.product_code
            WHERE d.device_report_product_code IS NOT NULL
            AND d.device_report_product_code != ''
            AND (LOWER(d.device_report_product_code) LIKE ? OR LOWER(pc.device_name) LIKE ?)
            GROUP BY d.device_report_product_code, pc.device_name
            ORDER BY count DESC
            LIMIT ?
        """
        search_term = f"%{search.lower()}%"
        params = [search_term, search_term, limit]
    else:
        query = """
            SELECT d.device_report_product_code, pc.device_name, COUNT(*) as count
            FROM devices d
            LEFT JOIN product_codes pc ON d.device_report_product_code = pc.product_code
            WHERE d.device_report_product_code IS NOT NULL
            AND d.device_report_product_code != ''
            GROUP BY d.device_report_product_code, pc.device_name
            ORDER BY count DESC
            LIMIT ?
        """
        params = [limit]

    results = db.fetch_all(query, params)
    return [
        {
            "value": r[0],
            "label": f"{r[0]} - {r[1]}" if r[1] else r[0],
            "count": r[2]
        }
        for r in results
    ]
