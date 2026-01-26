"""Events API router."""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
from datetime import date
import io
import csv

from api.services.queries import QueryService
from api.models.schemas import (
    EventListResponse,
    EventDetail,
    StatsResponse,
    ManufacturerItem,
    ProductCodeItem,
)

router = APIRouter()


@router.get("", response_model=EventListResponse)
async def list_events(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    event_types: Optional[str] = Query(None, description="Comma-separated event types (D,I,M,O)"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
    search_text: Optional[str] = Query(None, description="Search text"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Results per page"),
):
    """List MDR events with filtering and pagination."""
    query_service = QueryService()

    # Parse comma-separated values
    mfr_list = manufacturers.split(",") if manufacturers else None
    code_list = product_codes.split(",") if product_codes else None
    type_list = event_types.split(",") if event_types else None

    result = query_service.get_events(
        manufacturers=mfr_list,
        product_codes=code_list,
        event_types=type_list,
        date_from=date_from,
        date_to=date_to,
        search_text=search_text,
        page=page,
        page_size=page_size,
    )

    return result


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    event_types: Optional[str] = Query(None, description="Comma-separated event types (D,I,M,O)"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
):
    """Get summary statistics for events matching filters."""
    query_service = QueryService()

    mfr_list = manufacturers.split(",") if manufacturers else None
    code_list = product_codes.split(",") if product_codes else None
    type_list = event_types.split(",") if event_types else None

    return query_service.get_event_stats(
        manufacturers=mfr_list,
        product_codes=code_list,
        event_types=type_list,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/manufacturers", response_model=list[ManufacturerItem])
async def list_manufacturers(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(100, ge=1, le=500, description="Max results"),
):
    """Get list of manufacturers for autocomplete."""
    query_service = QueryService()
    return query_service.get_manufacturer_list(search=search, limit=limit)


@router.get("/product-codes", response_model=list[ProductCodeItem])
async def list_product_codes(
    search: Optional[str] = Query(None, description="Search term"),
    limit: int = Query(100, ge=1, le=500, description="Max results"),
):
    """Get list of product codes for autocomplete."""
    query_service = QueryService()
    return query_service.get_product_code_list(search=search, limit=limit)


@router.get("/export")
async def export_events(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    event_types: Optional[str] = Query(None, description="Comma-separated event types (D,I,M,O)"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
    search_text: Optional[str] = Query(None, description="Search text"),
    format: str = Query("csv", description="Export format (csv)"),
    max_records: int = Query(10000, ge=1, le=100000, description="Maximum records to export"),
):
    """Export events to CSV."""
    query_service = QueryService()

    mfr_list = manufacturers.split(",") if manufacturers else None
    code_list = product_codes.split(",") if product_codes else None
    type_list = event_types.split(",") if event_types else None

    # Get events (limited to max_records)
    result = query_service.get_events(
        manufacturers=mfr_list,
        product_codes=code_list,
        event_types=type_list,
        date_from=date_from,
        date_to=date_to,
        search_text=search_text,
        page=1,
        page_size=max_records,
    )

    events = result["events"]

    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        "MDR Report Key", "Report Number", "Date Received", "Date of Event",
        "Event Type", "Manufacturer", "Product Code"
    ])

    # Data
    for event in events:
        writer.writerow([
            event["mdr_report_key"],
            event["report_number"],
            event["date_received"],
            event["date_of_event"],
            event["event_type"],
            event["manufacturer"],
            event["product_code"],
        ])

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=maude_events_{date.today().isoformat()}.csv"
        },
    )


@router.get("/{mdr_report_key}", response_model=EventDetail)
async def get_event(mdr_report_key: str):
    """Get detailed information for a single event."""
    query_service = QueryService()
    event = query_service.get_event_detail(mdr_report_key)

    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    return event
