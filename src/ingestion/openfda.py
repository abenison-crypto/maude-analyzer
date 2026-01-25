"""openFDA API client for querying MAUDE data.

IMPORTANT: DO NOT use this module for bulk data ingestion.

The openFDA API returns partial data (~20-25 fields) compared to
FDA download files (86+ fields). Missing fields include:
- Complete manufacturer contact info
- Distributor information
- Phone/address details
- Full reporter details

Use this module ONLY for:
- Real-time alerts and monitoring
- Quick count queries
- Ad-hoc lookups
- Searching for specific records

For data ingestion, use FDA download files via the download.py module.
"""

import time
import requests
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Generator
from dataclasses import dataclass, field
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger

logger = get_logger("openfda")

# openFDA API endpoints
OPENFDA_BASE_URL = "https://api.fda.gov/device/event.json"

# Rate limiting
DEFAULT_RATE_LIMIT = 40  # requests per minute without API key
API_KEY_RATE_LIMIT = 240  # requests per minute with API key


@dataclass
class OpenFDAResult:
    """Result of an openFDA API query."""

    total_records: int = 0
    records_fetched: int = 0
    records: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    duration_seconds: float = 0


class OpenFDAClient:
    """Client for the openFDA Device Adverse Event API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        rate_limit: Optional[int] = None,
    ):
        """
        Initialize the openFDA client.

        Args:
            api_key: FDA API key (optional, but recommended for higher rate limits).
            rate_limit: Requests per minute (auto-detected based on API key).
        """
        self.api_key = api_key or config.api.fda_api_key
        self.rate_limit = rate_limit or (
            API_KEY_RATE_LIMIT if self.api_key else DEFAULT_RATE_LIMIT
        )
        self.session = requests.Session()
        self.last_request_time = 0.0
        self._request_count = 0

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        min_interval = 60.0 / self.rate_limit
        elapsed = time.time() - self.last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def _make_request(
        self,
        search: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Make a single API request.

        Args:
            search: Search query string.
            skip: Number of records to skip.
            limit: Maximum records to return (max 100).

        Returns:
            API response as dictionary.
        """
        self._wait_for_rate_limit()

        params = {
            "skip": skip,
            "limit": min(limit, 100),  # API max is 100
        }

        if search:
            params["search"] = search

        if self.api_key:
            params["api_key"] = self.api_key

        logger.debug(f"API request: skip={skip}, limit={limit}")

        try:
            response = self.session.get(
                OPENFDA_BASE_URL,
                params=params,
                timeout=60,
            )
            self.last_request_time = time.time()
            self._request_count += 1

            if response.status_code == 404:
                # No results found
                return {"meta": {"results": {"total": 0}}, "results": []}

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(search, skip, limit)
            raise

    def search(
        self,
        product_codes: Optional[List[str]] = None,
        manufacturers: Optional[List[str]] = None,
        date_received_start: Optional[date] = None,
        date_received_end: Optional[date] = None,
        event_type: Optional[str] = None,
        max_records: int = 1000,
    ) -> OpenFDAResult:
        """
        Search for device adverse events.

        Args:
            product_codes: Filter by product codes.
            manufacturers: Filter by manufacturer names.
            date_received_start: Start date for date_received.
            date_received_end: End date for date_received.
            event_type: Filter by event type (D, IN, M).
            max_records: Maximum records to fetch.

        Returns:
            OpenFDAResult with matching records.
        """
        start_time = datetime.now()
        result = OpenFDAResult()

        # Build search query
        # Note: openFDA uses space as AND, and quotes for exact matches
        search_parts = []

        if product_codes:
            # Use OR for multiple product codes
            codes_query = " OR ".join([f'device.device_report_product_code:{c}' for c in product_codes])
            if len(product_codes) > 1:
                codes_query = f"({codes_query})"
            search_parts.append(codes_query)

        if manufacturers:
            mfr_query = " OR ".join([f'device.manufacturer_d_name:"{m}"' for m in manufacturers])
            if len(manufacturers) > 1:
                mfr_query = f"({mfr_query})"
            search_parts.append(mfr_query)

        if date_received_start or date_received_end:
            start_str = date_received_start.strftime("%Y%m%d") if date_received_start else "*"
            end_str = date_received_end.strftime("%Y%m%d") if date_received_end else "*"
            search_parts.append(f"date_received:[{start_str} TO {end_str}]")

        if event_type:
            search_parts.append(f'event_type:{event_type}')

        search_query = " AND ".join(search_parts) if search_parts else None

        try:
            # First request to get total count
            response = self._make_request(search=search_query, skip=0, limit=1)
            result.total_records = response.get("meta", {}).get("results", {}).get("total", 0)

            if result.total_records == 0:
                logger.info("No records found matching search criteria")
                return result

            logger.info(f"Found {result.total_records:,} matching records")

            # Fetch records in batches
            records_to_fetch = min(result.total_records, max_records)
            skip = 0

            while result.records_fetched < records_to_fetch:
                batch_size = min(100, records_to_fetch - result.records_fetched)
                response = self._make_request(
                    search=search_query,
                    skip=skip,
                    limit=batch_size,
                )

                batch = response.get("results", [])
                if not batch:
                    break

                result.records.extend(batch)
                result.records_fetched += len(batch)
                skip += len(batch)

                logger.debug(f"Fetched {result.records_fetched}/{records_to_fetch} records")

        except Exception as e:
            logger.error(f"openFDA API error: {e}")
            result.error = str(e)

        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def get_recent_events(
        self,
        days: int = 30,
        product_codes: Optional[List[str]] = None,
        max_records: int = 5000,
    ) -> OpenFDAResult:
        """
        Get recent device adverse events.

        Args:
            days: Number of days back to search.
            product_codes: Optional list of product codes to filter by (None = all).
            max_records: Maximum records to return.

        Returns:
            OpenFDAResult with recent events.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        return self.search(
            product_codes=product_codes,
            date_received_start=start_date,
            date_received_end=end_date,
            max_records=max_records,
        )

    # Backward compatibility alias
    def get_recent_scs_events(
        self,
        days: int = 30,
        max_records: int = 5000,
    ) -> OpenFDAResult:
        """
        Get recent SCS device adverse events.

        .. deprecated::
            Use get_recent_events() with product_codes parameter instead.

        Args:
            days: Number of days back to search.
            max_records: Maximum records to return.

        Returns:
            OpenFDAResult with recent SCS events.
        """
        # Legacy SCS product codes
        scs_codes = ["GZB", "LGW", "PMP"]
        return self.get_recent_events(
            days=days,
            product_codes=scs_codes,
            max_records=max_records,
        )

    def get_events_since(
        self,
        since_date: date,
        product_codes: Optional[List[str]] = None,
        max_records: int = 10000,
    ) -> OpenFDAResult:
        """
        Get all events since a specific date.

        Args:
            since_date: Start date for records.
            product_codes: Filter by product codes (None = all products).
            max_records: Maximum records to return.

        Returns:
            OpenFDAResult with events since the date.
        """
        return self.search(
            product_codes=product_codes,
            date_received_start=since_date,
            date_received_end=date.today(),
            max_records=max_records,
        )

    def transform_to_maude_format(
        self,
        openfda_record: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Transform an openFDA record to MAUDE database format.

        .. deprecated::
            This method is intended for data ingestion, which is now deprecated.
            openFDA API returns only ~20-25 fields vs 86+ in FDA download files.
            Use FDA download files for data ingestion to get complete records.

            Missing fields include:
            - manufacturer_contact_* (address, phone details)
            - manufacturer_g1_* (global manufacturer info)
            - distributor_* (distributor information)
            - Full reporter details
            - device_date_of_manufacture
            - And many more...

        Args:
            openfda_record: Record from openFDA API.

        Returns:
            Record in MAUDE database format (PARTIAL - many fields will be null).
        """
        import warnings
        warnings.warn(
            "transform_to_maude_format() creates partial records. openFDA API "
            "returns ~20-25 fields vs 86+ in FDA download files. Use FDA "
            "download files for complete data ingestion.",
            DeprecationWarning,
            stacklevel=2
        )
        # Extract device info (take first device if multiple)
        devices = openfda_record.get("device", [])
        device = devices[0] if devices else {}

        # Extract patient info
        patients = openfda_record.get("patient", [])
        patient = patients[0] if patients else {}

        # Parse dates
        date_received = self._parse_date(openfda_record.get("date_received"))
        date_of_event = self._parse_date(openfda_record.get("date_of_event"))

        # Map event type from openFDA format to MAUDE codes
        event_type_map = {
            "Injury": "IN",
            "Malfunction": "M",
            "Death": "D",
            "Other": "O",
            "No answer provided": "O",
        }
        raw_event_type = openfda_record.get("event_type", "")
        event_type = event_type_map.get(raw_event_type, raw_event_type)

        # Build master record
        master_record = {
            "mdr_report_key": openfda_record.get("mdr_report_key"),
            "event_key": openfda_record.get("event_key"),
            "report_number": openfda_record.get("report_number"),
            "date_received": date_received,
            "date_of_event": date_of_event,
            "manufacturer_name": device.get("manufacturer_d_name"),
            "product_code": device.get("device_report_product_code"),
            "event_type": event_type,
            "type_of_report": openfda_record.get("type_of_report", [None])[0],
            "product_problem_flag": openfda_record.get("product_problem_flag"),
            "adverse_event_flag": openfda_record.get("adverse_event_flag"),
            "report_source_code": openfda_record.get("report_source_code"),
            "event_location": openfda_record.get("event_location"),
            "pma_pmn_number": device.get("device_operator"),
        }

        # Build device record
        device_record = {
            "mdr_report_key": openfda_record.get("mdr_report_key"),
            "device_event_key": device.get("device_event_key"),
            "device_sequence_number": device.get("device_sequence_number"),
            "brand_name": device.get("brand_name"),
            "generic_name": device.get("generic_name"),
            "manufacturer_d_name": device.get("manufacturer_d_name"),
            "manufacturer_d_city": device.get("manufacturer_d_city"),
            "manufacturer_d_state": device.get("manufacturer_d_state"),
            "manufacturer_d_country": device.get("manufacturer_d_country"),
            "device_report_product_code": device.get("device_report_product_code"),
            "model_number": device.get("model_number"),
            "catalog_number": device.get("catalog_number"),
            "lot_number": device.get("lot_number"),
            "device_availability": device.get("device_availability"),
            "device_age_text": device.get("device_age_text"),
        }

        # Build patient record
        patient_record = {
            "mdr_report_key": openfda_record.get("mdr_report_key"),
            "patient_sequence_number": patient.get("patient_sequence_number", 1),
            "date_received": date_received,
            "sequence_number_treatment": patient.get("sequence_number_treatment"),
            "sequence_number_outcome": patient.get("sequence_number_outcome"),
        }

        # Build text records
        text_records = []
        for mdr_text in openfda_record.get("mdr_text", []):
            text_records.append({
                "mdr_report_key": openfda_record.get("mdr_report_key"),
                "text_type_code": mdr_text.get("text_type_code"),
                "text_content": mdr_text.get("text"),
            })

        return {
            "master": master_record,
            "device": device_record,
            "patient": patient_record,
            "text": text_records,
        }

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse openFDA date string (YYYYMMDD format)."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str[:8], "%Y%m%d").date()
        except (ValueError, TypeError):
            return None


def fetch_recent_updates(
    days: int = 30,
    product_codes: Optional[List[str]] = None,
    api_key: Optional[str] = None,
) -> OpenFDAResult:
    """
    Convenience function to fetch recent updates.

    Args:
        days: Number of days back to search.
        product_codes: Optional product codes to filter by (None = all).
        api_key: Optional FDA API key.

    Returns:
        OpenFDAResult with recent events.
    """
    client = OpenFDAClient(api_key=api_key)
    return client.get_recent_events(days=days, product_codes=product_codes)


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Query openFDA Device Events API")
    parser.add_argument("--days", type=int, default=30, help="Days back to search")
    parser.add_argument("--max", type=int, default=100, help="Max records to fetch")
    parser.add_argument("--codes", type=str, help="Product codes (comma-separated)")
    parser.add_argument("--output", type=Path, help="Output JSON file")

    args = parser.parse_args()

    # Parse product codes if provided
    product_codes = args.codes.split(",") if args.codes else None

    client = OpenFDAClient()
    result = client.get_recent_events(
        days=args.days,
        product_codes=product_codes,
        max_records=args.max,
    )

    print(f"Total available: {result.total_records:,}")
    print(f"Fetched: {result.records_fetched:,}")
    print(f"Duration: {result.duration_seconds:.1f}s")

    if result.error:
        print(f"Error: {result.error}")

    if args.output and result.records:
        with open(args.output, "w") as f:
            json.dump(result.records, f, indent=2, default=str)
        print(f"Saved to: {args.output}")
    elif result.records:
        print(f"\nFirst record MDR key: {result.records[0].get('mdr_report_key')}")
