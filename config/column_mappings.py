"""
FDA-to-Database Column Mappings.

This module defines the mapping between FDA MAUDE column names (uppercase,
as they appear in FDA files) and the database column names (lowercase).

IMPORTANT: These mappings must match the column names in schema_registry.py
exactly. If you update one file, update this one too.

The mappings handle:
1. Case conversion (FDA uses UPPERCASE)
2. Name normalization (removing truncation, etc.)
3. Semantic renaming where needed for clarity
"""

from typing import Dict, List, Optional

# =============================================================================
# MASTER FILE COLUMN MAPPING (86 columns)
# Maps FDA column names to database column names
# =============================================================================

MASTER_COLUMN_MAPPING: Dict[str, str] = {
    # Key identifiers
    "MDR_REPORT_KEY": "mdr_report_key",
    "EVENT_KEY": "event_key",
    "REPORT_NUMBER": "report_number",
    "REPORT_SOURCE_CODE": "report_source_code",
    "MANUFACTURER_LINK_FLAG_": "manufacturer_link_flag_old",  # Note: trailing underscore in FDA

    # Event counts
    "NUMBER_DEVICES_IN_EVENT": "number_devices_in_event",
    "NUMBER_PATIENTS_IN_EVENT": "number_patients_in_event",

    # Key dates
    "DATE_RECEIVED": "date_received",
    "DATE_REPORT": "date_report",
    "DATE_OF_EVENT": "date_of_event",

    # Event flags (Y/N)
    "ADVERSE_EVENT_FLAG": "adverse_event_flag",
    "PRODUCT_PROBLEM_FLAG": "product_problem_flag",
    "REPROCESSED_AND_REUSED_FLAG": "reprocessed_and_reused_flag",

    # Reporter information
    "REPORTER_OCCUPATION_CODE": "reporter_occupation_code",
    "HEALTH_PROFESSIONAL": "health_professional",
    "INITIAL_REPORT_TO_FDA": "initial_report_to_fda",
    "REPORTER_STATE_CODE": "reporter_state_code",
    "REPORTER_COUNTRY_CODE": "reporter_country_code",

    # Facility dates
    "DATE_FACILITY_AWARE": "date_facility_aware",
    "REPORT_DATE": "report_date",
    "REPORT_TO_FDA": "report_to_fda",
    "DATE_REPORT_TO_FDA": "date_report_to_fda",

    # Event details
    "EVENT_LOCATION": "event_location",
    "EVENT_TYPE": "event_type",

    # Report to manufacturer
    "DATE_REPORT_TO_MANUFACTURER": "date_report_to_manufacturer",
    "REPORT_TO_MANUFACTURER": "report_to_manufacturer",
    "DATE_MANUFACTURER_RECEIVED": "date_manufacturer_received",

    # Manufacturer contact - name and address
    "MANUFACTURER_CONTACT_T_NAME": "manufacturer_contact_title",
    "MANUFACTURER_CONTACT_F_NAME": "manufacturer_contact_first_name",
    "MANUFACTURER_CONTACT_L_NAME": "manufacturer_contact_last_name",
    "MANUFACTURER_CONTACT_STREET_1": "manufacturer_contact_address_1",
    "MANUFACTURER_CONTACT_STREET_2": "manufacturer_contact_address_2",
    "MANUFACTURER_CONTACT_CITY": "manufacturer_contact_city",
    "MANUFACTURER_CONTACT_STATE": "manufacturer_contact_state",
    "MANUFACTURER_CONTACT_ZIP_CODE": "manufacturer_contact_zip",
    "MANUFACTURER_CONTACT_ZIP_EXT": "manufacturer_contact_zip_ext",
    "MANUFACTURER_CONTACT_COUNTRY": "manufacturer_contact_country",
    "MANUFACTURER_CONTACT_POSTAL": "manufacturer_contact_postal",

    # Manufacturer contact - phone (FDA splits into multiple fields)
    "MANUFACTURER_CONTACT_AREA_CODE": "manufacturer_contact_area_code",
    "MANUFACTURER_CONTACT_EXCHANGE": "manufacturer_contact_exchange",
    "MANUFACTURER_CONTACT_PHONE_NO": "manufacturer_contact_phone_no",
    "MANUFACTURER_CONTACT_EXTENSION": "manufacturer_contact_extension",
    "MANUFACTURER_CONTACT_PCOUNTRY": "manufacturer_contact_pcountry",
    "MANUFACTURER_CONTACT_PCITY": "manufacturer_contact_pcity",
    "MANUFACTURER_CONTACT_PLOCAL": "manufacturer_contact_plocal",

    # Global manufacturer (G1) info
    "MANUFACTURER_G1_NAME": "manufacturer_g1_name",
    "MANUFACTURER_G1_STREET_1": "manufacturer_g1_street_1",
    "MANUFACTURER_G1_STREET_2": "manufacturer_g1_street_2",
    "MANUFACTURER_G1_CITY": "manufacturer_g1_city",
    "MANUFACTURER_G1_STATE_CODE": "manufacturer_g1_state",
    "MANUFACTURER_G1_ZIP_CODE": "manufacturer_g1_zip",
    "MANUFACTURER_G1_ZIP_CODE_EXT": "manufacturer_g1_zip_ext",
    "MANUFACTURER_G1_COUNTRY_CODE": "manufacturer_g1_country",
    "MANUFACTURER_G1_POSTAL_CODE": "manufacturer_g1_postal",

    # Device manufacturing
    "DEVICE_DATE_OF_MANUFACTURE": "device_date_of_manufacture",
    "MFR_REPORT_TYPE": "mfr_report_type",

    # Device flags
    "SINGLE_USE_FLAG": "single_use_flag",
    "REMEDIAL_ACTION": "remedial_action",
    "PREVIOUS_USE_CODE": "previous_use_code",
    "REMOVAL_CORRECTION_NUMBER": "removal_correction_number",

    # Distributor info
    "DISTRIBUTOR_NAME": "distributor_name",
    "DISTRIBUTOR_ADDRESS_1": "distributor_address_1",
    "DISTRIBUTOR_ADDRESS_2": "distributor_address_2",
    "DISTRIBUTOR_CITY": "distributor_city",
    "DISTRIBUTOR_STATE_CODE": "distributor_state",
    "DISTRIBUTOR_ZIP_CODE": "distributor_zip",
    "DISTRIBUTOR_ZIP_CODE_EXT": "distributor_zip_ext",

    # Report type
    "TYPE_OF_REPORT": "type_of_report",

    # Main manufacturer info
    "MANUFACTURER_NAME": "manufacturer_name",
    "MANUFACTURER_ADDRESS_1": "manufacturer_address_1",
    "MANUFACTURER_ADDRESS_2": "manufacturer_address_2",
    "MANUFACTURER_CITY": "manufacturer_city",
    "MANUFACTURER_STATE_CODE": "manufacturer_state",
    "MANUFACTURER_ZIP_CODE": "manufacturer_zip",
    "MANUFACTURER_ZIP_CODE_EXT": "manufacturer_zip_ext",
    "MANUFACTURER_COUNTRY_CODE": "manufacturer_country",
    "MANUFACTURER_POSTAL_CODE": "manufacturer_postal",

    # Report classification
    "SOURCE_TYPE": "source_type",

    # Metadata dates
    "DATE_ADDED": "date_added",
    "DATE_CHANGED": "date_changed",

    # Product identification - Note: FDA uses abbreviated names
    "PMA_PMN_NUM": "pma_pmn_number",  # FDA: PMA_PMN_NUM, DB: pma_pmn_number
    "EXEMPTION_NUMBER": "exemption_number",
    "SUMMARY_REPORT": "summary_report_flag",  # FDA: SUMMARY_REPORT, DB: summary_report_flag

    # Supplemental info - Note: FDA uses abbreviated names
    "NOE_SUMMARIZED": "noe_summarized",
    "SUPPL_DATES_FDA_RECEIVED": "supplemental_dates_fda_received",  # FDA abbreviated
    "SUPPL_DATES_MFR_RECEIVED": "supplemental_dates_mfr_received",  # FDA abbreviated
}

# =============================================================================
# DEVICE FILE COLUMN MAPPING (28 columns)
# =============================================================================

DEVICE_COLUMN_MAPPING: Dict[str, str] = {
    # Core columns (all device file versions)
    "MDR_REPORT_KEY": "mdr_report_key",
    "DEVICE_EVENT_KEY": "device_event_key",
    "IMPLANT_FLAG": "implant_flag",
    "DATE_REMOVED_FLAG": "date_removed_flag",
    "DEVICE_SEQUENCE_NO": "device_sequence_number",  # FDA: NO, DB: NUMBER

    # New columns added in 2020+ format (positions 6-8 in 34-col files)
    "IMPLANT_DATE_YEAR": "implant_date_year",
    "DATE_REMOVED_YEAR": "date_removed_year",
    "SERVICED_BY_3RD_PARTY_FLAG": "serviced_by_3rd_party_flag",

    # Core columns (continue from both formats)
    "DATE_RECEIVED": "date_received",
    "BRAND_NAME": "brand_name",
    "GENERIC_NAME": "generic_name",
    "MANUFACTURER_D_NAME": "manufacturer_d_name",
    "MANUFACTURER_D_ADDRESS_1": "manufacturer_d_address_1",
    "MANUFACTURER_D_ADDRESS_2": "manufacturer_d_address_2",
    "MANUFACTURER_D_CITY": "manufacturer_d_city",
    "MANUFACTURER_D_STATE_CODE": "manufacturer_d_state",
    "MANUFACTURER_D_ZIP_CODE": "manufacturer_d_zip",
    "MANUFACTURER_D_ZIP_CODE_EXT": "manufacturer_d_zip_ext",
    "MANUFACTURER_D_COUNTRY_CODE": "manufacturer_d_country",
    "MANUFACTURER_D_POSTAL_CODE": "manufacturer_d_postal",
    "EXPIRATION_DATE_OF_DEVICE": "expiration_date_of_device",
    "MODEL_NUMBER": "model_number",
    "CATALOG_NUMBER": "catalog_number",
    "LOT_NUMBER": "lot_number",
    "OTHER_ID_NUMBER": "other_id_number",
    "DEVICE_OPERATOR": "device_operator",
    "DEVICE_AVAILABILITY": "device_availability",
    "DATE_RETURNED_TO_MANUFACTURER": "date_returned_to_manufacturer",
    "DEVICE_REPORT_PRODUCT_CODE": "device_report_product_code",
    "DEVICE_AGE_TEXT": "device_age_text",
    "DEVICE_EVALUATED_BY_MANUFACTUR": "device_evaluated_by_manufacturer",  # FDA truncated

    # New columns at end in 2020+ format (positions 32-34 in 34-col files)
    "COMBINATION_PRODUCT_FLAG": "combination_product_flag",
    "UDI_DI": "udi_di",      # Unique Device Identifier - Device ID
    "UDI-DI": "udi_di",      # Handle hyphenated version from some files
    "UDI_PUBLIC": "udi_public",  # Unique Device Identifier - Public
    "UDI-PUBLIC": "udi_public",  # Handle hyphenated version from some files
}

# =============================================================================
# PATIENT FILE COLUMN MAPPING (10 columns)
# =============================================================================

PATIENT_COLUMN_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "PATIENT_SEQUENCE_NUMBER": "patient_sequence_number",
    "DATE_RECEIVED": "date_received",
    "SEQUENCE_NUMBER_TREATMENT": "sequence_number_treatment",
    "SEQUENCE_NUMBER_OUTCOME": "sequence_number_outcome",
    "PATIENT_AGE": "patient_age",
    "PATIENT_SEX": "patient_sex",
    "PATIENT_WEIGHT": "patient_weight",
    "PATIENT_ETHNICITY": "patient_ethnicity",
    "PATIENT_RACE": "patient_race",
}

# =============================================================================
# TEXT FILE COLUMN MAPPING (6 columns)
# =============================================================================

TEXT_COLUMN_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "MDR_TEXT_KEY": "mdr_text_key",
    "TEXT_TYPE_CODE": "text_type_code",
    "PATIENT_SEQUENCE_NUMBER": "patient_sequence_number",
    "DATE_REPORT": "date_report",
    "FOI_TEXT": "text_content",  # Rename for clarity
}

# =============================================================================
# PROBLEM FILE COLUMN MAPPING (2 columns)
# =============================================================================

PROBLEM_COLUMN_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "DEVICE_PROBLEM_CODE": "device_problem_code",
}

# =============================================================================
# PATIENT PROBLEM FILE COLUMN MAPPING (5 columns)
# =============================================================================

PATIENT_PROBLEM_COLUMN_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "PATIENT_SEQUENCE_NO": "patient_sequence_number",
    "PROBLEM_CODE": "patient_problem_code",
    "DATE_ADDED": "date_added",
    "DATE_CHANGED": "date_changed",
}

# =============================================================================
# PROBLEM CODES LOOKUP COLUMN MAPPING (2 columns)
# =============================================================================

PROBLEM_LOOKUP_COLUMN_MAPPING: Dict[str, str] = {
    "DEVICE_PROBLEM_CODE": "problem_code",
    "DEVICE_PROBLEM_DESCRIPTION": "description",
}

# =============================================================================
# PATIENT PROBLEM DATA LOOKUP COLUMN MAPPING (2 columns)
# =============================================================================

PATIENT_PROBLEM_DATA_COLUMN_MAPPING: Dict[str, str] = {
    "PROBLEM_CODE": "problem_code",
    "PROBLEM_DESCRIPTION": "description",
}

# =============================================================================
# ASR REPORT COLUMN MAPPING (18 columns)
# =============================================================================

ASR_COLUMN_MAPPING: Dict[str, str] = {
    "REPORT_ID": "report_id",
    "REPORT_YEAR": "report_year",
    "BRAND_NAME": "brand_name",
    "GENERIC_NAME": "generic_name",
    "MANUFACTURER_NAME": "manufacturer_name",
    "PRODUCT_CODE": "product_code",
    "DEVICE_CLASS": "device_class",
    "REPORT_COUNT": "report_count",
    "EVENT_COUNT": "event_count",
    "DEATH_COUNT": "death_count",
    "INJURY_COUNT": "injury_count",
    "MALFUNCTION_COUNT": "malfunction_count",
    "DATE_START": "date_start",
    "DATE_END": "date_end",
    "EXEMPTION_NUMBER": "exemption_number",
    "PMA_PMN_NUMBER": "pma_pmn_number",
    "SUBMISSION_TYPE": "submission_type",
    "SUMMARY_TEXT": "summary_text",
}

# =============================================================================
# ASR PATIENT PROBLEM CODES COLUMN MAPPING (3 columns)
# =============================================================================

ASR_PPC_COLUMN_MAPPING: Dict[str, str] = {
    "REPORT_ID": "report_id",
    "PATIENT_PROBLEM_CODE": "patient_problem_code",
    "OCCURRENCE_COUNT": "occurrence_count",
}

# =============================================================================
# DEN LEGACY COLUMN MAPPING (19 columns)
# =============================================================================

DEN_COLUMN_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "REPORT_NUMBER": "report_number",
    "REPORT_SOURCE": "report_source",
    "DATE_RECEIVED": "date_received",
    "DATE_OF_EVENT": "date_of_event",
    "DATE_REPORT": "date_report",
    "BRAND_NAME": "brand_name",
    "GENERIC_NAME": "generic_name",
    "MODEL_NUMBER": "model_number",
    "CATALOG_NUMBER": "catalog_number",
    "LOT_NUMBER": "lot_number",
    "DEVICE_OPERATOR": "device_operator",
    "MANUFACTURER_NAME": "manufacturer_name",
    "MANUFACTURER_CITY": "manufacturer_city",
    "MANUFACTURER_STATE": "manufacturer_state",
    "MANUFACTURER_COUNTRY": "manufacturer_country",
    "EVENT_TYPE": "event_type",
    "EVENT_DESCRIPTION": "event_description",
    "PATIENT_OUTCOME": "patient_outcome",
    "REPORT_YEAR": "report_year",  # Derived field
}

# =============================================================================
# DISCLAIMER COLUMN MAPPING (3 columns)
# =============================================================================

DISCLAIMER_COLUMN_MAPPING: Dict[str, str] = {
    "MANUFACTURER_NAME": "manufacturer_name",
    "DISCLAIMER_TEXT": "disclaimer_text",
    "EFFECTIVE_DATE": "effective_date",
}

# =============================================================================
# Combined mapping by file type
# =============================================================================

COLUMN_MAPPINGS: Dict[str, Dict[str, str]] = {
    "master": MASTER_COLUMN_MAPPING,
    "device": DEVICE_COLUMN_MAPPING,
    "patient": PATIENT_COLUMN_MAPPING,
    "text": TEXT_COLUMN_MAPPING,
    "problem": PROBLEM_COLUMN_MAPPING,
    "patient_problem": PATIENT_PROBLEM_COLUMN_MAPPING,
    "patient_problem_data": PATIENT_PROBLEM_DATA_COLUMN_MAPPING,
    "problem_lookup": PROBLEM_LOOKUP_COLUMN_MAPPING,
    "asr": ASR_COLUMN_MAPPING,
    "asr_ppc": ASR_PPC_COLUMN_MAPPING,
    "den": DEN_COLUMN_MAPPING,
    "disclaimer": DISCLAIMER_COLUMN_MAPPING,
}

# =============================================================================
# Reverse mappings (DB column -> FDA column)
# =============================================================================

REVERSE_MASTER_MAPPING = {v: k for k, v in MASTER_COLUMN_MAPPING.items()}
REVERSE_DEVICE_MAPPING = {v: k for k, v in DEVICE_COLUMN_MAPPING.items()}
REVERSE_PATIENT_MAPPING = {v: k for k, v in PATIENT_COLUMN_MAPPING.items()}
REVERSE_TEXT_MAPPING = {v: k for k, v in TEXT_COLUMN_MAPPING.items()}
REVERSE_PROBLEM_MAPPING = {v: k for k, v in PROBLEM_COLUMN_MAPPING.items()}
REVERSE_PATIENT_PROBLEM_MAPPING = {v: k for k, v in PATIENT_PROBLEM_COLUMN_MAPPING.items()}
REVERSE_PATIENT_PROBLEM_DATA_MAPPING = {v: k for k, v in PATIENT_PROBLEM_DATA_COLUMN_MAPPING.items()}
REVERSE_PROBLEM_LOOKUP_MAPPING = {v: k for k, v in PROBLEM_LOOKUP_COLUMN_MAPPING.items()}
REVERSE_ASR_MAPPING = {v: k for k, v in ASR_COLUMN_MAPPING.items()}
REVERSE_ASR_PPC_MAPPING = {v: k for k, v in ASR_PPC_COLUMN_MAPPING.items()}
REVERSE_DEN_MAPPING = {v: k for k, v in DEN_COLUMN_MAPPING.items()}
REVERSE_DISCLAIMER_MAPPING = {v: k for k, v in DISCLAIMER_COLUMN_MAPPING.items()}

REVERSE_COLUMN_MAPPINGS: Dict[str, Dict[str, str]] = {
    "master": REVERSE_MASTER_MAPPING,
    "device": REVERSE_DEVICE_MAPPING,
    "patient": REVERSE_PATIENT_MAPPING,
    "text": REVERSE_TEXT_MAPPING,
    "problem": REVERSE_PROBLEM_MAPPING,
    "patient_problem": REVERSE_PATIENT_PROBLEM_MAPPING,
    "patient_problem_data": REVERSE_PATIENT_PROBLEM_DATA_MAPPING,
    "problem_lookup": REVERSE_PROBLEM_LOOKUP_MAPPING,
    "asr": REVERSE_ASR_MAPPING,
    "asr_ppc": REVERSE_ASR_PPC_MAPPING,
    "den": REVERSE_DEN_MAPPING,
    "disclaimer": REVERSE_DISCLAIMER_MAPPING,
}

# =============================================================================
# Helper functions
# =============================================================================


def get_db_column_name(fda_column: str, file_type: str) -> Optional[str]:
    """
    Get the database column name for an FDA column.

    Args:
        fda_column: FDA column name (uppercase)
        file_type: Type of MAUDE file

    Returns:
        Database column name or None if not found
    """
    mapping = COLUMN_MAPPINGS.get(file_type, {})
    return mapping.get(fda_column.upper())


def get_fda_column_name(db_column: str, file_type: str) -> Optional[str]:
    """
    Get the FDA column name for a database column.

    Args:
        db_column: Database column name (lowercase)
        file_type: Type of MAUDE file

    Returns:
        FDA column name or None if not found
    """
    mapping = REVERSE_COLUMN_MAPPINGS.get(file_type, {})
    return mapping.get(db_column.lower())


def map_record_columns(
    record: Dict[str, any],
    file_type: str,
    to_db: bool = True
) -> Dict[str, any]:
    """
    Map record column names between FDA and database formats.

    Args:
        record: Record dictionary
        file_type: Type of MAUDE file
        to_db: If True, map FDA->DB; if False, map DB->FDA

    Returns:
        Record with mapped column names
    """
    if to_db:
        mapping = COLUMN_MAPPINGS.get(file_type, {})
    else:
        mapping = REVERSE_COLUMN_MAPPINGS.get(file_type, {})

    mapped = {}
    for key, value in record.items():
        # Normalize key for lookup
        lookup_key = key.upper() if to_db else key.lower()
        new_key = mapping.get(lookup_key)

        if new_key:
            mapped[new_key] = value
        else:
            # If no mapping found, use lowercase version of key
            mapped[key.lower() if to_db else key] = value

    return mapped


def get_all_db_columns(file_type: str) -> List[str]:
    """
    Get all database column names for a file type.

    Args:
        file_type: Type of MAUDE file

    Returns:
        List of database column names
    """
    mapping = COLUMN_MAPPINGS.get(file_type, {})
    return list(mapping.values())


def get_all_fda_columns(file_type: str) -> List[str]:
    """
    Get all FDA column names for a file type.

    Args:
        file_type: Type of MAUDE file

    Returns:
        List of FDA column names
    """
    mapping = COLUMN_MAPPINGS.get(file_type, {})
    return list(mapping.keys())


def get_unmapped_columns(record: Dict[str, any], file_type: str) -> List[str]:
    """
    Get list of columns in record that don't have mappings.

    Useful for debugging schema mismatches.

    Args:
        record: Record dictionary with FDA column names
        file_type: Type of MAUDE file

    Returns:
        List of unmapped column names
    """
    mapping = COLUMN_MAPPINGS.get(file_type, {})
    unmapped = []
    for key in record.keys():
        if key.upper() not in mapping:
            unmapped.append(key)
    return unmapped
