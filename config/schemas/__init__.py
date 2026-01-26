"""FDA MAUDE Schema Definitions by File Type and Era.

This package contains historical and current schema definitions for all FDA MAUDE
file types, accounting for schema evolution over time.
"""

from .device_schemas import (
    DEVICE_SCHEMA_CURRENT,
    DEVICE_SCHEMA_1998_2010,
    DEVICE_SCHEMA_PRE_1998,
    get_device_schema,
)
from .master_schemas import (
    MASTER_SCHEMA_86_COLUMNS,
    MASTER_SCHEMA_84_COLUMNS,
    get_master_schema,
)
from .text_schemas import (
    TEXT_SCHEMA_CURRENT,
    TEXT_SCHEMA_PRE_1996,
    get_text_schema,
)

__all__ = [
    "DEVICE_SCHEMA_CURRENT",
    "DEVICE_SCHEMA_1998_2010",
    "DEVICE_SCHEMA_PRE_1998",
    "get_device_schema",
    "MASTER_SCHEMA_86_COLUMNS",
    "MASTER_SCHEMA_84_COLUMNS",
    "get_master_schema",
    "TEXT_SCHEMA_CURRENT",
    "TEXT_SCHEMA_PRE_1996",
    "get_text_schema",
]
