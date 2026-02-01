#!/usr/bin/env python3
"""
TypeScript Type Generator for MAUDE Analyzer Frontend.

This script generates TypeScript interfaces and constants from the Unified Schema Registry,
ensuring type safety and consistency between backend and frontend.

Usage:
    python scripts/generate_frontend_types.py [--output OUTPUT_DIR]

The generated file includes:
- TypeScript interfaces for all database tables
- Field name constants for API responses
- Event type mappings with full metadata
- Outcome code definitions
- Text type definitions
- Schema version constant
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.unified_schema import (
    get_schema_registry,
    SCHEMA_VERSION,
    DUCKDB_TO_TS_TYPE,
)


def generate_typescript_types(output_dir: str = None) -> str:
    """
    Generate TypeScript types from the Unified Schema Registry.

    Args:
        output_dir: Output directory for generated files

    Returns:
        Path to generated file
    """
    registry = get_schema_registry()

    # Get default output directory
    if output_dir is None:
        project_root = Path(__file__).parent.parent
        output_dir = project_root / "frontend" / "src" / "types" / "generated"

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / "schema.ts"

    # Generate TypeScript content
    ts_content = generate_typescript_content(registry)

    # Write to file
    with open(output_file, "w") as f:
        f.write(ts_content)

    print(f"Generated TypeScript types at: {output_file}")
    return str(output_file)


def generate_typescript_content(registry) -> str:
    """Generate the TypeScript file content."""

    lines = [
        "/**",
        " * AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY",
        " *",
        " * Generated from UnifiedSchemaRegistry by generate_frontend_types.py",
        f" * Generated at: {datetime.now().isoformat()}",
        f" * Schema Version: {SCHEMA_VERSION}",
        " *",
        " * This file provides type-safe access to schema definitions that match",
        " * the backend database schema exactly.",
        " */",
        "",
        "// =============================================================================",
        "// SCHEMA VERSION",
        "// =============================================================================",
        "",
        f"export const SCHEMA_VERSION = '{SCHEMA_VERSION}' as const",
        "",
    ]

    # Generate table interfaces
    lines.extend(generate_table_interfaces(registry))

    # Generate field constants
    lines.extend(generate_field_constants(registry))

    # Generate event type definitions
    lines.extend(generate_event_types(registry))

    # Generate outcome definitions
    lines.extend(generate_outcome_codes(registry))

    # Generate text type definitions
    lines.extend(generate_text_types(registry))

    # Generate helper functions
    lines.extend(generate_helper_functions())

    return "\n".join(lines)


def generate_table_interfaces(registry) -> list:
    """Generate TypeScript interfaces for each table."""
    lines = [
        "// =============================================================================",
        "// TABLE INTERFACES",
        "// =============================================================================",
        "",
    ]

    for table_name, table_def in registry.tables.items():
        # Convert table name to PascalCase for interface name
        interface_name = "".join(word.capitalize() for word in table_name.split("_"))

        lines.append(f"/**")
        lines.append(f" * {table_def.display_name}")
        lines.append(f" * {table_def.description}")
        lines.append(f" */")
        lines.append(f"export interface {interface_name} {{")

        for col in table_def.columns:
            ts_type = col.ts_type or DUCKDB_TO_TS_TYPE.get(col.data_type, "unknown")
            nullable_suffix = " | null" if col.nullable else ""
            optional_marker = "?" if col.is_optional else ""

            # Add description as comment if present
            if col.description:
                lines.append(f"  /** {col.description} */")

            lines.append(f"  {col.db_name}{optional_marker}: {ts_type}{nullable_suffix}")

        lines.append("}")
        lines.append("")

    return lines


def generate_field_constants(registry) -> list:
    """Generate field name constants for API responses."""
    lines = [
        "// =============================================================================",
        "// FIELD NAME CONSTANTS",
        "// =============================================================================",
        "",
        "/**",
        " * Field names as used in API responses, organized by table.",
        " * Use these instead of string literals for type safety.",
        " */",
        "export const FIELDS = {",
    ]

    # Group fields by table to avoid duplicate keys
    for table_name, table_def in registry.tables.items():
        # Convert table name to camelCase for the namespace
        namespace = "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(table_name.split("_"))
        )
        lines.append(f"  {namespace}: {{")
        for col in table_def.columns:
            const_name = col.db_name.upper()
            lines.append(f"    {const_name}: '{col.api_name}',")
        lines.append(f"  }},")

    lines.append("} as const")
    lines.append("")

    # Also generate a flat list of common fields (deduplicated)
    lines.append("/**")
    lines.append(" * Common field names (deduplicated across tables).")
    lines.append(" */")
    lines.append("export const COMMON_FIELDS = {")

    # Collect unique field names
    seen = set()
    for table_def in registry.tables.values():
        for col in table_def.columns:
            if col.db_name not in seen:
                seen.add(col.db_name)
                const_name = col.db_name.upper()
                lines.append(f"  {const_name}: '{col.api_name}',")

    lines.append("} as const")
    lines.append("")
    lines.append("export type FieldName = (typeof COMMON_FIELDS)[keyof typeof COMMON_FIELDS]")
    lines.append("")

    return lines


def generate_event_types(registry) -> list:
    """Generate event type definitions and mappings."""
    lines = [
        "// =============================================================================",
        "// EVENT TYPE DEFINITIONS",
        "// =============================================================================",
        "",
        "export interface EventTypeInfo {",
        "  dbCode: string",
        "  filterCode: string",
        "  name: string",
        "  description: string",
        "  severity: number",
        "  color: string",
        "  bgClass: string",
        "  textClass: string",
        "}",
        "",
        "/**",
        " * Event types with full metadata.",
        " * Keys are database codes (D, IN, M, O).",
        " */",
        "export const EVENT_TYPES: Record<string, EventTypeInfo> = {",
    ]

    for code, event_type in registry.get_all_event_types().items():
        lines.append(f"  '{code}': {{")
        lines.append(f"    dbCode: '{event_type.db_code}',")
        lines.append(f"    filterCode: '{event_type.filter_code}',")
        lines.append(f"    name: '{event_type.name}',")
        lines.append(f"    description: '{event_type.description}',")
        lines.append(f"    severity: {event_type.severity},")
        lines.append(f"    color: '{event_type.color}',")
        lines.append(f"    bgClass: '{event_type.bg_class}',")
        lines.append(f"    textClass: '{event_type.text_class}',")
        lines.append(f"  }},")

    lines.append("} as const")
    lines.append("")

    # Generate filter code mapping
    lines.append("/**")
    lines.append(" * Maps filter codes to database codes.")
    lines.append(" * Filter uses 'I' for injury, database uses 'IN'.")
    lines.append(" */")
    lines.append("export const FILTER_TO_DB_CODE: Record<string, string> = {")
    for event_type in registry.get_all_event_types().values():
        lines.append(f"  '{event_type.filter_code}': '{event_type.db_code}',")
    lines.append("} as const")
    lines.append("")

    # Generate reverse mapping
    lines.append("/**")
    lines.append(" * Maps database codes to filter codes.")
    lines.append(" */")
    lines.append("export const DB_TO_FILTER_CODE: Record<string, string> = {")
    for event_type in registry.get_all_event_types().values():
        lines.append(f"  '{event_type.db_code}': '{event_type.filter_code}',")
    lines.append("} as const")
    lines.append("")

    # Generate event type options for dropdowns
    lines.append("/**")
    lines.append(" * Event type options for filter dropdowns/buttons.")
    lines.append(" * Excludes 'Unknown' (*) type.")
    lines.append(" */")
    lines.append("export const EVENT_TYPE_OPTIONS = [")
    for code, event_type in registry.get_all_event_types().items():
        if code != "*":
            lines.append(f"  {{ value: '{event_type.filter_code}', label: '{event_type.name}', dbCode: '{event_type.db_code}' }},")
    lines.append("] as const")
    lines.append("")

    # Generate type for event type codes
    lines.append("export type EventTypeDbCode = keyof typeof EVENT_TYPES")
    lines.append("export type EventTypeFilterCode = (typeof EVENT_TYPE_OPTIONS)[number]['value']")
    lines.append("")

    return lines


def generate_outcome_codes(registry) -> list:
    """Generate patient outcome definitions."""
    lines = [
        "// =============================================================================",
        "// PATIENT OUTCOME DEFINITIONS",
        "// =============================================================================",
        "",
        "export interface OutcomeInfo {",
        "  code: string",
        "  name: string",
        "  dbField: string",
        "  severity: number",
        "  colorClass: string",
        "}",
        "",
        "export const OUTCOME_CODES: Record<string, OutcomeInfo> = {",
    ]

    for code, outcome in registry.get_all_outcomes().items():
        lines.append(f"  '{code}': {{")
        lines.append(f"    code: '{outcome.code}',")
        lines.append(f"    name: '{outcome.name}',")
        lines.append(f"    dbField: '{outcome.db_field}',")
        lines.append(f"    severity: {outcome.severity},")
        lines.append(f"    colorClass: '{outcome.color_class}',")
        lines.append(f"  }},")

    lines.append("} as const")
    lines.append("")

    lines.append("export type OutcomeCode = keyof typeof OUTCOME_CODES")
    lines.append("")

    return lines


def escape_ts_string(s: str) -> str:
    """Escape a string for use in TypeScript single-quoted strings."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def generate_text_types(registry) -> list:
    """Generate text type definitions."""
    lines = [
        "// =============================================================================",
        "// TEXT TYPE DEFINITIONS",
        "// =============================================================================",
        "",
        "export interface TextTypeInfo {",
        "  code: string",
        "  name: string",
        "  description: string",
        "  priority: number",
        "}",
        "",
        "export const TEXT_TYPE_CODES: Record<string, TextTypeInfo> = {",
    ]

    for code, text_type in registry.get_all_text_types().items():
        lines.append(f"  '{code}': {{")
        lines.append(f"    code: '{escape_ts_string(text_type.code)}',")
        lines.append(f"    name: '{escape_ts_string(text_type.name)}',")
        lines.append(f"    description: '{escape_ts_string(text_type.description)}',")
        lines.append(f"    priority: {text_type.priority},")
        lines.append(f"  }},")

    lines.append("} as const")
    lines.append("")

    lines.append("export type TextTypeCode = keyof typeof TEXT_TYPE_CODES")
    lines.append("")

    return lines


def generate_helper_functions() -> list:
    """Generate helper functions for type conversions."""
    lines = [
        "// =============================================================================",
        "// HELPER FUNCTIONS",
        "// =============================================================================",
        "",
        "/**",
        " * Convert filter code to database code.",
        " * Handles I -> IN conversion for injury.",
        " */",
        "export function filterToDbCode(filterCode: string): string {",
        "  return FILTER_TO_DB_CODE[filterCode] || filterCode",
        "}",
        "",
        "/**",
        " * Convert database code to filter code.",
        " * Handles IN -> I conversion for injury.",
        " */",
        "export function dbToFilterCode(dbCode: string): string {",
        "  return DB_TO_FILTER_CODE[dbCode] || dbCode",
        "}",
        "",
        "/**",
        " * Get event type display info by database code.",
        " */",
        "export function getEventTypeDisplay(dbCode: string): { label: string; color: string } {",
        "  const eventType = EVENT_TYPES[dbCode]",
        "  if (eventType) {",
        "    return {",
        "      label: eventType.name,",
        "      color: `${eventType.bgClass} ${eventType.textClass}`,",
        "    }",
        "  }",
        "  return { label: dbCode || 'Unknown', color: 'bg-gray-100 text-gray-800' }",
        "}",
        "",
        "/**",
        " * Get text type display name.",
        " */",
        "export function getTextTypeName(code: string): string {",
        "  return TEXT_TYPE_CODES[code]?.name || code",
        "}",
        "",
        "/**",
        " * Get outcome display name.",
        " */",
        "export function getOutcomeName(code: string): string {",
        "  return OUTCOME_CODES[code]?.name || code",
        "}",
        "",
        "/**",
        " * Convert list of filter codes to database codes.",
        " */",
        "export function convertFilterEventTypes(filterCodes: string[]): string[] {",
        "  return filterCodes.map(filterToDbCode)",
        "}",
        "",
    ]

    return lines


def main():
    parser = argparse.ArgumentParser(
        description="Generate TypeScript types from Unified Schema Registry"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output directory for generated files",
        default=None
    )
    parser.add_argument(
        "--json", "-j",
        help="Also output schema as JSON file",
        action="store_true"
    )

    args = parser.parse_args()

    # Generate TypeScript types
    output_file = generate_typescript_types(args.output)

    # Optionally generate JSON schema
    if args.json:
        registry = get_schema_registry()
        json_file = Path(output_file).parent / "schema.json"
        with open(json_file, "w") as f:
            f.write(registry.export_to_json())
        print(f"Generated JSON schema at: {json_file}")

    print(f"Schema version: {SCHEMA_VERSION}")


if __name__ == "__main__":
    main()
