"""
Record-level transformation functions for entity mapping.

Pure functions that transform individual records (dicts) based on source data.
These functions are used by EntityMapper but can be used standalone for custom
preprocessing or testing.

Each function modifies the record dictionary in-place, applying specific
transformations like value mapping, date formatting, ID generation, etc.

Author: PCGL Data Mapping Team
Date: 2026-01-07
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional, List, Callable, Union
from .utils import (
    format_date_to_pcgl,
    calculate_duration_in_days,
    generate_record_id,
    calculate_age_in_days,
    _map_field_value
)

logger = logging.getLogger(__name__)

__all__ = [
    'apply_value_to_record',
    'apply_age_to_record',
    'apply_identifier_to_record',
    'apply_note_to_record',
    'apply_date_to_record',
    'apply_duration_to_record',
    'apply_integer_to_record',
]


# =============================================================================
# VALUE MAPPING TRANSFORMS
# =============================================================================

def apply_value_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    source_field: Optional[str],
    value_mappings: Optional[Dict[Any, Any]] = None,
    default_value: Any = None,
    has_default: bool = False
) -> None:
    """
    Apply direct or value-mapped field to a single record.
    
    Handles various mapping scenarios:
    - Direct field copy (source → target)
    - Value mappings (transform values using dict)
    - Default values (when source is missing)
    - Multi-field targets (list of target fields)
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name or list of field names
        source_row: Source data row
        source_field: Source field name (can be None for default_value only)
        value_mappings: Optional mapping dictionary
        default_value: Default value if source is None/missing
        has_default: Whether default_value was explicitly provided (even if None)
    """
    # Handle fields with no source (use default value)
    if source_field is None or source_field == 'null':
        if has_default:  # Check if default_value key exists, not just if value is not None
            if isinstance(default_value, list):
                if isinstance(target_field, list):
                    # Multi-field with default list values
                    for i, tf in enumerate(target_field):
                        if i < len(default_value):
                            record[tf] = default_value[i]
                else:
                    # Single field with list default → join
                    record[target_field] = "|".join(str(v) for v in default_value) if default_value else None
            else:
                # Single default value (can be None to explicitly null a field)
                if isinstance(target_field, list):
                    # Apply to first field
                    if target_field:
                        record[target_field[0]] = default_value
                else:
                    record[target_field] = default_value
        return
    
    # Get source value
    value = source_row.get(source_field)
    
    # Apply value mappings if present
    if value_mappings and pd.notna(value):
        # Use the imported _map_field_value utility which handles:
        # - List target fields
        # - Value mappings
        # - Note field appending
        _map_field_value(record, target_field, value, value_mappings)
    else:
        # Direct mapping without transformation
        if pd.notna(value):
            if isinstance(target_field, list):
                # Apply to first field
                if target_field:
                    record[target_field[0]] = value
            else:
                record[target_field] = value
        elif default_value is not None:
            # Use default value
            if isinstance(target_field, list):
                if target_field:
                    record[target_field[0]] = default_value
            else:
                record[target_field] = default_value


# =============================================================================
# AGE CALCULATION TRANSFORMS
# =============================================================================

def apply_age_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    age_params: Dict[str, Any],
    participant_id_field: Optional[str] = None
) -> None:
    """
    Apply age calculation to a single record.
    
    Calculates age in days based on birth date and event date.
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name
        source_row: Source data row
        age_params: Age calculation parameters (birth_date_field, event_date_field, etc.)
                   - event_date_field can be a single field name (str) or list of field names to try sequentially
        participant_id_field: Optional field name for participant ID (for context in warnings)
    """

    # Get parameters
    birth_date_field = age_params.get('birth_date_field')
    event_date_field = age_params.get('event_date_field')
    event_offset_field = age_params.get('event_offset_field')
    age_fallback_field = age_params.get('age_fallback_field')
    
    birth_date = source_row.get(birth_date_field) if birth_date_field else None
    
    # Handle event_date_field as single field or list of fields
    event_date = None
    if event_date_field:
        if isinstance(event_date_field, list):
            # List of fields - collect values from all fields
            event_date = [source_row.get(field) for field in event_date_field]
        else:
            # Single field
            event_date = source_row.get(event_date_field)
    
    event_offset = source_row.get(event_offset_field) if event_offset_field else None
    age_years = source_row.get(age_fallback_field) if age_fallback_field else None
    
    # Build context for better error messages (include participant ID and field info)
    context = {'target_field': target_field}
    if participant_id_field and participant_id_field in source_row.index:
        pid = source_row.get(participant_id_field)
        if pd.notna(pid):
            context['participant_id'] = pid
    
    # Add source field information for debugging (with values)
    source_fields = []
    if birth_date_field:
        value_repr = f"'{birth_date}'" if pd.notna(birth_date) else "null"
        source_fields.append(f"birth_date={birth_date_field}({value_repr})")
    if event_date_field:
        if isinstance(event_date_field, list):
            # List of event date fields
            event_date_parts = []
            for i, field in enumerate(event_date_field):
                value = event_date[i] if isinstance(event_date, list) and i < len(event_date) else None
                value_repr = f"'{value}'" if pd.notna(value) else "null"
                event_date_parts.append(f"{field}({value_repr})")
            source_fields.append(f"event_date=[{', '.join(event_date_parts)}]")
        else:
            # Single event date field
            value_repr = f"'{event_date}'" if pd.notna(event_date) else "null"
            source_fields.append(f"event_date={event_date_field}({value_repr})")
    if event_offset_field:
        value_repr = event_offset if pd.notna(event_offset) else "null"
        source_fields.append(f"event_offset={event_offset_field}({value_repr})")
    if age_fallback_field:
        value_repr = age_years if pd.notna(age_years) else "null"
        source_fields.append(f"age_fallback={age_fallback_field}({value_repr})")
    context['source_fields'] = ', '.join(source_fields) if source_fields else 'none'
    
    # Add event date field names to context for better logging
    if event_date_field:
        if isinstance(event_date_field, list):
            context['event_date_field_names'] = event_date_field
        else:
            context['event_date_field_names'] = [event_date_field]
    
    # Calculate and set age
    age_days = calculate_age_in_days(
        birth_date=birth_date,
        event_date=event_date,
        age_years=age_years,
        event_offset_days=event_offset,
        context=context
    )
    
    # Always set the field (can be None to explicitly null out a previously calculated value)
    if age_days is not None:
        record[target_field] = age_days


# =============================================================================
# IDENTIFIER GENERATION TRANSFORMS
# =============================================================================

def apply_identifier_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    identifier_params: Dict[str, Any],
    source_field: Optional[str] = None
) -> None:
    """
    Apply identifier generation to a single record.
    
    Generates structured IDs in format: {prefix}_{type}_{suffix}
    
    YAML params:
    - prefix_field: Field name containing participant ID
    - type: Record type (e.g., 'treatment', 'measurement')
    - suffix_fields: List of field names or literal values to build suffix
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name
        source_row: Source data row
        identifier_params: ID generation parameters (prefix_field, type, suffix_fields)
        source_field: Source field name (used when suffix element is "source_field_name")
    """
    # Get parameters (support both old and new parameter names)
    prefix_field = identifier_params.get('prefix_field') or identifier_params.get('record_prefix_field')
    record_type = identifier_params.get('type') or identifier_params.get('record_type')
    suffix_fields = identifier_params.get('suffix_fields', [])
    
    # Handle None value for suffix_fields
    if suffix_fields is None:
        suffix_fields = []
    
    # Build prefix
    record_prefix = source_row.get(prefix_field)
    
    # Build suffix from suffix_fields list
    suffix_parts = []
    for field_or_value in suffix_fields:
        if field_or_value == "source_field_name":
            # Special case: use the actual source field name
            if source_field:
                suffix_parts.append(str(source_field))
        elif isinstance(field_or_value, str) and field_or_value.startswith('literal:'):
            # Force literal string usage with 'literal:' prefix
            # e.g., 'literal:SUBJECT_ID' uses the string 'SUBJECT_ID' not the field value
            literal_value = field_or_value[8:]  # Remove 'literal:' prefix
            suffix_parts.append(literal_value)
        elif field_or_value in source_row.index:
            # Field exists in source row - use its value
            value = source_row.get(field_or_value)
            if pd.notna(value):
                suffix_parts.append(str(value))
        else:
            # Literal value (field doesn't exist in source) - use as-is
            suffix_parts.append(str(field_or_value))
    
    # Join suffix parts with underscore
    record_suffix = "_".join(suffix_parts) if suffix_parts else None
    
    # Generate ID
    record[target_field] = generate_record_id(
        record_prefix=record_prefix,
        record_type=record_type,
        record_suffix=record_suffix
    )


# =============================================================================
# NOTE AGGREGATION TRANSFORMS
# =============================================================================

def apply_note_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    note_fields: List[Any]
) -> None:
    """
    Apply note aggregation to a single record.
    
    Combines multiple fields into a single note field with format:
    "field1: value1; field2: value2"
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name
        source_row: Source data row
        note_fields: List of source field names to aggregate
    """
    if not isinstance(note_fields, list):
        note_fields = [note_fields]
    
    parts = []
    for field in note_fields:
        value = source_row.get(field)
        if pd.notna(value) and str(value) not in ['', '-1', 'nan']:
            parts.append(f"{field}: {value}")
    
    if parts:
        note_text = '; '.join(parts)
        # Check if target field already has content
        if record.get(target_field):
            record[target_field] += ' | ' + note_text
        else:
            record[target_field] = note_text


# =============================================================================
# DATE AND TIME TRANSFORMS
# =============================================================================

def apply_date_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    source_field: Optional[Union[str, List[str]]],
    participant_id_field: Optional[str] = None
) -> None:
    """
    Apply date formatting to a single record.
    
    Converts date values to PCGL standard format (YYYY-MM-DD).
    If source_field is a list, tries each field sequentially and uses the first non-null, parseable date.
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name
        source_row: Source data row
        source_field: Source field name or list of field names to try sequentially
        participant_id_field: Optional field name for participant ID (for context in warnings)
    """
    if not source_field:
        return
    
    # Build context for better error messages
    context = {'target_field': target_field}
    if participant_id_field and participant_id_field in source_row.index:
        pid = source_row.get(participant_id_field)
        if pd.notna(pid):
            context['participant_id'] = pid
    
    # Handle source_field as list - try each field sequentially
    if isinstance(source_field, list):
        for field_name in source_field:
            date_value = source_row.get(field_name)
            if pd.notna(date_value):
                formatted_date = format_date_to_pcgl(date_value, context=context)
                if formatted_date is not None:
                    record[target_field] = formatted_date
                    # Log which field was used
                    from .utils import _format_context, logger
                    ctx = _format_context(context)
                    logger.debug(f"{ctx}Using date from field: {field_name}='{formatted_date}'")
                    return
        # No valid date found in any field
        return
    else:
        # Single field
        date_value = source_row.get(source_field)
        if pd.notna(date_value):
            record[target_field] = format_date_to_pcgl(date_value, context=context)


def apply_duration_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    start_field: Optional[str],
    end_field: Optional[str]
) -> None:
    """
    Apply duration calculation to a single record.
    
    Calculates duration in days between start and end dates.
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name
        source_row: Source data row
        start_field: Start date field name
        end_field: End date field name
    """
    if start_field and end_field:
        start_date = source_row.get(start_field)
        end_date = source_row.get(end_field)
        duration = calculate_duration_in_days(start_date, end_date)
        if duration is not None:
            record[target_field] = duration


# =============================================================================
# TYPE CONVERSION TRANSFORMS
# =============================================================================

def apply_integer_to_record(
    record: Dict[str, Any],
    target_field: str,
    source_row: pd.Series,
    source_field: Optional[str],
    value_mappings: Optional[Dict[Any, Any]] = None,
    default_value: Any = None,
    logger_instance: Optional[logging.Logger] = None,
    participant_id_field: Optional[str] = None
) -> None:
    """
    Apply integer conversion to a single record.
    
    Converts field values to integers with optional value mapping first.
    Handles various input formats (float, string, etc.).
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name
        source_row: Source data row
        source_field: Source field name
        value_mappings: Optional mapping dictionary (applied before integer conversion)
        default_value: Default value if source is None/missing
        logger_instance: Optional logger for warnings
        participant_id_field: Optional field name for participant ID (for context in warnings)
    """
    log = logger_instance or logger
    
    # Build context for better error messages
    context = None
    if participant_id_field and participant_id_field in source_row.index:
        pid = source_row.get(participant_id_field)
        if pd.notna(pid):
            context = {'participant_id': pid}
    
    # Import format context helper
    from .utils import _format_context
    ctx = _format_context(context)
    
    # Handle fields with no source (use default value)
    if source_field is None or source_field == 'null':
        if default_value is not None:
            try:
                record[target_field] = int(float(default_value))
            except (ValueError, TypeError) as e:
                log.warning(f"{ctx}Cannot convert default value '{default_value}' to integer for {target_field}: {e}")
                record[target_field] = None
        return
    
    # Get value from source
    value = source_row.get(source_field)
    
    # Apply value mappings first (if present)
    if value_mappings and pd.notna(value):
        # Convert value to type that can be used as dict key
        try:
            key = int(float(value))
        except (ValueError, TypeError):
            key = value
        
        # Apply mapping
        if key in value_mappings:
            value = value_mappings[key]
    
    # Convert to integer (if not None/null from mapping)
    if pd.notna(value) and value is not None:
        try:
            # Convert to int (handles float, string, etc.)
            record[target_field] = int(float(value))
        except (ValueError, TypeError) as e:
            log.warning(f"{ctx}Cannot convert '{value}' to integer for {target_field}: {e}")
            record[target_field] = None
    elif default_value is not None:
        # Use default value if source is None/NaN
        try:
            record[target_field] = int(float(default_value))
        except (ValueError, TypeError) as e:
            log.warning(f"{ctx}Cannot convert default value '{default_value}' to integer for {target_field}: {e}")
            record[target_field] = None
