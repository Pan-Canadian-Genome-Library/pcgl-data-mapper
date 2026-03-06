"""
Mapper Utilities - Consolidated utility functions for entity mappers.

This module consolidates frequently-used utility functions that are needed by
entity mappers, reducing the number of import paths and simplifying dependencies.

Functions consolidated here:
- Field mapping and value transformation
- Age calculation
- ID generation
- Validation
- Logging and DataFrame operations
- Conditional field population
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional, Union, Callable, List
from datetime import datetime, date
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)


def _format_context(context: Optional[Dict[str, Any]] = None) -> str:
    """
    Format context information for log messages.
    
    Args:
        context: Optional context dictionary with keys like 'participant_id', 'record_id', 'target_field', etc.
        
    Returns:
        Formatted context string (e.g., "[participant_id=P001, target_field=age_at_diagnosis]") or empty string
    """
    if not context:
        return ""
    
    parts = []
    if 'participant_id' in context and context['participant_id']:
        parts.append(f"participant_id={context['participant_id']}")
    if 'record_id' in context and context['record_id']:
        parts.append(f"record_id={context['record_id']}")
    if 'target_field' in context and context['target_field']:
        parts.append(f"target_field={context['target_field']}")
    
    return f"[{', '.join(parts)}] " if parts else ""


__all__ = [
    'log_mapping_summary',
    'convert_nullable_int_columns',
    'validate_participant_id',
    'validate_age_in_days',
    'calculate_age_in_days',
    'parse_age_with_units',
    '_map_field_value',
    '_set_or_append_field',
    'generate_record_id',
    'parse_date',
    'format_date_to_pcgl',
    'read_data_file',
    'calculate_duration_in_days',
    'clean_numeric_string',
    'safe_int_conversion',
]

# ============================================================================
# DATA READING FUNCTIONS
# ============================================================================

def read_data_file(file_path: Path) -> pd.DataFrame:
    """
    Read data file with automatic format detection.
    
    Supports CSV, TSV, and TXT files with automatic delimiter detection.
    
    Args:
        file_path: Path to data file
        
    Returns:
        DataFrame with loaded data
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()
    
    # Determine delimiter based on file extension
    if suffix == '.tsv':
        delimiter = '\t'
    elif suffix == '.txt':
        # Try to auto-detect delimiter for .txt files
        delimiter = None  # pandas will auto-detect
    else:  # .csv or others
        delimiter = ','
    
    # Read with appropriate delimiter and encoding fallbacks
    encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1']
    last_error = None

    for encoding in encodings_to_try:
        try:
            if delimiter:
                df = pd.read_csv(file_path, sep=delimiter, encoding=encoding)
            else:
                # Let pandas auto-detect (works for most cases)
                df = pd.read_csv(file_path, sep=None, engine='python', encoding=encoding)
            return df
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    # Final fallback: replace undecodable characters
    if delimiter:
        df = pd.read_csv(file_path, sep=delimiter, encoding='utf-8', errors='replace')
    else:
        df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8', errors='replace')
    
    return df


# ============================================================================
# FIELD MAPPING FUNCTIONS
# ============================================================================

def _map_field_value(
    record: Dict[str, Any],
    target_field: Any,
    source_value: Any,
    value_mappings: Dict[Any, Any],
    append_mode: bool = False
) -> None:
    """
    Map source value to target field(s) using value_mappings.
    
    Handles:
    - Single field: Sets record[target_field] = mapped_value
    - List of fields: Maps list values to corresponding fields
    - Note fields: Appends to existing content instead of replacing
    - Checkbox aggregation: Appends when append_mode=True
    
    Args:
        record: Record dictionary to update (modified in place)
        target_field: Target field name or list of field names
        source_value: Value to map (will be converted to int if possible)
        value_mappings: Mapping dictionary {source_value: mapped_value}
        append_mode: If True, always append values (for checkbox aggregation)
    """
    if pd.isna(source_value):
        return
    
    # Convert source_value to lookup key
    try:
        if isinstance(source_value, (int, float)):
            lookup_key = int(float(source_value))
        else:
            lookup_key = source_value
    except (ValueError, TypeError):
        lookup_key = source_value
    
    # Get mapped value
    mapped_value = value_mappings.get(lookup_key)
    if mapped_value is None:
        return
    
    # Apply mapped value to target field(s)
    if isinstance(target_field, list):
        # Multi-field mapping
        if isinstance(mapped_value, list):
            # List to list: [code, term] <- ['HP:123', 'Disease']
            for i, field in enumerate(target_field):
                if i < len(mapped_value):
                    _set_or_append_field(record, field, mapped_value[i], append_mode)
        else:
            # Single value to first field
            if len(target_field) > 0:
                _set_or_append_field(record, target_field[0], mapped_value, append_mode)
    else:
        # Single field mapping
        _set_or_append_field(record, target_field, mapped_value, append_mode)


def _set_or_append_field(
    record: Dict[str, Any],
    field_name: str,
    value: Any,
    append_mode: bool = False
) -> None:
    """
    Set field value or append if field name contains 'note' or append_mode is True.
    
    Only appends if the value is different from existing values (prevents duplicates).
    
    Args:
        record: Record dictionary to update
        field_name: Field name
        value: Value to set or append
        append_mode: If True, always append (used for checkbox aggregation)
    """
    if not value:
        return
    
    # Check if this is a note field or append mode is enabled
    is_note_field = 'note' in field_name.lower()
    should_append = append_mode or is_note_field
    
    if should_append and record.get(field_name):
        # Only append if value is different from existing values
        existing = str(record[field_name])
        new_value = str(value)
        
        # Check if value already exists in pipe-separated list
        existing_values = existing.split("|")
        if new_value not in existing_values:
            record[field_name] += "|" + new_value
        # else: value already exists, skip appending to avoid duplicates
    else:
        # Set or replace value
        record[field_name] = value


# ============================================================================
# AGE CALCULATION FUNCTIONS
# ============================================================================

def parse_age_with_units(age_value: Optional[Union[str, int, float]], context: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """
    Parse age from various formats and convert to days.
    
    Supports:
    - Numeric value (default unit: years) - e.g., 25 → 25 years → 9131 days
    - Single value with unit suffix:
      - "24 months" → 730 days
      - "200 days" → 200 days
      - "17 weeks" → 119 days
      - "2.5 years" → 913 days
    - Compound formats (multiple components):
      - "1 year 7 months" → 578 days
      - "2 years 3 months" → 821 days
      - "5 months 2 weeks" → 166 days
    
    Unit conversion factors:
    - years: × 365.25 (accounting for leap years)
    - months: × 30.44 (average days per month)
    - weeks: × 7
    - days: × 1
    
    Args:
        age_value: Age as number (years) or string with unit suffix(es)
        context: Optional context dict with 'participant_id' or 'record_id' for better error messages
        
    Returns:
        Age in days (int), or None if cannot parse
        
    Examples:
        >>> parse_age_with_units(25)
        9131
        >>> parse_age_with_units("24 months")
        730
        >>> parse_age_with_units("200 days")
        200
        >>> parse_age_with_units("17 weeks")
        119
        >>> parse_age_with_units("1 year 7 months")
        578
    """
    if pd.isna(age_value):
        return None
    
    # Handle numeric values (default unit: years)
    if isinstance(age_value, (int, float)):
        try:
            age_years = float(age_value)
            if 0 <= age_years <= 120:  # Reasonable age range
                return int(age_years * 365.25)
            else:
                ctx = _format_context(context)
                source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
                logger.warning(f"{ctx}Age {age_years} years seems unreasonable{source_info}")
                return None
        except (ValueError, TypeError):
            return None
    
    # Handle string values
    if isinstance(age_value, str):
        age_str = str(age_value).strip().lower()
        
        # Try to parse as plain number first
        try:
            age_years = float(age_str)
            if 0 <= age_years <= 120:
                return int(age_years * 365.25)
        except ValueError:
            pass
        
        # Parse value with unit suffix
        import re
        
        # Try compound format first (e.g., "1 year 7 months", "2 years 3 months 5 days")
        # Find all number-unit pairs in the string
        compound_pattern = r'(\d+\.?\d*)\s*(years?|months?|weeks?|days?)'
        matches = re.findall(compound_pattern, age_str)
        
        if matches:
            total_days = 0
            try:
                for value_str, unit in matches:
                    value = float(value_str)
                    
                    # Convert to days based on unit
                    if unit.startswith('year'):
                        total_days += int(value * 365.25)
                    elif unit.startswith('month'):
                        total_days += int(value * 30.44)  # Average days per month
                    elif unit.startswith('week'):
                        total_days += int(value * 7)
                    elif unit.startswith('day'):
                        total_days += int(value)
                    else:
                        ctx = _format_context(context)
                        source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
                        logger.warning(f"{ctx}Unknown age unit: {unit}{source_info}")
                        return None
                
                # Validate reasonable range
                if 0 <= total_days <= 120 * 365.25:
                    return total_days
                else:
                    ctx = _format_context(context)
                    source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
                    logger.warning(f"{ctx}Calculated age {total_days} days ({total_days/365.25:.1f} years) seems unreasonable{source_info}")
                    return None
                    
            except ValueError:
                ctx = _format_context(context)
                source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
                logger.warning(f"{ctx}Could not parse age value: {age_value}{source_info}")
                return None
        else:
            ctx = _format_context(context)
            source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
            logger.warning(f"{ctx}Could not parse age format: {age_value}{source_info}")
            return None
    
    return None


def calculate_age_in_days(
    birth_date: Optional[Union[str, datetime, date]],
    event_date: Optional[Union[str, datetime, date, List[Union[str, datetime, date]]]],
    age_years: Optional[Union[float, str]] = None,
    assume_mid_month: bool = True,
    event_offset_days: Optional[int] = None,
    context: Optional[Dict[str, Any]] = None
) -> Optional[int]:
    """
    Calculate age in days from birth date to event date.
    
    This is the primary age calculation function used across all mappers.
    Uses birth_date if available, otherwise falls back to age with units.
    Can apply an offset (positive or negative) from the event date.
    
    Args:
        birth_date: Date of birth (string in 'YYYY/MM/DD' or 'YYYY-MM-DD' format,
                    datetime, or date object)
        event_date: Date of event (single date or list of dates to try sequentially).
                    If a list is provided, uses the first non-null date that can be parsed.
                    Format: string in 'YYYY/MM/DD' or 'YYYY-MM-DD' format, datetime, or date object
        age_years: Age fallback value (supports multiple formats):
                  - Numeric value (interpreted as years): 25 → 9131 days
                  - String with units: "24 months", "200 days", "17 weeks"
                  See parse_age_with_units() for full format details
        assume_mid_month: If True and birth_date is partial (year/month only), 
                         assumes day 15 (default: True)
        event_offset_days: Days offset from event_date (positive for future, negative for past).
                          If provided, calculates age at (event_date + event_offset_days).
        context: Optional context dict with 'participant_id' or 'record_id' for better error messages
        
    Returns:
        Age in days (int), or None if calculation not possible
    """
    # Handle event_date as list - try each date sequentially
    event_date_to_use = None
    event_date_field_used = None
    if isinstance(event_date, list):
        field_names = context.get('event_date_field_names', []) if context else []
        for i, candidate_date in enumerate(event_date):
            if pd.notna(candidate_date):
                # Try to parse this date
                parsed = parse_date(candidate_date, assume_mid_month, context)
                if parsed is not None:
                    event_date_to_use = candidate_date
                    event_date_field_used = field_names[i] if i < len(field_names) else f"field_{i}"
                    ctx = _format_context(context)
                    logger.debug(f"{ctx}Using event date from list: {event_date_field_used}='{candidate_date}'")
                    break
        # If no valid date found in list, event_date_to_use remains None
    else:
        event_date_to_use = event_date
        if context and 'event_date_field_names' in context and context['event_date_field_names']:
            event_date_field_used = context['event_date_field_names'][0]
    
    # Primary method: calculate from birth date
    # Log reason for fallback if birth_date is missing but event_date is available
    if pd.isna(birth_date) and pd.notna(event_date_to_use):
        ctx = _format_context(context)
        source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
        logger.debug(f"{ctx}Cannot calculate age from dates: birth_date is null, falling back to age field{source_info}")
    
    if pd.notna(birth_date) and pd.notna(event_date_to_use):
        try:
            # Parse birth date - handle multiple formats
            birth_dt = parse_date(birth_date, assume_mid_month, context)
            if birth_dt is None:
                ctx = _format_context(context)
                logger.warning(f"{ctx}Could not parse birth_date: {birth_date}")
                raise ValueError(f"Could not parse birth_date: {birth_date}")
            
            # Parse event date - handle multiple date formats
            event_dt = parse_date(event_date_to_use, assume_mid_month, context)
            if event_dt is None:
                ctx = _format_context(context)
                logger.warning(f"{ctx}Could not parse event_date: {event_date_to_use}")
                raise ValueError(f"Could not parse event_date: {event_date_to_use}")
            
            # Calculate days difference
            age_days = (event_dt - birth_dt).days
            
            # Apply offset if provided
            if pd.notna(event_offset_days):
                try:
                    offset = int(float(event_offset_days))
                    age_days += offset
                    ctx = _format_context(context)
                    logger.debug(f"{ctx}Applied offset of {offset} days to age calculation")
                except (ValueError, TypeError) as e:
                    ctx = _format_context(context)
                    logger.warning(f"{ctx}Invalid event_offset_days value: {event_offset_days}, error: {e}")
            
            # Validate result
            if age_days < 0:
                ctx = _format_context(context)
                source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
                logger.warning(f"{ctx}Negative age calculated: {age_days} days (event before birth){source_info}")
                return None
            if age_days > 120 * 365.25:
                ctx = _format_context(context)
                logger.warning(f"{ctx}Age {age_days} days ({age_days/365.25:.1f} years) seems unreasonable")
            
            return age_days
                
        except (ValueError, TypeError) as e:
            ctx = _format_context(context)
            source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
            logger.debug(f"{ctx}Error calculating age from birth date: {e}{source_info}")
            # Fall through to alternative method
    
    # Fallback method: use age with units parsing
    if pd.notna(age_years):
        try:
          # Try parsing with units first (handles "24 months", "200 days", etc.)
          age_days = parse_age_with_units(age_years, context)
          if age_days is not None:
              ctx = _format_context(context)
              source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
              logger.debug(f"{ctx}Used age fallback: {age_years} = {age_days} days{source_info}")
              return age_days

        except (ValueError, TypeError) as e:
            ctx = _format_context(context)
            source_info = f" (from {context.get('source_fields', 'unknown fields')})" if context and 'source_fields' in context else ""
            logger.debug(f"{ctx}Error using age fallback: {e}{source_info}")
    
    # Both methods failed
    return None


def parse_date(
    date_value: Optional[Union[str, datetime, date]],
    assume_mid_month: bool = False,
    context: Optional[Dict[str, Any]] = None
) -> Optional[date]:
    """
    Parse date from various formats.
    
    Handles:
    - String in 'YYYY/MM/DD', 'YYYY-MM-DD', 'YYYYMMDD' formats
    - String in 'YYYY-MM', 'YYYY/MM' (partial date)
    - datetime object
    - date object
    
    Args:
        date_value: Date in various formats
        assume_mid_month: If True and date is partial (year/month only),
                         assumes day 15 (default: True)
        
    Returns:
        Parsed date object, or None if cannot parse
    """
    if pd.isna(date_value):
        return None
    
    # Handle datetime object
    if isinstance(date_value, datetime):
        return date_value.date()
    
    # Handle date object
    if isinstance(date_value, date):
        return date_value
    
    # Handle string
    date_str = str(date_value).strip()
    
    # Try various formats
    date_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%Y%m%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # Try partial date (YYYY-MM or YYYY/MM)
    if assume_mid_month:
        partial_formats = ['%Y-%m', '%Y/%m']
        for fmt in partial_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return date(dt.year, dt.month, 15)  # Assume mid-month
            except ValueError:
                continue
    
    ctx = _format_context(context)
    logger.warning(f"{ctx}Could not parse date: {date_value}")
    return None

# ============================================================================
# ID GENERATION FUNCTIONS
# ============================================================================

def generate_record_id(
    record_prefix: str,
    record_type: str,
    record_suffix: str = None
) -> Optional[str]:
    """
    Generate unique record ID in standardized format.
    
    Format: 
    - With prefix: {record_prefix}_{record_type}_{record_suffix} or {record_prefix}_{record_type}
    - Without prefix: {record_type}_{record_suffix} or {record_type}
    
    Args:
        record_prefix: Optional prefix for the record ID (typically participant ID). Can be None.
        record_type: Type of record (e.g., 'comorbidity', 'phenotype', 'treatment', 'member')
        record_suffix: Optional suffix built from field values or literal strings
        
    Returns:
        Unique record ID (e.g., "P001_treatment_cort", "P001_comorbidity", "member_0", "member"), 
        or None if record_type is missing
    """
    # Return None if no record type specified
    if not record_type or pd.isna(record_type):
        return None
    
    # Build ID based on available components
    has_prefix = record_prefix is not None and not pd.isna(record_prefix)
    has_suffix = record_suffix is not None and record_suffix != ""
    
    if has_prefix and has_suffix:
        return f"{record_prefix}_{record_type}_{record_suffix}"
    elif has_prefix:
        return f"{record_prefix}_{record_type}"
    elif has_suffix:
        return f"{record_type}_{record_suffix}"
    else:
        return f"{record_type}"


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_participant_id(participant_id: Any) -> bool:
    """
    Validate that participant ID is present and valid.
    
    Args:
        participant_id: Participant ID to validate
        
    Returns:
        True if valid, False otherwise
    """
    if pd.isna(participant_id):
        return False
    
    id_str = str(participant_id).strip()
    return len(id_str) > 0


def validate_age_in_days(
    age_days: Optional[int],
    min_age: int = 0,
    max_age: int = 120 * 365,
    allow_none: bool = True
) -> bool:
    """
    Validate age in days is within reasonable range.
    
    Args:
        age_days: Age in days to validate
        min_age: Minimum acceptable age in days (default: 0)
        max_age: Maximum acceptable age in days (default: 120 years = 43,800 days)
        allow_none: Whether None is acceptable (default: True)
        
    Returns:
        True if valid, False otherwise
    """
    # Check for None, NaN, or pd.NA using pandas isna
    if pd.isna(age_days):
        return allow_none
    
    try:
        age = int(age_days)
        return min_age <= age <= max_age
    except (ValueError, TypeError):
        return False


# ============================================================================
# DATAFRAME OPERATIONS
# ============================================================================

def convert_nullable_int_columns(
    df: pd.DataFrame,
    int_columns: Optional[List[str]] = None,
    auto_detect: bool = True
) -> pd.DataFrame:
    """
    Convert columns with mixed int/None values to Int64 dtype for proper nullable integer handling.
    
    This function systematically handles the pandas behavior where columns with mixed 
    int and None values become float64 with NaN. It converts them to Int64 (nullable 
    integer type) to preserve integer display while allowing None/NaN values.
    
    Args:
        df: DataFrame to modify
        int_columns: List of column names to convert (if None, uses auto_detect)
        auto_detect: If True, auto-detect columns that look like integers with nulls (default: True)
        
    Returns:
        DataFrame with converted columns
    """
    if df.empty:
        return df
    
    columns_to_convert = []
    
    # Use explicitly specified columns if provided
    if int_columns is not None:
        columns_to_convert = [col for col in int_columns if col in df.columns]
    
    # Auto-detect columns if enabled and no explicit columns specified
    elif auto_detect:
        # Common patterns for integer columns in PCGL schema
        age_patterns = ['age_at_', 'age_']
        duration_patterns = ['duration', '_days']
        count_patterns = ['_count', 'number_of_']
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Check if column matches common patterns
            is_candidate = any(
                pattern in col_lower 
                for pattern in age_patterns + duration_patterns + count_patterns
            )
            
            if is_candidate:
                # Check if column has numeric data with possible nulls
                if df[col].dtype in ['float64', 'object']:
                    # Try to determine if values are actually integers or can be rounded to integers
                    non_null_values = df[col].dropna()
                    if len(non_null_values) > 0:
                        try:
                            # Check if all non-null values are numeric (can be converted to float)
                            # Age fields from calculations may have float precision errors
                            # so we accept any numeric values, not just perfect integers
                            _ = [float(v) for v in non_null_values]
                            columns_to_convert.append(col)
                        except (ValueError, TypeError):
                            pass
    
    # Log what columns were identified for conversion
    if columns_to_convert:
        logger.info(f"Auto-detected {len(columns_to_convert)} columns for Int64 conversion: {columns_to_convert}")
    
    # Convert identified columns
    for col in columns_to_convert:
        if col in df.columns:
            try:
                # Round float values before converting to Int64
                # This handles cases where calculated fields produce floats like 11523.299998995
                if df[col].dtype in ['float64', 'float32']:
                    df[col] = df[col].round().astype('Int64')
                    logger.debug(f"Converted column '{col}' (float) to Int64 dtype")
                else:
                    df[col] = df[col].astype('Int64')
                    logger.debug(f"Converted column '{col}' to Int64 dtype")
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert column '{col}' to Int64: {e}")
    
    return df


def format_date_to_pcgl(
    date_value: Optional[Union[str, datetime, date]],
    context: Optional[Dict[str, Any]] = None
) -> Optional[str]:
    """
    Format date to PCGL standard format (YYYY-MM-DD).
    
    Args:
        date_value: Date value (string, datetime, or date object)
        context: Optional context dict with 'participant_id' or 'record_id' for better error messages
        
    Returns:
        Formatted date string (YYYY-MM-DD), or None if invalid/missing
    """
    if pd.isna(date_value):
        return None
    
    try:
        # Parse date first
        parsed_date = parse_date(date_value, context=context)
        if parsed_date:
            return parsed_date.strftime('%Y-%m-%d')
        return None
    except Exception as e:
        ctx = _format_context(context)
        logger.warning(f"{ctx}Error formatting date {date_value}: {e}")
        return None


def calculate_duration_in_days(
    start_date: Optional[Union[str, datetime, date]], 
    end_date: Optional[Union[str, datetime, date]],
    date_format: str = '%Y/%m/%d'
) -> Optional[int]:
    """
    Calculate duration in days between two dates.
    
    Args:
        start_date: Start date (string, datetime, or date object)
        end_date: End date (string, datetime, or date object)
        date_format: Format string for parsing date strings (default: '%Y/%m/%d')
                    Note: This is primarily for backward compatibility
        
    Returns:
        Duration in days (int), or None if calculation not possible
    """
    if pd.isna(start_date) or pd.isna(end_date):
        return None
    
    try:
        # Parse start date
        start_dt = parse_date(start_date)
        if start_dt is None:
            return None
        
        # Parse end date
        end_dt = parse_date(end_date)
        if end_dt is None:
            return None
        
        # Calculate duration
        duration = (end_dt - start_dt).days
        
        # Return only non-negative durations
        return duration if duration >= 0 else None
        
    except (ValueError, TypeError) as e:
        logger.debug(f"Error calculating duration between {start_date} and {end_date}: {e}")
        return None


def log_mapping_summary(
    logger: logging.Logger,
    total_records: int,
    entity_name: str,
    participant_count: Optional[int] = None,
    additional_stats: Optional[Dict[str, Any]] = None
):
    """
    Log standardized mapping summary.
    
    Args:
        logger: Logger instance
        total_records: Total number of records created
        entity_name: Name of entity being mapped
        participant_count: Number of unique participants (optional)
        additional_stats: Additional statistics to log (optional)
    """
    logger.info("=" * 80)
    logger.info(f"{entity_name} Mapping Summary")
    logger.info("=" * 80)
    logger.info(f"Total {entity_name} records: {total_records}")
    
    if participant_count is not None:
        logger.info(f"Unique participants: {participant_count}")
        if total_records > 0 and participant_count > 0:
            avg_per_participant = total_records / participant_count
            logger.info(f"Average records per participant: {avg_per_participant:.2f}")
    
    if additional_stats:
        logger.info("Additional statistics:")
        for key, value in additional_stats.items():
            logger.info(f"  - {key}: {value}")
    
    logger.info("=" * 80)

# ============================================================================
# DATA CLEANING FUNCTIONS
# ============================================================================

def clean_numeric_string(value: Any) -> Optional[str]:
    """
    Clean numeric string by removing formatting (commas, etc.).
    
    Handles Excel-style number formatting like "2,019" or "1,234.56"
    
    Args:
        value: Numeric value (may be string with formatting)
        
    Returns:
        Cleaned string, or None if invalid
        
    Examples:
        >>> clean_numeric_string("2,019")
        '2019'
        >>> clean_numeric_string("1,234.56")
        '1234.56'
        >>> clean_numeric_string(None)
        None
    """
    if pd.isna(value):
        return None
    
    # Remove commas and extra whitespace
    cleaned = str(value).replace(',', '').strip()
    
    return cleaned if len(cleaned) > 0 else None


def safe_int_conversion(
    value: Any,
    default: Optional[int] = None
) -> Optional[int]:
    """
    Safely convert value to integer with fallback.
    
    Handles string formatting, float conversion, and invalid inputs gracefully.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Integer value, or default if conversion fails
        
    Examples:
        >>> safe_int_conversion("42")
        42
        >>> safe_int_conversion("2,019")
        2019
        >>> safe_int_conversion("invalid", default=0)
        0
        >>> safe_int_conversion(None)
        None
    """
    if pd.isna(value) or value == '':
        return default
    
    try:
        # Clean numeric string first
        cleaned = clean_numeric_string(value)
        if cleaned:
            return int(float(cleaned))
    except (ValueError, TypeError):
        pass
    
    return default

