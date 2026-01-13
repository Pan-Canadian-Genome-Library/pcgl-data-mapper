"""
Custom Transformation Functions for {StudyName} Study

CUSTOMIZATION INSTRUCTIONS:
1. Replace {StudyName} with your study name
2. Implement your actual custom functions below
3. Update function names and logic to match your study needs
4. Register functions in __init__.py {STUDYNAME}_CUSTOM_FUNCTIONS dict

These functions are called from YAML configurations:
- Expansion functions: entity.function in YAML (for pattern: custom)
- Age functions: age_params.custom_function in YAML
"""

import pandas as pd
import logging
from datetime import date
from typing import Optional, Any, List, Dict

logger = logging.getLogger(__name__)


# ============================================================================
# EXPANSION FUNCTIONS
# ============================================================================

def example_custom_expansion_function(
    source_df: pd.DataFrame,
    config: Any,
    params: dict,
    **kwargs
) -> pd.DataFrame:
    """
    Example custom expansion function for converting wide data to long format.
    
    CUSTOMIZE THIS FUNCTION for your study's specific expansion needs.
    
    Called from YAML when:
        entity:
          pattern: custom
          function: example_custom_expansion_function
          params:
            your_param1: value1
            your_param2: value2
    
    Args:
        source_df: Source DataFrame with wide-format data
        config: MappingConfig instance containing entity configuration
        params: Parameters from params in YAML
        **kwargs: Additional arguments
        
    Returns:
        Expanded DataFrame in long format with one row per expanded item
        
    Example:
        Input (wide):
            participant_id | relationship_1 | relationship_2
            P001          | Mother        | Father
            
        Output (long):
            participant_id | relationship
            P001          | Mother
            P001          | Father
    """
    logger.info("Using custom expansion function: example_custom_expansion_function")
    
    # Get parameters from YAML
    participant_id_field = params.get('participant_id_field', 'participant_id')
    # CUSTOMIZE: Add more parameters as needed
    
    expanded_records = []
    
    # Process each source row
    for idx, source_row in source_df.iterrows():
        participant_id = source_row.get(participant_id_field)
        
        if pd.isna(participant_id):
            continue
        
        # CUSTOMIZE: Implement your expansion logic here
        # Example: Expand multiple relationship fields into separate records
        relationship_fields = ['relationship_1', 'relationship_2', 'relationship_3']
        
        for rel_field in relationship_fields:
            rel_value = source_row.get(rel_field)
            
            if pd.notna(rel_value):
                # Create base record
                record = {field: None for field in config.entity_fields}
                
                # Populate from mappings
                for field_config in config.mappings:
                    # Apply standard field mapping logic
                    # (This is simplified - actual implementation would use
                    # _apply_field_mapping_to_record from EntityMapper)
                    target_field = field_config.get('target_field')
                    source_field = field_config.get('source_field')
                    
                    if source_field and target_field:
                        record[target_field] = source_row.get(source_field)
                
                # Add expanded field value
                record['relationship'] = rel_value
                
                expanded_records.append(record)
    
    logger.info(f"Expanded {len(source_df)} rows into {len(expanded_records)} records")
    
    return pd.DataFrame(expanded_records)


# ============================================================================
# DATE/AGE CONSTRUCTION FUNCTIONS
# ============================================================================

def example_custom_age_function(
    row: pd.Series,
    params: dict
) -> Optional[date]:
    """
    Example custom function to construct birth date from partial information.
    
    CUSTOMIZE THIS FUNCTION for your study's date construction needs.
    
    Called from YAML when:
        target_type: age
        params:
          custom_function: example_custom_age_function
          year_field: birth_year
          month_field: birth_month
          # ... other params
    
    Args:
        row: Source data row (pandas Series)
        params: Parameters from YAML age_params
        
    Returns:
        Constructed date object, or None if construction failed
        
    Example:
        Input: birth_year=1990, birth_month=6
        Output: date(1990, 6, 15)  # Assumes mid-month
    """
    try:
        # Get field names from params
        year_field = params.get('year_field')
        month_field = params.get('month_field')
        day_field = params.get('day_field')  # Optional
        
        # Extract values
        year = row.get(year_field)
        month = row.get(month_field)
        
        if pd.isna(year) or pd.isna(month):
            return None
        
        # Convert to integers
        year_val = int(float(year))
        month_val = int(float(month))
        
        # Get day or assume mid-month
        if day_field and pd.notna(row.get(day_field)):
            day_val = int(float(row.get(day_field)))
        else:
            day_val = 15  # Assume mid-month if day not provided
        
        # Construct date
        return date(year_val, month_val, day_val)
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Error constructing date: {e}")
        return None


# ============================================================================
# DATA TRANSFORMATION FUNCTIONS
# ============================================================================

def example_data_cleaning_function(
    value: Any,
    params: dict
) -> Optional[str]:
    """
    Example function for custom data cleaning/transformation.
    
    CUSTOMIZE THIS for your study's specific data cleaning needs.
    
    Args:
        value: Raw value to clean
        params: Cleaning parameters
        
    Returns:
        Cleaned value
    """
    if pd.isna(value):
        return None
    
    # CUSTOMIZE: Implement your cleaning logic
    cleaned = str(value).strip().upper()
    
    # Example: Apply study-specific transformations
    replacements = params.get('replacements', {})
    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)
    
    return cleaned if cleaned else None


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def example_custom_validation(
    df: pd.DataFrame,
    params: dict
) -> List[str]:
    """
    Example custom validation function for study-specific checks.
    
    CUSTOMIZE THIS for your study's validation requirements.
    
    Args:
        df: Mapped DataFrame to validate
        params: Validation parameters
        
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    
    # CUSTOMIZE: Implement your validation logic
    # Example: Check for required field combinations
    required_combos = params.get('required_combinations', [])
    
    for combo in required_combos:
        fields = combo.get('fields', [])
        missing = df[fields].isna().all(axis=1)
        
        if missing.any():
            errors.append(
                f"Found {missing.sum()} records missing all of {fields}"
            )
    
    return errors


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def example_helper_function(value: Any) -> Any:
    """
    Example helper function for common transformations.
    
    CUSTOMIZE THIS for reusable logic within your custom functions.
    """
    # Your helper logic here
    return value


# Export all custom functions for testing
__all__ = [
    'example_custom_expansion_function',
    'example_custom_age_function',
    'example_data_cleaning_function',
    'example_custom_validation',
]
