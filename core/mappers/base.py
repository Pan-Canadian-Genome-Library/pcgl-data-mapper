"""
Core Mappers Module - PCGL Data Mapping Framework

This module provides the complete foundation for YAML-driven entity mapping in the PCGL
data mapping framework. It includes three main classes that work together to transform
research data into PCGL schema format.

Classes:
    MappingConfig: Configuration container that loads and parses YAML files
    EntityMapper: Complete YAML-driven mapper for individual entities
    StudyDataMapper: Orchestrator for processing entire studies with auto-discovery of entities
"""

import logging
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
import pandas as pd
import yaml
import importlib
from datetime import datetime
import fnmatch
import re

from .utils import (
    _map_field_value,
    log_mapping_summary,
    convert_nullable_int_columns,
    validate_participant_id,
    validate_age_in_days,
    read_data_file,
)
from .record_transforms import (
    apply_value_to_record,
    apply_age_to_record,
    apply_identifier_to_record,
    apply_note_to_record,
    apply_date_to_record,
    apply_duration_to_record,
    apply_integer_to_record
)


# Configure logging
logger = logging.getLogger(__name__)

__all__ = ['EntityMapper', 'MappingConfig', 'StudyDataMapper']


class MappingConfig:
    """
    Container for mapping configuration data.
    
    This class encapsulates all configuration needed for mapping a specific entity.
    It can be loaded from YAML files or constructed from Python dictionaries.
    """
    
    def __init__(self, config_dict: Dict[str, Any]):
        """
        Initialize mapping configuration.
        
        Expected YAML structure:
        - entity: {name, schema, fields, pattern, function, params}
        - mappings: List of field configurations
        - configs: Dictionary of checkbox configurations for expansion entities
        
        Args:
            config_dict: Dictionary containing mapping configuration
        """
        # Parse entity block
        entity_config = config_dict.get('entity', {})
        self.entity_name = entity_config.get('name', 'Unknown')
        self.target_schema = entity_config.get('schema', 'base')
        if isinstance(self.target_schema, list):
            self.target_schema = self.target_schema[0]  # Use first schema
        
        # Parse fields - expects dict format: { base: [...], extension: [...] }
        fields_config = entity_config.get('fields', {})
        
        # Validate format
        if not isinstance(fields_config, dict):
            raise ValueError(
                f"Invalid 'fields' configuration in entity '{self.entity_name}'. "
                f"Expected dict format with 'base' and 'extension' keys, got {type(fields_config).__name__}. "
                f"Required format:\n"
                f"  fields:\n"
                f"    base:\n"
                f"      - field1\n"
                f"      - field2\n"
                f"    extension:\n"
                f"      - field3"
            )
        
        # Combine all schema fields into a single list for record initialization
        self.entity_fields = []
        # Process schemas in consistent order: base first, then extension, then others
        for schema_name in ['base', 'extension']:
            if schema_name in fields_config:
                field_list = fields_config[schema_name]
                if isinstance(field_list, list):
                    self.entity_fields.extend(field_list)
        # Add any other schemas not in base/extension
        for schema_name, field_list in fields_config.items():
            if schema_name not in ['base', 'extension'] and isinstance(field_list, list):
                self.entity_fields.extend(field_list)
        # Store original structure for schema-specific field access
        self.entity_fields_by_schema = fields_config
        
        self.entity_pattern = entity_config.get('pattern', 'direct')
        self.custom_function = entity_config.get('function')  # For custom pattern
        self.pattern_params = entity_config.get('params', {})  # For any pattern
        # Allow custom prefix for code/term fields (default to entity name)
        self.code_term_prefix = entity_config.get('code_term_prefix', self.entity_name.lower())
        
        # Parse source_files configuration (for multi-file support)
        self.source_files = self._parse_source_files(entity_config)
        
        # Mappings are a list of field configs
        self.mappings = config_dict.get('mappings', []) or []
        
        # Configs for checkbox expansion (comorbidity, phenotype, etc.)
        # Changed from dict to list format: configs is now a list with source_field property
        self.configs = config_dict.get('configs', []) or []
        
        # Common configuration fields - handle None values when keys exist but are empty
        self.preprocessing = config_dict.get('preprocessing', []) or []
        self.filters = config_dict.get('filters', {}) or {}
        self.validations = config_dict.get('validations', []) or []
        self.post_processing = config_dict.get('post_processing', []) or []
        
    @classmethod
    def from_yaml(cls, yaml_path: Path) -> 'MappingConfig':
        """
        Load mapping configuration from YAML file.
        
        Args:
            yaml_path: Path to YAML configuration file
            
        Returns:
            MappingConfig instance
        """
        with open(yaml_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        return cls(config_dict)
    
    def _parse_source_files(self, entity_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse source_files configuration from entity config.
        
        Supports multiple formats:
        1. No config: None (backward compatible - uses default input)
        2. Single string: source_file: "file.csv"
        3. Named roles: source_files: {primary: "...", secondary: [...]}
        
        Args:
            entity_config: Entity configuration dictionary
            
        Returns:
            Parsed source files configuration or None
        """
        # Check for source_file (singular) - simple string format
        source_file = entity_config.get('source_file')
        if source_file:
            if isinstance(source_file, str):
                return {
                    'primary': source_file,
                    'secondary': []
                }
        
        # Check for source_files (plural) - named roles or list format
        source_files = entity_config.get('source_files')
        if not source_files:
            # No source files specified - backward compatible mode
            return None
        
        # Handle named roles format
        if isinstance(source_files, dict):
            primary = source_files.get('primary')
            secondary = source_files.get('secondary', [])
            
            if not primary:
                raise ValueError(
                    f"Entity '{entity_config.get('name')}': source_files must specify 'primary' file"
                )
            
            # Normalize secondary to list format
            if not isinstance(secondary, list):
                secondary = [secondary] if secondary else []
            
            # Parse each secondary file configuration
            parsed_secondary = []
            for sec_file in secondary:
                if isinstance(sec_file, str):
                    # Simple string - use defaults
                    parsed_secondary.append({
                        'file': sec_file,
                        'join_on': 'participant_id',  # Default join key
                        'join_type': 'left',           # Default join type
                        'columns': None                # Load all columns
                    })
                elif isinstance(sec_file, dict):
                    # Full configuration object
                    file_path = sec_file.get('file')
                    if not file_path:
                        raise ValueError(
                            f"Entity '{entity_config.get('name')}': secondary file must specify 'file' path"
                        )
                    
                    # file_path can be a string (single file) or list (multiple files to concatenate)
                    # Validate that list elements are strings
                    if isinstance(file_path, list):
                        if not all(isinstance(f, str) for f in file_path):
                            raise ValueError(
                                f"Entity '{entity_config.get('name')}': when 'file' is a list, "
                                f"all elements must be strings (file names)"
                            )
                    elif not isinstance(file_path, str):
                        raise ValueError(
                            f"Entity '{entity_config.get('name')}': 'file' must be a string or list of strings"
                        )
                    
                    join_on = sec_file.get('join_on', 'participant_id')
                    join_type = sec_file.get('join_type', 'left')
                    columns = sec_file.get('columns')
                    
                    # Validate join_type
                    valid_join_types = ['left', 'right', 'inner', 'outer']
                    if join_type not in valid_join_types:
                        raise ValueError(
                            f"Entity '{entity_config.get('name')}': invalid join_type '{join_type}'. "
                            f"Must be one of: {valid_join_types}"
                        )
                    
                    parsed_secondary.append({
                        'file': file_path,  # Can be string or list
                        'join_on': join_on,
                        'join_type': join_type,
                        'columns': columns
                    })
                else:
                    raise ValueError(
                        f"Entity '{entity_config.get('name')}': secondary file must be string or dict, "
                        f"got {type(sec_file).__name__}"
                    )
            
            return {
                'primary': primary,
                'secondary': parsed_secondary
            }
        
        raise ValueError(
            f"Entity '{entity_config.get('name')}': source_files must be a dict with 'primary' and 'secondary' keys"
        )
    

class EntityMapper:
    """
    Entity mapper with complete YAML-driven implementation.
    
    This class provides a complete, production-ready mapper that works with
    standardized YAML configuration. 
    
    All entity mappers can now use this class directly with their YAML config.
    No need for subclassing unless you have very specific custom logic.
    """
    
    def __init__(
        self,
        config: MappingConfig,
        study_id: str,
        custom_functions: Optional[Dict[str, Callable]] = None
    ):
        """
        Initialize entity mapper.
        
        Args:
            config: Mapping configuration
            study_id: Study identifier (e.g., 'HostSeq', 'BQC19')
            custom_functions: Optional dictionary of custom transformation functions
        """
        self.config = config
        self.study_id = study_id
        self.custom_functions = custom_functions or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Cache for field descriptions (used in note aggregation)
        self._field_descriptions: Dict[str, pd.Series] = {}
        
        # Statistics tracking
        self.stats = {
            'input_records': 0,
            'filtered_records': 0,
            'output_records': 0,
            'validation_errors': 0,
            'warnings': 0
        }
    
    def map(
        self,
        source_df: pd.DataFrame,
        schema: Optional[Any] = None,
        source_date: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Main entry point for mapping. Orchestrates the entire mapping process.
        
        This is the standard interface called by the main data mapper. All entity
        mappers follow this signature.
        
        Args:
            source_df: Source DataFrame with raw data
            schema: Optional PCGL schema for validation
            source_date: Optional source data date
            **kwargs: Additional mapper-specific arguments
            
        Returns:
            Mapped DataFrame in target schema format
        """
        self.logger.info("=" * 80)
        self.logger.info(f"{self.config.entity_name} Mapper - {self.study_id}")
        self.logger.info("=" * 80)
        
        # Track input
        self.stats['input_records'] = len(source_df)
        self.logger.info(f"Input records: {self.stats['input_records']}")
        
        # 1. Preprocess
        preprocessed_df = self.preprocess(source_df, **kwargs)
        
        if preprocessed_df.empty:
            self.logger.warning("No records after preprocessing")
            return self._empty_dataframe()
        
        self.stats['filtered_records'] = len(preprocessed_df)
        self.logger.info(f"Records after preprocessing: {self.stats['filtered_records']}")
        
        # 2. Map fields
        mapped_df = self.map_fields(preprocessed_df, **kwargs)
        
        if mapped_df.empty:
            self.logger.warning("No records after field mapping")
            return self._empty_dataframe()
        
        # 3. Post-process
        final_df = self.postprocess(mapped_df, **kwargs)
        
        # 4. Validate
        validation_errors = self.validate_mapped_data(final_df)
        self.stats['validation_errors'] = len(validation_errors)
        
        if validation_errors:
            self.logger.warning(f"Found {len(validation_errors)} validation errors")
            for error in validation_errors[:10]:  # Log first 10 errors
                self.logger.warning(f"  - {error}")
            if len(validation_errors) > 10:
                self.logger.warning(f"  ... and {len(validation_errors) - 10} more errors")
        
        # 5. Log summary
        self.stats['output_records'] = len(final_df)
        self._log_summary()
        
        return final_df
    
    def preprocess(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Preprocess source data before mapping.
        
        Applies preprocessing steps from YAML configuration:
        - clean_numeric: Remove commas, spaces from numeric fields
        - strip_whitespace: Remove leading/trailing whitespace
        - uppercase/lowercase: Convert text case
        - field_filters: Filter rows based on field values
        - REDCap filtering: Handle baseline/repeat instruments (auto-detected)
        
        Override this method to add entity-specific or study-specific preprocessing.
        
        Args:
            source_df: Source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Preprocessed DataFrame
        """
        df = source_df.copy()
        
        # Apply preprocessing steps from configuration
        for step in self.config.preprocessing:
            step_type = step.get('type')
            fields = step.get('fields', [])
            
            if step_type == 'clean_numeric':
                # Remove commas, spaces from numeric fields
                # Support field patterns (e.g., '*_numeric', 'measurement_*')
                fields_to_clean = self._resolve_field_patterns(df, fields)
                
                for field in fields_to_clean:
                    if field in df.columns:
                        df[field] = df[field].astype(str).str.replace(',', '', regex=False)
                        df[field] = df[field].str.replace(' ', '', regex=False)
                        df[field] = pd.to_numeric(df[field], errors='coerce')
                        self.logger.debug(f"Cleaned numeric field: {field}")
                
                if fields_to_clean:
                    self.logger.info(f"Cleaned {len(fields_to_clean)} numeric fields")
            
            elif step_type == 'strip_whitespace':
                # Remove leading/trailing whitespace
                for field in fields:
                    if field in df.columns:
                        df[field] = df[field].astype(str).str.strip()
                        self.logger.debug(f"Stripped whitespace from: {field}")
            
            elif step_type == 'uppercase':
                # Convert to uppercase
                for field in fields:
                    if field in df.columns:
                        df[field] = df[field].astype(str).str.upper()
                        self.logger.debug(f"Converted to uppercase: {field}")
            
            elif step_type == 'lowercase':
                # Convert to lowercase
                for field in fields:
                    if field in df.columns:
                        df[field] = df[field].astype(str).str.lower()
                        self.logger.debug(f"Converted to lowercase: {field}")
            
            elif step_type == 'calculate_field':
                # Calculate new field using pandas eval() with formula
                target = step.get('target')
                formula = step.get('formula')
                
                if not target or not formula:
                    self.logger.warning(f"Skipping incomplete calculate_field: missing 'target' or 'formula'")
                    continue
                
                try:
                    # Use pandas eval() for safe formula evaluation
                    # Automatically handles column references and NaN propagation
                    df[target] = df.eval(formula)
                    self.logger.info(f"Calculated field '{target}' using formula: {formula}")
                except Exception as e:
                    self.logger.error(
                        f"Error calculating field '{target}' with formula '{formula}': {e}\n"
                        f"Hint: Ensure all column names in the formula exist in the data. "
                        f"Use column.fillna(0) to handle nulls, or allow NaN propagation (default)."
                    )
                    raise ValueError(
                        f"Invalid calculate_field formula for '{target}': {formula}\n"
                        f"Error: {e}"
                    ) from e
            
            else:
                self.logger.warning(f"Unknown preprocessing type: {step_type}")
        
        # Apply field-based filters from configuration
        field_filters = self.config.filters.get('field_filters', [])
        if field_filters:
            df = self._apply_field_filters(df, field_filters)
        
        # Apply REDCap baseline/repeat instrument filtering
        include_rows = self.config.filters.get('include_rows')
        if include_rows:
            df = self._apply_redcap_filtering(df)
        
        return df
    
    def _resolve_field_patterns(self, df: pd.DataFrame, field_patterns: list) -> list:
        """
        Resolve field patterns to actual column names.
        
        Supports:
        - Exact field names: ['field1', 'field2']
        - Wildcard patterns: ['*_numeric', 'measurement_*', '*_result_*']
        - Special value 'auto': Detects all numeric-looking columns
        
        Args:
            df: DataFrame with columns to match
            field_patterns: List of field names or patterns
            
        Returns:
            List of actual column names that match the patterns
        """
        
        if not field_patterns:
            return []
        
        # Handle 'auto' - detect columns that look numeric
        if field_patterns == 'auto' or (isinstance(field_patterns, list) and 'auto' in field_patterns):
            # Find columns with numeric-looking data (containing commas or spaces)
            numeric_cols = []
            for col in df.columns:
                if df[col].dtype == object:  # String column
                    # Check if any values contain commas or spaces in numbers
                    sample = df[col].dropna().astype(str).head(100)
                    if sample.str.match(r'^[\d,\s]+\.?\d*$').any():
                        numeric_cols.append(col)
            return numeric_cols
        
        matched_fields = []
        
        for pattern in field_patterns:
            if '*' in pattern or '?' in pattern:
                # Wildcard pattern - match against all columns
                for col in df.columns:
                    if fnmatch.fnmatch(col, pattern) and col not in matched_fields:
                        matched_fields.append(col)
            else:
                # Exact field name
                if pattern not in matched_fields:
                    matched_fields.append(pattern)
        
        return matched_fields
    
    def _apply_redcap_filtering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply REDCap baseline/repeat instrument filtering.
        
        Universal REDCap pattern that works for any study using REDCap.
        Auto-detects REDCap structure and applies include_rows configuration.
        
        Configuration (YAML filters section):
        ```yaml
        filters:
          eligible_participants: true  # Optional: triggers study-specific filtering
          include_rows:
            baseline: true
            repeat_instruments: ["lab_results", "visits"]
          merge_baseline_fields: ["dob_year", "sex"]
          participant_id_field: "participant_id"
        ```
        
        Args:
            df: DataFrame to filter
            
        Returns:
            Filtered DataFrame
        """
        # Check if this is REDCap data
        is_redcap = 'redcap_repeat_instrument' in df.columns
        
        # Get configuration
        participant_id_field = self.config.filters.get('participant_id_field', 'participant_id')
        include_rows = self.config.filters.get('include_rows', {})
        merge_fields = self.config.filters.get('merge_baseline_fields', [])
        
        # STEP 1: Handle non-REDCap data
        if not is_redcap:
            # For non-REDCap data, just apply eligibility filter if configured
            if self.config.filters.get('eligible_participants', False):
                self.logger.info("Applying participant eligibility filtering (non-REDCap)")
                return self._filter_eligible_participants(df)
            return df
        
        # STEP 2: Extract baseline rows
        baseline_df = df[df['redcap_repeat_instrument'].isna()].copy()
        self.logger.info(f"Found {len(baseline_df)} baseline records")
        
        # STEP 3: Apply eligibility filtering to baseline
        if self.config.filters.get('eligible_participants', False):
            self.logger.info("Applying participant eligibility filtering")
            eligible_baseline = self._filter_eligible_participants(baseline_df)
            eligible_ids = set(eligible_baseline[participant_id_field].unique())
            self.logger.info(f"Found {len(eligible_ids)} eligible participants")
        else:
            eligible_baseline = baseline_df
            eligible_ids = set(baseline_df[participant_id_field].unique())
            self.logger.info(f"All {len(eligible_ids)} participants included (no eligibility filter)")
        
        # STEP 4: Include specified rows based on configuration
        include_baseline = include_rows.get('baseline', True)
        repeat_instruments = include_rows.get('repeat_instruments', [])
        
        rows_to_include = []
        
        # Include baseline rows for eligible participants
        if include_baseline:
            rows_to_include.append(eligible_baseline)
            self.logger.info(f"Including {len(eligible_baseline)} baseline rows")
        
        # Include repeat instrument rows for eligible participants
        if repeat_instruments:
            for instrument in repeat_instruments:
                instrument_rows = df[
                    (df['redcap_repeat_instrument'] == instrument) &
                    (df[participant_id_field].isin(eligible_ids))
                ].copy()
                rows_to_include.append(instrument_rows)
                self.logger.info(f"Including {len(instrument_rows)} '{instrument}' repeat rows")
        
        if not rows_to_include:
            self.logger.warning("No rows to include based on configuration")
            return pd.DataFrame(columns=df.columns)
        
        # Combine all included rows
        filtered_df = pd.concat(rows_to_include, ignore_index=True)
        self.logger.info(f"Total rows after REDCap filtering: {len(filtered_df)}")
        
        # STEP 5: Merge baseline fields into repeat rows if needed
        if merge_fields and repeat_instruments:
            filtered_df = self._merge_baseline_fields(
                filtered_df, 
                eligible_baseline, 
                participant_id_field, 
                merge_fields
            )
        
        return filtered_df
    
    def _filter_eligible_participants(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter for eligible participants based on study-specific criteria.
        
        Supports two patterns:
        1. YAML Configuration (Simple): Define filters.participant_eligibility in YAML
        2. Method Override (Complex): Override this method in study-specific mapper
        
        YAML Pattern (for simple AND-combined conditions):
        ```yaml
        filters:
          eligible_participants: true
          participant_eligibility:
            - field: consent
              operator: equals
              value: 1
            - field: age_years
              operator: greater_equal
              value: 18
            - field: enrollment_status
              operator: in
              value: ["Active", "Complete"]
        ```
        
        Override Pattern (for complex logic like HostSeq):
        ```python
        def _filter_eligible_participants(self, df: pd.DataFrame) -> pd.DataFrame:
            # Complex multi-field logic with OR conditions, nested AND/OR, etc.
            consent_mask = (df['consent'] == 0) | (df['consent'].isna())
            covid_mask = (df['suspected'] == 1) & ((df['test'] == 0) | (df['test'] == 2))
            return df[~(consent_mask | covid_mask)].copy()
        ```
        
        Args:
            df: DataFrame to filter (usually baseline records for REDCap)
            
        Returns:
            Filtered DataFrame with only eligible participants
        """
        # Check if YAML eligibility conditions are configured
        participant_eligibility = self.config.filters.get('participant_eligibility')
        
        if participant_eligibility:
            # Use YAML-based filtering (expects list of filter dicts)
            self.logger.info("Applying YAML-configured participant eligibility filters")
            return self._apply_participant_eligibility_filters(df, participant_eligibility)
        
        # No YAML config - use default or override behavior
        # If this method is overridden in subclass, the override will run
        # Otherwise, return all participants (default stub behavior)
        self.logger.debug("Using default eligibility filter (no exclusions)")
        return df
    
    def _apply_participant_eligibility_filters(
        self,
        df: pd.DataFrame,
        eligibility_conditions: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Apply YAML-configured participant eligibility filters.
        
        Uses same operators as field_filters for consistency.
        See _apply_filters() for supported operators.
        
        Args:
            df: DataFrame to filter
            eligibility_conditions: List of filter configurations from YAML
            
        Returns:
            Filtered DataFrame
        """
        filtered_df, initial_count, final_count = self._apply_filters(
            df, eligibility_conditions, "eligibility"
        )
        
        total_excluded = initial_count - final_count
        if total_excluded > 0:
            retention_rate = 100 * final_count / initial_count if initial_count > 0 else 0
            self.logger.info(
                f"Participant eligibility complete: {initial_count} → {final_count} participants "
                f"(excluded {total_excluded}, retention {retention_rate:.1f}%)"
            )
        
        return filtered_df
    
    def _apply_filters(
        self,
        df: pd.DataFrame,
        filters: List[Dict[str, Any]],
        filter_type: str = "field"
    ) -> tuple:
        """
        Apply filtering conditions to DataFrame (shared logic for field and eligibility filters).
        
        All filters are combined with AND logic.
        
        Supported operators:
        - equals, not_equals: Field equals/doesn't equal value
        - in, not_in: Field value in/not in list
        - is_not_null, is_null: Field is/isn't null/NaN
        - greater_than, less_than: Field > or < value (numeric)
        - greater_equal, less_equal: Field >= or <= value (numeric)
        - regex_match_any: Field matches any of the regex patterns (value is list of regex strings)
        
        Args:
            df: DataFrame to filter
            filters: List of filter configurations
            filter_type: Type of filter for logging ("field" or "eligibility")
            
        Returns:
            Tuple of (filtered_df, initial_count, final_count)
        """
        initial_count = len(df)
        
        for filter_config in filters:
            field = filter_config.get('field')
            operator = filter_config.get('operator')
            value = filter_config.get('value')
            
            if not field or not operator:
                self.logger.warning(f"Skipping incomplete {filter_type} filter: {filter_config}")
                continue
            
            if field not in df.columns:
                self.logger.warning(f"{filter_type.capitalize()} field '{field}' not found, skipping")
                continue
            
            before_count = len(df)
            
            # Apply filter based on operator (no .copy() needed in loop)
            if operator == 'equals':
                df = df[df[field] == value]
            elif operator == 'not_equals':
                df = df[df[field] != value]
            elif operator == 'in':
                df = df[df[field].isin(value)]
            elif operator == 'not_in':
                df = df[~df[field].isin(value)]
            elif operator == 'is_not_null':
                df = df[df[field].notna()]
            elif operator == 'is_null':
                df = df[df[field].isna()]
            elif operator == 'greater_than':
                df = df[df[field] > value]
            elif operator == 'less_than':
                df = df[df[field] < value]
            elif operator == 'greater_equal':
                df = df[df[field] >= value]
            elif operator == 'less_equal':
                df = df[df[field] <= value]
            elif operator == 'regex_match_any':
                # Match records where field matches any of the regex patterns
                if isinstance(value, list):
                    try:
                        df = df[df[field].astype(str).apply(
                            lambda x: any(re.match(pattern, str(x)) for pattern in value)
                        )]
                    except re.error as e:
                        self.logger.error(
                            f"Invalid regex pattern in {filter_type} filter for field '{field}': {e}\n"
                            f"Patterns: {value}\n"
                            f"Hint: Special regex characters like *, +, ?, etc. need to be escaped or used correctly."
                        )
                        raise ValueError(
                            f"Invalid regex pattern in field '{field}': {e}. "
                            f"Check your filter configuration for special characters that need escaping."
                        ) from e
                else:
                    self.logger.warning(f"regex_match_any expects a list of patterns, got {type(value)}")
                    continue
            else:
                self.logger.warning(f"Unknown operator '{operator}', skipping")
                continue
            
            after_count = len(df)
            excluded = before_count - after_count
            if excluded > 0:
                label = "Eligibility filter" if filter_type == "eligibility" else "Field filter"
                self.logger.info(
                    f"{label}: {field} {operator} {value} "
                    f"({before_count} → {after_count}, excluded {excluded})"
                )
        
        # Single copy at the end
        return df.copy(), initial_count, len(df)
    
    def _apply_field_filters(self, df: pd.DataFrame, field_filters: list) -> pd.DataFrame:
        """
        Apply field-based filtering conditions to include only matching records.
        
        All filters are combined with AND logic.
        See _apply_filters() for supported operators.
        
        Args:
            df: DataFrame to filter
            field_filters: List of filter configurations
            
        Returns:
            Filtered DataFrame
        """
        if not field_filters:
            return df
        
        filtered_df, initial_count, final_count = self._apply_filters(df, field_filters, "field")
        
        total_excluded = initial_count - final_count
        if total_excluded > 0:
            self.logger.info(
                f"Field filtering complete: {initial_count} → {final_count} rows "
                f"({total_excluded} excluded)"
            )
        
        return filtered_df
    
    def _merge_baseline_fields(
        self, 
        df: pd.DataFrame, 
        baseline_df: pd.DataFrame, 
        participant_id_field: str, 
        baseline_fields: list
    ) -> pd.DataFrame:
        """
        Merge baseline fields into repeat/event records.
        
        This allows repeat instrument or event records to access baseline data like
        date of birth, age, or other participant-level information needed
        for calculations.
        
        Universal pattern for longitudinal studies (REDCap and non-REDCap).
        
        Args:
            df: Event/repeat instrument DataFrame
            baseline_df: Baseline records DataFrame
            participant_id_field: Field name for participant ID
            baseline_fields: List of field names to merge from baseline
            
        Returns:
            DataFrame with baseline fields merged in
        """
        if not participant_id_field:
            self.logger.error("participant_id_field is required for merging baseline fields")
            return df
        
        # Select only needed fields from baseline
        merge_fields = [participant_id_field] + baseline_fields
        
        # Filter for fields that exist in baseline_df
        existing_fields = [f for f in merge_fields if f in baseline_df.columns]
        missing_fields = [f for f in merge_fields if f not in baseline_df.columns]
        
        if missing_fields:
            self.logger.warning(f"Baseline fields not found in source data: {missing_fields}")
        
        if len(existing_fields) <= 1:  # Only participant ID field
            self.logger.warning("No baseline fields available to merge")
            return df
        
        # Get unique baseline records (one per participant)
        baseline_subset = baseline_df[existing_fields].drop_duplicates(subset=[participant_id_field])
        
        self.logger.info(f"Merging baseline fields into event records: {baseline_fields}")
        
        # Merge with event/repeat records
        # Use suffixes to handle conflicts (baseline fields take precedence if they don't exist in df)
        df = df.merge(
            baseline_subset,
            on=participant_id_field,
            how='left',
            suffixes=('', '_baseline')
        )
        
        # For fields that existed in both, use baseline value if original was null
        for field in baseline_fields:
            if field in df.columns and f"{field}_baseline" in df.columns:
                # Fill nulls in original with baseline values
                df[field] = df[field].fillna(df[f"{field}_baseline"])
                # Drop the _baseline column
                df = df.drop(columns=[f"{field}_baseline"])
        
        return df
    
    def map_fields(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Map source fields to target schema fields using configuration.
        
        This method checks entity.pattern to determine mapping strategy:
        - 'direct': One-to-one mapping (one output record per input record)
        - 'expansion': Wide-to-long mapping (multiple output records per input record)
        - 'custom': Custom transformation logic via entity.function
        
        Args:
            source_df: Preprocessed source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            DataFrame with mapped fields (direct, expanded, or custom)
        """
        # Check entity pattern to determine mapping strategy
        if self.config.entity_pattern == 'expansion':
            # Use expansion pattern: wide-to-long conversion
            return self._map_expansion_pattern(source_df, **kwargs)
        elif self.config.entity_pattern == 'custom':
            # Use custom pattern: user-defined transformation
            return self._map_custom_pattern(source_df, **kwargs)
        else:
            # Use direct pattern: one-to-one mapping
            return self._map_direct_pattern(source_df, **kwargs)
    
    def _map_direct_pattern(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Map using direct pattern (one-to-one mapping).
        
        1. Initialize DataFrame with all entity fields from entity.fields
        2. Apply mappings from mappings block using _apply_field_mapping_to_record
        3. Unmapped fields remain null
        
        Args:
            source_df: Source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Mapped DataFrame (same number of rows as input)
        """
        # Initialize list to collect records
        records = []
        
        # Process each row
        for idx, source_row in source_df.iterrows():
            # Initialize record with all entity fields = None
            record = {field: None for field in self.config.entity_fields}
            
            # Apply each field mapping
            for field_config in self.config.mappings:
                try:
                    self._apply_field_mapping_to_record(
                        record=record,
                        field_config=field_config,
                        source_row=source_row,
                        default_source_field=None
                    )
                except Exception as e:
                    self.logger.error(f"Error mapping field {field_config.get('target_field')}: {e}")
                    self.stats['warnings'] += 1
            
            records.append(record)
        
        # Convert to DataFrame
        mapped_df = pd.DataFrame(records, index=source_df.index)
        
        return mapped_df
    
    def _map_expansion_pattern(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Map using expansion pattern (checkbox wide-to-long conversion).
        
        Creates N records from checkbox configurations. Each checked checkbox
        generates one output record with code/term from the config.
        
        Args:
            source_df: Source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Expanded DataFrame (N records per input row, where N = number of checked boxes)
        """
        # Get expansion parameters with fallbacks
        # Check params first, then filters, then use default
        participant_id_field = (
            self.config.pattern_params.get('participant_id_field') or 
            self.config.filters.get('participant_id_field') 
        )
        skip_values = self.config.pattern_params.get('skip_values', [0])
        
        # Collect all expanded records
        expanded_records = []
        
        # Process each participant
        for idx, source_row in source_df.iterrows():
            participant_id = source_row.get(participant_id_field)
            if pd.isna(participant_id):
                continue
            
            # Process each configured checkbox field
            for config in self.config.configs:
                checkbox_field = config.get('source_field')
                if not checkbox_field:
                    continue
                
                # Check checkbox value
                checkbox_value = source_row.get(checkbox_field)
                if pd.isna(checkbox_value):
                    continue
                
                # Try to convert to int for numeric radio/checkbox values
                # But also support string values for categorical radio buttons
                numeric_value = None
                try:
                    numeric_value = int(float(checkbox_value))
                    # Check if numeric value is in skip_values
                    if numeric_value in skip_values:
                        continue
                except (ValueError, TypeError):
                    # Non-numeric value (e.g., 'A', 'AB', 'O' for blood type)
                    # Check if the string representation is in skip_values
                    if checkbox_value in skip_values or str(checkbox_value) in skip_values:
                        continue
                    # Use the original value
                    pass
                
                # Create record(s) for this checkbox
                records = self._create_records_for_checkbox(
                    source_row=source_row,
                    checkbox_field=checkbox_field,
                    checkbox_value=checkbox_value,
                    config=config
                )
                
                expanded_records.extend(records)
        
        if not expanded_records:
            self.logger.warning(f"No records created from expansion for {self.config.entity_name}")
            return pd.DataFrame(columns=self.config.entity_fields)
        
        return pd.DataFrame(expanded_records)
    
    def _map_custom_pattern(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Map using custom pattern (user-defined transformation).
        
        Calls a custom expansion function from CUSTOM_FUNCTIONS to perform
        arbitrary transformations. Use for complex wide-to-long conversions
        that don't fit the standard checkbox expansion pattern.
        
        Args:
            source_df: Source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            DataFrame returned by custom function
        """
        if not self.config.custom_function:
            raise ValueError(
                f"Pattern 'custom' requires 'function' to be specified in entity config"
            )
        
        if self.config.custom_function not in self.custom_functions:
            raise ValueError(
                f"Custom function '{self.config.custom_function}' not found in CUSTOM_FUNCTIONS. "
                f"Available functions: {list(self.custom_functions.keys())}"
            )
        
        custom_func = self.custom_functions[self.config.custom_function]
        self.logger.info(f"Using custom pattern with function: {self.config.custom_function}")
        
        result_df = custom_func(
            source_df=source_df,
            config=self.config,
            params=self.config.pattern_params,
            **kwargs
        )
        
        return result_df
    
    def _apply_field_mapping_to_record(
        self,
        record: Dict[str, Any],
        field_config: Dict[str, Any],
        source_row: pd.Series,
        default_source_field: Optional[str] = None
    ) -> None:
        """
        Apply a single field mapping to a record (unified routing logic).
        
        Supports both mappings and enrichments config patterns:
        - source_type='radio': Map from single source field with value_mappings
        - source_type='checkbox': Aggregate checked values with value_mappings
        - target_type='identifier', 'age', 'note', 'date', 'duration', 'custom', 'direct', 'value'
        
        Args:
            record: Record dictionary to update (modified in place)
            field_config: Field configuration from mappings or enrichments block
            source_row: Source data row
            default_source_field: Optional default source field (expansion pattern uses checkbox_field)
        """
        target_field = field_config.get('target_field')
        if not target_field:
            return
        
        # Get source field (use default if not specified and no default_value)
        source_field = field_config.get('source_field')
        default_value = field_config.get('default_value')
        
        # Only fall back to default_source_field if neither source_field nor default_value are specified
        # Note: 'default_value' in field_config checks if the key exists, allowing default_value: null
        if source_field is None and 'default_value' not in field_config and default_source_field:
            source_field = default_source_field
        
        # Check for enrichment-specific source_type handling (radio/checkbox)
        source_type = field_config.get('source_type')
        value_mappings = field_config.get('value_mappings')
        
        try:
            # Handle enrichment source_type patterns first
            if source_type == 'radio':
                # Radio selection - map single value
                source_value = source_row.get(source_field)
                _map_field_value(record, target_field, source_value, value_mappings)
                return
            
            elif source_type == 'checkbox':
                # Checkbox aggregation (create_records=false)
                # Check each checkbox and apply mapping
                # Supports both single values and list values in value_mappings
                append_mode = field_config.get('append', True)  # Default to True for backward compatibility
                
                if value_mappings:
                    for checkbox_field_name, mapped_value in value_mappings.items():
                        checkbox_value = source_row.get(checkbox_field_name)
                        if checkbox_value == 1:
                            # mapped_value can be a single value or a list
                            # target_field can be a single field or a list of fields
                            # _map_field_value handles all combinations
                            _map_field_value(record, target_field, 1, {1: mapped_value}, append_mode=append_mode)
                return
            
            # Route based on target_type (standard mapping logic)
            target_type = field_config.get('target_type', 'direct')
            
            if target_type == 'identifier':
                params = field_config.get('params', {})
                apply_identifier_to_record(record, target_field, source_row, params, source_field)
            
            elif target_type == 'age':
                params = field_config.get('params', {})
                apply_age_to_record(record, target_field, source_row, params, self.custom_functions)
            
            elif target_type == 'note':
                note_fields = field_config.get('source_field', [])
                apply_note_to_record(record, target_field, source_row, note_fields)
            
            elif target_type == 'date':
                apply_date_to_record(record, target_field, source_row, source_field)
            
            elif target_type == 'duration':
                params = field_config.get('params', {})
                start_field = params.get('start_date_field')
                end_field = params.get('end_date_field')
                apply_duration_to_record(record, target_field, source_row, start_field, end_field)
            
            elif target_type == 'integer':
                apply_integer_to_record(record, target_field, source_row, source_field, value_mappings, default_value, self.logger)
            
            else:  # direct, value
                has_default = 'default_value' in field_config
                apply_value_to_record(
                    record, target_field, source_row, source_field, value_mappings, default_value, has_default
                )
        except Exception as e:
            self.logger.warning(f"Error applying mapping for {target_field}: {e}")
    
    def postprocess(self, mapped_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Post-process mapped data.
        
        Includes:
        - Filtering out records with null required fields
        - Aggregating field descriptions into note fields
        - Type conversions (nullable integers, dates, etc.)
        - Column reordering and cleanup
        
        Override this method to add entity-specific post-processing.
        
        Args:
            mapped_df: Transformed DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Final DataFrame ready for output
        """
        df = mapped_df.copy()
        
        # Filter out rows with null required fields
        # This is critical for maintaining referential integrity
        required_fields = []
        for validation in self.config.validations:
            if validation.get('type') == 'required':
                field = validation.get('field')
                if field:
                    required_fields.append(field)
        
        if required_fields:
            initial_count = len(df)
            for field in required_fields:
                if field in df.columns:
                    before_count = len(df)
                    # Get the IDs of records being removed before filtering
                    removed_records = df[df[field].isna()]
                    if len(removed_records) > 0:
                        id_field = 'submitter_participant_id' if 'submitter_participant_id' in df.columns else df.columns[0]
                        removed_ids = removed_records[id_field].tolist()
                        self.logger.warning(f"Removed {len(removed_records)} records with null '{field}': {removed_ids}")
                    
                    df = df[df[field].notna()].copy()
            
            total_removed = initial_count - len(df)
            if total_removed > 0:
                self.logger.info(f"Total records removed due to null required fields: {total_removed}")
        
        # Aggregate field descriptions into note fields
        for field_config in self.config.mappings:
            if field_config.get('target_type') == 'note':
                target_field = field_config['target_field']
                
                # Check if any fields have descriptions in _field_descriptions
                if self._field_descriptions:
                    # Build note by combining descriptions from value mappings
                    def build_note(row):
                        parts = []
                        
                        # Add descriptions from value-mapped fields
                        for source_field, descriptions in self._field_descriptions.items():
                            if source_field in df.columns and row.name in descriptions.index:
                                desc = descriptions.loc[row.name]
                                if pd.notna(desc) and desc:
                                    # Get the actual mapped value
                                    mapped_val = row.get(source_field)
                                    if pd.notna(mapped_val):
                                        parts.append(f"{source_field}: {mapped_val} ({desc})")
                        
                        # Combine with existing note content if any
                        existing_note = row.get(target_field)
                        if pd.notna(existing_note) and existing_note:
                            parts.insert(0, str(existing_note))
                        
                        return '; '.join(parts) if parts else None
                    
                    if target_field in df.columns:
                        df[target_field] = df.apply(build_note, axis=1)
        
        # Apply post-processing steps from configuration
        if self.config.post_processing:
            for step in self.config.post_processing:
                step_type = step.get('type')
                
                if step_type == 'filter_records':
                    # Filter records using shared filter logic
                    df, initial, final = self._apply_filters(df, [step], "post-processing")
                    if initial != final:
                        self.logger.info(
                            f"Post-processing filtering: {initial} → {final} records "
                            f"({initial - final} excluded)"
                        )
                
                elif step_type == 'clean_numeric':
                    # Clean numeric fields in mapped output (remove commas, spaces)
                    fields = step.get('fields', [])
                    fields_to_clean = self._resolve_field_patterns(df, fields)
                    
                    for field in fields_to_clean:
                        if field in df.columns:
                            df[field] = df[field].astype(str).str.replace(',', '', regex=False)
                            df[field] = df[field].str.replace(' ', '', regex=False)
                            df[field] = pd.to_numeric(df[field], errors='coerce')
                            self.logger.debug(f"Cleaned numeric field in output: {field}")
                    
                    if fields_to_clean:
                        self.logger.info(f"Post-processing: Cleaned {len(fields_to_clean)} numeric fields")
                
                elif step_type == 'convert_nullable_int':
                    columns = step.get('columns', 'auto')
                    if columns == 'auto':
                        df = convert_nullable_int_columns(df, auto_detect=True)
                    else:
                        df = convert_nullable_int_columns(df, int_columns=columns, auto_detect=False)
        
        # Apply PCGL data model conditional field rules
        # age_at_death should only be populated when vital_status is 'Deceased'
        if 'vital_status' in df.columns and 'age_at_death' in df.columns:
            has_age_before = df['age_at_death'].notna().sum()
            deceased_mask = df['vital_status'] == 'Deceased'
            df.loc[~deceased_mask, 'age_at_death'] = None
            has_age_after = df['age_at_death'].notna().sum()
            
            if has_age_before > has_age_after:
                self.logger.info(
                    f"PCGL data model rule: age_at_death nulled for non-deceased "
                    f"({has_age_before} → {has_age_after} records)"
                )
        
        return df
    
    def validate_mapped_data(self, mapped_df: pd.DataFrame) -> List[str]:
        """
        Validate mapped data against configured validation rules.
        
        Checks:
        - Required fields are present and not null
        - Values are within valid ranges
        - Participant IDs are valid format
        - Age values are within expected ranges
        - Unique constraints are satisfied
        
        Override this method to add entity-specific validation rules.
        
        Args:
            mapped_df: Mapped DataFrame to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Apply configured validations
        for validation in self.config.validations:
            validation_type = validation.get('type')
            field = validation.get('field')
            
            if validation_type == 'required':
                # Check for required fields
                if field in mapped_df.columns:
                    null_count = mapped_df[field].isna().sum()
                    if null_count > 0:
                        errors.append(f"Required field '{field}' has {null_count} null values")
            
            elif validation_type == 'participant_id':
                # Validate participant IDs
                if field in mapped_df.columns:
                    invalid_ids = ~mapped_df[field].apply(validate_participant_id)
                    if invalid_ids.any():
                        errors.append(f"Field '{field}' has {invalid_ids.sum()} invalid participant IDs")
            
            elif validation_type == 'age_range':
                # Validate age is within range
                if field in mapped_df.columns:
                    min_age = validation.get('min_age', 0)
                    max_age = validation.get('max_age', 120 * 365)
                    invalid_ages = ~mapped_df[field].apply(
                        lambda x: validate_age_in_days(x, min_age, max_age)
                    )
                    if invalid_ages.any():
                        errors.append(f"Field '{field}' has {invalid_ages.sum()} ages outside valid range")
            
            elif validation_type == 'unique':
                # Check for unique values
                if field in mapped_df.columns:
                    duplicates = mapped_df[field].duplicated().sum()
                    if duplicates > 0:
                        errors.append(f"Field '{field}' has {duplicates} duplicate values")
        
        return errors
    
    # ========================================================================
    # EXPANSION PATTERN HELPERS
    # ========================================================================
    
    def _create_records_for_checkbox(
        self,
        source_row: pd.Series,
        checkbox_field: str,
        checkbox_value: int,
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Create one or more records for a single checked checkbox.
        
        Process:
        1. Create base record from mappings (reusing direct pattern logic)
        2. Populate code/term/source_text from config root keys
        3. Check for enrichments that create additional records
        4. Apply all other enrichments
        
        Args:
            source_row: Source data row
            checkbox_field: Name of checkbox field
            checkbox_value: Value of checkbox
            config: Configuration for this checkbox
            
        Returns:
            List of record dictionaries (usually 1, can be N if enrichments create records)
        """
        # Step 1: Initialize record with all entity fields = None
        base_record = {field: None for field in self.config.entity_fields}
        
        # Apply each mapping using unified routing logic
        for field_config in self.config.mappings:
            self._apply_field_mapping_to_record(
                record=base_record,
                field_config=field_config,
                source_row=source_row,
                default_source_field=checkbox_field
            )
        
        # Step 2: Populate code/term/source_text from config root keys
        self._populate_code_term_fields(base_record, config)
        
        # Step 3: Check for enrichments that create additional records
        enrichments = config.get('enrichments', [])
        records_to_expand = []
        other_enrichments = []
        
        for enrichment in enrichments:
            if enrichment.get('source_type') == 'checkbox' and enrichment.get('create_records'):
                records_to_expand.append(enrichment)
            else:
                other_enrichments.append(enrichment)
        
        # Step 4: Expand records if needed
        if records_to_expand:
            # Create N records from checkbox enrichments
            all_records = []
            for expand_enrichment in records_to_expand:
                expanded = self._expand_records_from_enrichment(
                    base_record=base_record,
                    source_row=source_row,
                    enrichment=expand_enrichment
                )
                all_records.extend(expanded)
        else:
            # Single record
            all_records = [base_record]
        
        # Step 5: Apply other enrichments to all records
        # Enrichments use the same config pattern as mappings, so we can reuse
        # _apply_field_mapping_to_record which now handles both patterns
        for record in all_records:
            for enrichment in other_enrichments:
                self._apply_field_mapping_to_record(
                    record=record,
                    field_config=enrichment,
                    source_row=source_row,
                    default_source_field=checkbox_field
                )
        
        return all_records
    
    def _populate_code_term_fields(
        self,
        record: Dict[str, Any],
        config: Dict[str, Any]
    ) -> None:
        """
        Populate code/term/source_text fields from config root keys.
        
        Uses configurable prefix from entity.code_term_prefix (defaults to entity name):
        - config['code'] → {prefix}_code
        - config['term'] → {prefix}_term
        - config['source_label'] → {prefix}_source_text
        
        This allows entities like Medication to use 'drug_code' instead of 'medication_code'.
        
        Args:
            record: Record dictionary to update
            config: Configuration with code/term/source_label
        """
        prefix = self.config.code_term_prefix if self.config.code_term_prefix else self.config.entity_name.lower()
        
        # Map config keys to target field names
        field_mappings = {
            'code': f'{prefix}_code',
            'term': f'{prefix}_term',
            'source_label': f'{prefix}_source_text'
        }
        
        for config_key, target_field in field_mappings.items():
            if config_key in config and target_field in record:
                record[target_field] = config[config_key]
    
    def _expand_records_from_enrichment(
        self,
        base_record: Dict[str, Any],
        source_row: pd.Series,
        enrichment: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Expand base record into N records based on checkbox enrichment.
        
        For each checked checkbox in value_mappings:
        - Create a copy of base_record
        - Update target_field with mapped value
        
        Args:
            base_record: Base record to expand
            source_row: Source data row
            enrichment: Enrichment configuration with value_mappings
            
        Returns:
            List of expanded records
        """
        
        target_field = enrichment.get('target_field')
        value_mappings = enrichment.get('value_mappings', {})
        
        expanded_records = []
        
        # Check each checkbox field in value_mappings
        for checkbox_field, mapped_value in value_mappings.items():
            checkbox_value = source_row.get(checkbox_field)
            
            # Skip if not checked
            if pd.isna(checkbox_value) or checkbox_value != 1:
                continue
            
            # Create new record
            new_record = base_record.copy()
            
            # Update target_field(s) with mapped value
            _map_field_value(new_record, target_field, 1, {1: mapped_value})
            
            # Note: Do NOT regenerate IDs here!
            # The base_record already has the correct ID from the mappings block.
            # All expanded records should share the same parent ID (e.g., submitter_treatment_id)
            # but have different code/term values.
            
            expanded_records.append(new_record)
        
        # If no records created, return base record
        return expanded_records if expanded_records else [base_record]
    
    def _empty_dataframe(self) -> pd.DataFrame:
        """
        Create empty DataFrame with target schema columns from entity.fields.
        
        Returns:
            Empty DataFrame with correct column structure
        """
        return pd.DataFrame(columns=self.config.entity_fields)
    
    def _log_summary(self):
        """Log mapping summary statistics."""
        log_mapping_summary(
            self.logger,
            total_records=self.stats['output_records'],
            entity_name=self.config.entity_name,
            additional_stats={
                'Input records': self.stats['input_records'],
                'After filtering': self.stats['filtered_records'],
                'Validation errors': self.stats['validation_errors'],
                'Warnings': self.stats['warnings']
            }
        )


class StudyDataMapper:
    """
    Generic study data mapper that auto-discovers and processes entities.
    
    This is the main orchestrator class for processing entire studies.
    It handles:
    - Study directory auto-detection
    - Entity auto-discovery from YAML configs
    - Dynamic loading of study-specific or default mappers
    - Batch processing of all entities
    - Results collection and reporting
    
    The mapper automatically:
    - Uses default EntityMapper if no custom code
    - Detects and uses CUSTOM_FUNCTIONS if available
    - Uses study-specific create_mapper() if defined
    """
    
    def __init__(self, study_id: str, config_dir: Optional[Path] = None, 
                 study_root: Optional[Path] = None):
        """
        Initialize study data mapper.
        
        Args:
            study_id: Study identifier (e.g., 'HostSeq', 'BQC19')
            config_dir: Optional path to YAML config directory
                       (default: auto-detected from <study_root>/<study_id>/config/)
            study_root: Optional root directory for studies
                       (default: ./studies/ relative to this file)
        """
        self.study_id = study_id
        self.logger = logging.getLogger(f"{__name__}.StudyDataMapper")
        
        # Auto-detect study root and directory
        if study_root is None:
            # Default: studies/ directory relative to this module
            study_root = Path(__file__).parent.parent.parent / 'studies'
        
        self.study_dir = study_root / study_id
        if not self.study_dir.exists():
            raise FileNotFoundError(
                f"Study directory not found: {self.study_dir}\n"
                f"Expected structure: {study_root}/{study_id}/"
            )
        
        # Auto-detect or use provided config directory
        self.config_dir = config_dir or (self.study_dir / 'config')
        if not self.config_dir.exists():
            raise FileNotFoundError(
                f"Config directory not found: {self.config_dir}\n"
                f"Expected YAML files in: {study_root}/{study_id}/config/"
            )
        
        # Load mapper factory (study-specific or default)
        self.custom_functions = {}
        self.custom_create_mapper = self._load_custom_mapper()
        
        self.mappers = {}
        self.results = {}
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_input_records': 0,
            'entities_processed': 0,
            'total_output_records': 0,
            'validation_errors': 0
        }
        
        self.logger.info(f"Initializing {study_id} Data Mapper")
        self.logger.info(f"Study directory: {self.study_dir}")
        self.logger.info(f"Config directory: {self.config_dir}")
        
        # Discover entities from YAML config files
        self.entities = self._discover_entities()
        self.logger.info(f"Discovered {len(self.entities)} entities")
        self.logger.info(f"Entities: {', '.join(self.entities)}")
        
        # Create entity mappers
        self._initialize_mappers()
    
    def _load_custom_mapper(self) -> Optional[Callable]:
        """
        Load custom mapper function if available.
        
        Checks for study-specific create_mapper function and CUSTOM_FUNCTIONS.
        Sets self.custom_functions and returns custom create_mapper if found.
        
        Returns:
            Custom create_mapper function if available, otherwise None
        """
        module_path = f'studies.{self.study_id}.mappers'
        
        try:
            # Try to import study-specific mapper module
            self.logger.info(f"Attempting to load: {module_path}")
            mapper_module = importlib.import_module(module_path)
            
            # Check for study-specific create_mapper function
            if hasattr(mapper_module, 'create_mapper'):
                self.logger.info(f"✓ Using study-specific create_mapper from {module_path}")
                return mapper_module.create_mapper
            
            # Check for custom functions (without create_mapper)
            if hasattr(mapper_module, 'CUSTOM_FUNCTIONS'):
                self.custom_functions = mapper_module.CUSTOM_FUNCTIONS
                self.logger.info(
                    f"✓ Using default mapper with {len(self.custom_functions)} "
                    f"custom functions from {module_path}"
                )
            else:
                self.logger.info(f"Module {module_path} found but no create_mapper or CUSTOM_FUNCTIONS")
                self.logger.info("✓ Using default mapper (no custom functions)")
                
        except ImportError:
            # Module doesn't exist - use pure default
            self.logger.info(f"No custom module found for {self.study_id}")
            self.logger.info("✓ Using default mapper (no custom functions)")
        
        return None
    
    def create_mapper(self, entity_name: str, study_id: str) -> EntityMapper:
        """
        Create entity mapper (uses custom or default implementation).
        
        Args:
            entity_name: Name of entity to create mapper for
            study_id: Study identifier
            
        Returns:
            Configured EntityMapper instance
        """
        # Use custom create_mapper if available
        if self.custom_create_mapper:
            return self.custom_create_mapper(entity_name, study_id)
        
        # Otherwise use default implementation
        config_file = self.config_dir / f'{entity_name}.yaml'
        
        if not config_file.exists():
            available = [f.stem for f in self.config_dir.glob('*.yaml')]
            raise FileNotFoundError(
                f"Config file not found: {config_file}\n"
                f"Available configs: {available}"
            )
        
        config = MappingConfig.from_yaml(config_file)
        return EntityMapper(config, study_id, custom_functions=self.custom_functions)
    
    def _discover_entities(self) -> List[str]:
        """
        Auto-discover entities from YAML config files.
        
        Returns:
            List of entity names (without .yaml extension)
        """
        yaml_files = sorted(self.config_dir.glob('*.yaml'))
        entities = [f.stem for f in yaml_files]
        
        if not entities:
            raise ValueError(f"No YAML config files found in {self.config_dir}")
        
        return entities
    
    def _initialize_mappers(self):
        """Initialize all entity mappers using factory function."""
        self.logger.info("Loading mapper configurations...")
        
        for entity_name in self.entities:
            self.mappers[entity_name] = self.create_mapper(entity_name, self.study_id)
        
        self.logger.info(f"Successfully loaded {len(self.mappers)} mappers")
    
    def load_source_data(self, input_path: Path) -> pd.DataFrame:
        """
        Load source data from file (supports CSV, TSV, TXT).
        
        Args:
            input_path: Path to input data file (.csv, .tsv, or .txt)
            
        Returns:
            Source DataFrame
        """
        self.logger.info(f"Loading source data from {input_path}")
        
        df = read_data_file(input_path)
        self.logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")
        self.stats['total_input_records'] = len(df)
        return df
    
    def set_input_directory(self, input_dir: Path):
        """
        Set the input directory for multi-file mode.
        
        Args:
            input_dir: Directory containing source CSV files
        """
        self.input_dir = Path(input_dir)
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory not found: {self.input_dir}")
        self.logger.info(f"Input directory set to: {self.input_dir}")
    
    def load_entity_source_data(self, entity_name: str) -> pd.DataFrame:
        """
        Load source data for a specific entity based on its configuration.
        
        Supports:
        - Single primary file
        - Multiple files with joins (left, right, inner, outer)
        - Column filtering to load only needed columns
        
        Args:
            entity_name: Name of entity to load data for
            
        Returns:
            Source DataFrame for this entity
        """
        mapper = self.mappers[entity_name]
        config = mapper.config
        
        # If no source_files config, look for auto-discovery
        if not config.source_files:
            # Try to auto-discover entity-specific file
            # Try auto-discovery with common file extensions
            for ext in ['.csv', '.tsv', '.txt']:
                auto_file = self.input_dir / f"{entity_name.lower()}{ext}"
                if auto_file.exists():
                    self.logger.info(f"Auto-discovered source file: {auto_file}")
                    df = read_data_file(auto_file)
                    self.logger.info(f"Loaded {len(df)} records from {auto_file.name}")
                    return df
            
            # If no file found, raise error
            raise FileNotFoundError(
                f"No source_files configured for entity '{entity_name}' and "
                f"auto-discovery failed. Expected: {entity_name.lower()}.csv/.tsv/.txt\n"
                f"Please specify source_file or source_files in the entity YAML config."
            )
        
        # Load primary file
        primary_file = config.source_files['primary']
        primary_path = self.input_dir / primary_file
        
        if not primary_path.exists():
            raise FileNotFoundError(
                f"Primary source file not found for entity '{entity_name}': {primary_path}"
            )
        
        self.logger.info(f"Loading primary file for {entity_name}: {primary_file}")
        primary_df = read_data_file(primary_path)
        self.logger.info(f"  Loaded {len(primary_df)} records with {len(primary_df.columns)} columns")
        
        # Load and join secondary files if specified
        secondary_files = config.source_files.get('secondary', [])
        
        if not secondary_files:
            return primary_df
        
        result_df = primary_df
        
        for sec_config in secondary_files:
            sec_file = sec_config['file']
            join_on = sec_config['join_on']
            
            # Handle file union: if sec_file is a list, concatenate all files
            if isinstance(sec_file, list):
                self.logger.info(f"Loading and concatenating {len(sec_file)} secondary files")
                
                sec_dfs = []
                for file_name in sec_file:
                    file_path = self.input_dir / file_name
                    
                    if not file_path.exists():
                        raise FileNotFoundError(
                            f"Secondary source file not found for entity '{entity_name}': {file_path}"
                        )
                    
                    self.logger.info(f"  Loading: {file_name}")
                    df = read_data_file(file_path)
                    self.logger.info(f"    {len(df)} records, {len(df.columns)} columns")
                    sec_dfs.append(df)
                
                # Concatenate with union of columns (fill missing with NaN)
                sec_df = pd.concat(sec_dfs, ignore_index=True, sort=False)
                all_columns = sec_df.columns.tolist()
                self.logger.info(f"  Concatenated result: {len(sec_df)} records, {len(all_columns)} columns (union)")
                
            else:
                # Single file (existing behavior)
                sec_path = self.input_dir / sec_file
                
                if not sec_path.exists():
                    raise FileNotFoundError(
                        f"Secondary source file not found for entity '{entity_name}': {sec_path}"
                    )
                
                self.logger.info(f"Loading secondary file: {sec_file}")
                sec_df = read_data_file(sec_path)
                self.logger.info(f"  Loaded {len(sec_df)} records with {len(sec_df.columns)} columns")
            
            # Filter columns if specified
            columns = sec_config.get('columns')
            
            if columns:
                # Ensure join key(s) are included
                join_keys = [join_on] if isinstance(join_on, str) else join_on
                cols_to_load = list(set(columns + join_keys))
                sec_df = sec_df[cols_to_load]
                self.logger.info(f"  Filtered to {len(cols_to_load)} columns")
            
            # Perform join
            join_type = sec_config['join_type']
            
            self.logger.info(f"  Joining on '{join_on}' using '{join_type}' join")
            
            result_df = result_df.merge(
                sec_df,
                on=join_on,
                how=join_type,
                suffixes=('', '_secondary')
            )
            
            self.logger.info(f"  Result after join: {len(result_df)} records")
        
        # TEMPORARY DEBUG: Print entity data after joining
        if entity_name.lower() == 'diagnosis':
            self.logger.info("=" * 80)
            self.logger.info("DEBUG: DIAGNOSIS ENTITY - DATA AFTER JOINING")
            self.logger.info("=" * 80)
            self.logger.info(f"Shape: {result_df.shape}")
            self.logger.info(f"Columns: {list(result_df.columns)}")
            self.logger.info("\nFirst 10 rows:")
            self.logger.info("\n" + result_df.head(10).to_string())
            self.logger.info("\nData types:")
            self.logger.info("\n" + str(result_df.dtypes))
            self.logger.info("=" * 80)
        
        return result_df
    
    def process_entity(self, entity_name: str, source_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a single entity.
        
        Args:
            entity_name: Name of entity to process
            source_df: Source DataFrame
            
        Returns:
            Mapped DataFrame for this entity
        """
        self.logger.info("=" * 80)
        self.logger.info(f"Processing {entity_name.capitalize()} Entity")
        self.logger.info("=" * 80)
        
        mapper = self.mappers[entity_name]
        result_df = mapper.map(source_df)
        
        # Collect statistics
        self.stats['entities_processed'] += 1
        self.stats['total_output_records'] += len(result_df)
        self.stats['validation_errors'] += mapper.stats.get('validation_errors', 0)
        
        self.logger.info(f"{entity_name.capitalize()}: {len(result_df)} records created")
        return result_df
    
    def process_all_entities(self, source_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process all entities in sequence (single-file mode).
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Dictionary mapping entity names to result DataFrames
        """
        self.stats['start_time'] = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info(f"{self.study_id.upper()} - PROCESSING ALL ENTITIES (Single-File Mode)")
        self.logger.info("=" * 80)
        
        results = {}
        
        for entity_name in self.entities:
            try:
                results[entity_name] = self.process_entity(entity_name, source_df)
            except Exception as e:
                self.logger.error(f"Error processing {entity_name}: {e}", exc_info=True)
                results[entity_name] = pd.DataFrame()
        
        self.stats['end_time'] = datetime.now()
        self.results = results
        
        return results
    
    def process_all_entities_multifile(self) -> Dict[str, pd.DataFrame]:
        """
        Process all entities in sequence (multi-file mode).
        
        Each entity loads its own source data based on configuration.
        
        Returns:
            Dictionary mapping entity names to result DataFrames
        """
        self.stats['start_time'] = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info(f"{self.study_id.upper()} - PROCESSING ALL ENTITIES (Multi-File Mode)")
        self.logger.info("=" * 80)
        
        results = {}
        
        for entity_name in self.entities:
            try:
                # Load entity-specific source data
                entity_source_df = self.load_entity_source_data(entity_name)
                
                # Update total input records (sum across all entities)
                self.stats['total_input_records'] += len(entity_source_df)
                
                # Process entity
                results[entity_name] = self.process_entity(entity_name, entity_source_df)
                
            except Exception as e:
                self.logger.error(f"Error processing {entity_name}: {e}", exc_info=True)
                results[entity_name] = pd.DataFrame()
        
        self.stats['end_time'] = datetime.now()
        self.results = results
        
        return results
    
    def save_results(self, output_dir: Path):
        """
        Save all entity results to CSV files.
        
        Args:
            output_dir: Directory to save output files
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("=" * 80)
        self.logger.info("SAVING RESULTS")
        self.logger.info("=" * 80)
        
        for entity_name, df in self.results.items():
            if not df.empty:
                output_path = output_dir / f"{entity_name.lower()}.csv"
                df.to_csv(output_path, index=False)
                self.logger.info(f"Saved {entity_name}: {output_path} ({len(df)} records)")
            else:
                self.logger.warning(f"Skipped {entity_name}: No records to save")
    
    def generate_summary_report(self) -> str:
        """Generate mapping summary report."""
        duration = None
        if self.stats['start_time'] and self.stats['end_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
        
        report = []
        report.append("=" * 80)
        report.append(f"{self.study_id.upper()} DATA MAPPER - SUMMARY REPORT")
        report.append("=" * 80)
        report.append(f"Study ID: {self.study_id}")
        report.append(f"Study Directory: {self.study_dir}")
        report.append(f"Config Directory: {self.config_dir}")
        report.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if duration:
            report.append(f"Processing Time: {duration.total_seconds():.2f} seconds")
        
        report.append("")
        report.append("MAPPER DESIGN:")
        report.append(f"  Study-Agnostic: Yes")
        
        # Check if using custom or default mapper
        if self.custom_create_mapper:
            report.append(f"  Implementation: Study-specific")
        else:
            report.append(f"  Implementation: Default (generic)")
        
        report.append(f"  Entities: {len(self.entities)}")
        report.append(f"  Config Files: {len(self.entities)} YAML files")
        
        report.append("")
        report.append("INPUT DATA:")
        report.append(f"  Total records: {self.stats['total_input_records']}")
        
        report.append("")
        report.append("PROCESSING SUMMARY:")
        report.append(f"  Entities processed: {self.stats['entities_processed']}/{len(self.entities)}")
        
        report.append("")
        report.append("OUTPUT DATA:")
        for entity_name, df in self.results.items():
            report.append(f"  {entity_name.capitalize()}: {len(df)} records")
        report.append(f"  Total output records: {self.stats['total_output_records']}")
        
        report.append("")
        report.append("QUALITY METRICS:")
        report.append(f"  Validation errors: {self.stats['validation_errors']}")
        
        if self.stats['total_input_records'] > 0:
            participant_df = self.results.get('participant', pd.DataFrame())
            if not participant_df.empty:
                retention_rate = (
                    participant_df.shape[0] / 
                    self.stats['total_input_records'] * 100
                )
                report.append(f"  Participant retention rate: {retention_rate:.1f}%")
        
        report.append("")
        report.append("ENTITY DETAILS:")
        for entity_name, df in self.results.items():
            if not df.empty:
                report.append(f"  {entity_name.capitalize()}:")
                report.append(f"    Records: {len(df)}")
                report.append(f"    Columns: {len(df.columns)}")
                
                mapper = self.mappers.get(entity_name)
                if mapper and hasattr(mapper, 'stats'):
                    report.append(f"    Input→Output: {mapper.stats['input_records']}→{mapper.stats['output_records']}")
                    report.append(f"    Errors: {mapper.stats['validation_errors']}")
        
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_summary_report(self, output_dir: Path):
        """Save summary report to file."""
        report = self.generate_summary_report()
        
        # Print to console
        print("\n" + report)
        
        # Save to file
        output_dir = Path(output_dir)
        report_path = output_dir / "mapping_summary.txt"
        with open(report_path, 'w') as f:
            f.write(report)
        
        self.logger.info(f"Summary report saved: {report_path}")

