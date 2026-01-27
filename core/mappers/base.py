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
    safe_int_conversion
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
        self.pattern_params = entity_config.get('params', {})  # For any pattern
        # Allow custom prefix for code/term fields (default to entity name)
        self.code_term_prefix = entity_config.get('code_term_prefix', self.entity_name.lower())
        
        # Parse source_files configuration (for multi-file support)
        self.source_files = self._parse_source_files(entity_config)
        
        # Mappings are a list of field configs
        self.mappings = config_dict.get('mappings', []) or []
        
        # Configs for checkbox expansion (comorbidity, phenotype, etc.)
        # Changed from dict to list format: configs is now a list with source_field property
        # Support range expansion for repetitive configs
        self.configs = self._expand_range_configs(config_dict.get('configs', []) or [])
        
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
    
    def _expand_range_configs(self, configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Expand range-based config templates into individual configs.
        
        Detects configs with range syntax and expands them:
        - type: range (explicit) OR presence of start/end/template keys
        - Replaces {n} placeholders in template with values from start to end
        - Supports {n:02d} for zero-padded formatting
        
        Args:
            configs: List of config dictionaries, may include range configs
            
        Returns:
            Expanded list with range configs replaced by individual configs
        """
        if not configs:
            return []
        
        expanded_configs = []
        
        for config in configs:
            # Check if this is a range config
            is_range = (
                config.get('type') == 'range' or 
                ('start' in config and 'end' in config and 'template' in config)
            )
            
            if is_range:
                # Extract range parameters
                start = config.get('start')
                end = config.get('end')
                template = config.get('template')
                
                if not all([start is not None, end is not None, template is not None]):
                    logger.warning(f"Incomplete range config, skipping: {config}")
                    continue
                
                # Expand range
                for n in range(start, end + 1):
                    # Create a deep copy of the template
                    expanded_config = self._substitute_placeholders(template, n)
                    expanded_configs.append(expanded_config)
            else:
                # Regular config, add as-is
                expanded_configs.append(config)
        
        return expanded_configs
    
    def _substitute_placeholders(self, obj: Any, n: int) -> Any:
        """
        Recursively substitute {n} placeholders in strings within a nested structure.
        
        Supports:
        - {n} - replaced with the number
        - {n:02d} - replaced with zero-padded number (e.g., 01, 02, ...)
        - {n:03d} - 3-digit padding, etc.
        
        Args:
            obj: Object to process (dict, list, str, or other)
            n: Value to substitute for {n}
            
        Returns:
            New object with placeholders substituted
        """
        if isinstance(obj, dict):
            # Recursively process dictionary
            return {k: self._substitute_placeholders(v, n) for k, v in obj.items()}
        elif isinstance(obj, list):
            # Recursively process list
            return [self._substitute_placeholders(item, n) for item in obj]
        elif isinstance(obj, str):
            # Substitute placeholders in string
            # Support both {n} and {n:format} patterns
            result = obj
            # Replace formatted placeholders first (e.g., {n:02d})
            import re
            formatted_pattern = r'\{n:(\d+)d\}'
            for match in re.finditer(formatted_pattern, result):
                width = int(match.group(1))
                formatted_value = str(n).zfill(width)
                result = result.replace(match.group(0), formatted_value)
            # Replace simple {n} placeholder
            result = result.replace('{n}', str(n))
            return result
        else:
            # Return as-is for other types
            return obj
    

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
        study_id: str
    ):
        """
        Initialize entity mapper.
        
        Args:
            config: Mapping configuration
            study_id: Study identifier (e.g., 'HostSeq', 'BQC19')
        """
        self.config = config
        self.study_id = study_id
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
    
    def _construct_date_from_components(
        self,
        row: pd.Series,
        df: pd.DataFrame,
        year_field: str,
        month_field: str,
        day_field: Optional[str],
        default_day: int
    ) -> Optional[str]:
        """
        Construct date string from year/month/day components.
        
        Uses safe_int_conversion for robust parsing (handles "2,019", spaces, etc.).
        
        Args:
            row: DataFrame row
            df: Full DataFrame (for column checks)
            year_field: Name of year field (required)
            month_field: Name of month field (required)
            day_field: Name of day field (optional)
            default_day: Default day if not provided
            
        Returns:
            Date string in YYYY-MM-DD format, or None if invalid
        """
        
        # Year is required
        year = row.get(year_field)
        if pd.isna(year):
            return None
        
        year_val = safe_int_conversion(year)
        if not year_val:
            return None
        
        # Month is required
        month = row.get(month_field)
        if pd.isna(month):
            return None
        
        month_val = safe_int_conversion(month)
        if not month_val:
            return None
        
        # Get day (use default if not provided or field missing)
        if day_field and day_field in df.columns:
            day = row.get(day_field)
            if pd.notna(day):
                day_val = safe_int_conversion(day)
            else:
                day_val = default_day
        else:
            day_val = default_day
        
        if not day_val:
            return None
        
        # Validate ranges
        if not (1 <= month_val <= 12):
            return None
        if not (1 <= day_val <= 31):
            return None
        
        # Format as YYYY-MM-DD
        return f"{year_val}-{month_val:02d}-{day_val:02d}"
    
    def preprocess(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Preprocess source data before mapping.
        
        Processing order (optimized for efficiency):
        1. Apply filters first (on raw data)
        2. Merge baseline fields (enrichment)
        3. Clean and generate fields (only on filtered data)
        
        Override this method to add entity-specific or study-specific preprocessing.
        
        Args:
            source_df: Source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Preprocessed DataFrame
        """
        df = source_df.copy()
        
        # ====================================================================
        # STEP 1: Apply filters first (on raw data)
        # ====================================================================
        # Apply new unified filter structure
        filters_config = self.config.filters
        participant_id_field = filters_config.get('participant_id_field', 'participant_id')
        
        # 1. Identify eligible participants
        # Start with all participants, then apply filter if configured
        if participant_id_field in df.columns:
            eligible_participant_ids = set(df[participant_id_field].unique())
            
            participant_filter = filters_config.get('participant', {}).get('filter') if filters_config.get('participant') else None
            if participant_filter:
                eligible_participant_ids = self._get_eligible_participants(df, participant_filter)
                
                # Check if all participants were excluded
                if not eligible_participant_ids:
                    self.logger.warning("No eligible participants after applying participant filter")
                    return pd.DataFrame(columns=df.columns)
        else:
            # Participant ID field not found - warn but continue without participant filtering
            if filters_config.get('participant') or filters_config.get('rows'):
                self.logger.warning(
                    f"Participant ID field '{participant_id_field}' not found in data. "
                    f"Participant/row filtering will be skipped."
                )
            eligible_participant_ids = None
        
        # 2. Apply row selection filtering (if configured)
        rows_config = filters_config.get('rows', [])
        if rows_config:
            df = self._apply_row_selectors(df, rows_config, eligible_participant_ids)
        elif eligible_participant_ids is not None:
            # No row selectors, but we have participant filtering applied - filter to eligible participants
            df = df[df[participant_id_field].isin(eligible_participant_ids)].copy()
            self.logger.info(f"Filtered to {len(eligible_participant_ids)} eligible participants")
        
        # ====================================================================
        # STEP 2: Merge baseline fields (enrichment)
        # ====================================================================
        enrich_config = filters_config.get('enrich') or {}
        merge_fields = enrich_config.get('merge_baseline_fields', [])
        if merge_fields:
            participant_id_field = filters_config.get('participant_id_field', 'participant_id')
            # Extract baseline rows for merging
            baseline_df = df[df.get('redcap_repeat_instrument', pd.Series([None]*len(df))).isna()].copy()
            if len(baseline_df) > 0:
                df = self._merge_baseline_fields(df, baseline_df, participant_id_field, merge_fields)
        
        # ====================================================================
        # STEP 3: Clean and generate fields (after filtering and enrichment)
        # This is more efficient - only process data that survived filtering
        # ====================================================================
        for step in self.config.preprocessing:
            step_type = step.get('type')
            fields = step.get('fields', [])
            
            if step_type == 'clean_numeric':
                # Remove commas, spaces from numeric fields
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
            
            elif step_type == 'construct_date':
                # Construct date from year/month/day components
                target = step.get('target')
                params = step.get('params', {})
                
                if not target:
                    continue
                
                year_field = params.get('year_field')
                month_field = params.get('month_field')
                day_field = params.get('day_field')
                default_day = params.get('default_day', 15)
                
                if not year_field or not month_field:
                    continue
                
                try:
                    df[target] = df.apply(
                        lambda row: self._construct_date_from_components(row, df, year_field, month_field, day_field, default_day),
                        axis=1
                    )
                    
                    fields_desc = [year_field, month_field]
                    if day_field:
                        fields_desc.append(day_field)
                    self.logger.info(f"Constructed date field '{target}' from {', '.join(fields_desc)}")
                    
                except Exception as e:
                    self.logger.error(f"Error constructing date field '{target}': {e}")
                    raise ValueError(f"Invalid construct_date configuration for '{target}': {e}") from e
            
            else:
                self.logger.warning(f"Unknown preprocessing type: {step_type}")
        
        return df
    
    def _get_eligible_participants(self, df: pd.DataFrame, participant_filter: List[Dict[str, Any]]) -> set:
        """
        Identify eligible participants based on filter criteria.
        
        Applies filters.participant.filter from YAML configuration to the dataset
        and returns a deduplicated set of participant IDs from matching rows.
        
        Note: For REDCap data, only rows with the filter fields (typically baseline)
        will match, naturally limiting evaluation to relevant rows.
        
        Args:
            df: DataFrame to evaluate
            participant_filter: List of filter configurations
            
        Returns:
            Set of eligible participant IDs
        """
        self.logger.info("Identifying eligible participants")
        
        participant_id_field = self.config.filters.get('participant_id_field', 'participant_id')
        
        if participant_id_field not in df.columns:
            self.logger.warning(f"Participant ID field '{participant_id_field}' not found in data")
            return set()
        
        initial_participants = df[participant_id_field].nunique()
        
        # Apply filter to all rows - rows without filter fields will be naturally excluded
        filtered_df, _, _ = self._apply_filters(df, participant_filter, "participant")
        eligible_participant_ids = set(filtered_df[participant_id_field].unique())
        final_participants = len(eligible_participant_ids)
        
        excluded_participants = initial_participants - final_participants
        if excluded_participants > 0:
            retention_rate = 100 * final_participants / initial_participants if initial_participants > 0 else 0
            self.logger.info(
                f"Participant eligibility: {initial_participants} → {final_participants} participants "
                f"(excluded {excluded_participants}, retention {retention_rate:.1f}%)"
            )
        
        return eligible_participant_ids
    
    def _apply_row_selectors(
        self, 
        df: pd.DataFrame, 
        rows_config: List[Dict[str, Any]], 
        eligible_participant_ids: Optional[set] = None
    ) -> pd.DataFrame:
        """
        Apply row selection filtering based on filters.rows configuration.
        
        Each row selector has a name and filter conditions.
        All matching rows from all selectors are included (union).
        
        If eligible_participant_ids is provided, only rows for those participants are included.
        
        Example configuration:
        ```yaml
        filters:
          rows:
            - name: baseline
              filter:
                - field: redcap_repeat_instrument
                  op: is_null
            - name: lab_visits
              filter:
                - field: redcap_repeat_instrument
                  op: equals
                  value: laboratory
        ```
        
        Args:
            df: DataFrame to filter
            rows_config: List of row selector configurations
            eligible_participant_ids: Optional set of eligible participant IDs to filter to
            
        Returns:
            DataFrame with selected rows
        """
        if not rows_config:
            return df
        
        self.logger.info(f"Applying {len(rows_config)} row selector(s)")
        
        all_selected_rows = []
        
        for row_selector in rows_config:
            selector_name = row_selector.get('name', 'unnamed')
            row_filter = row_selector.get('filter', [])
            
            if not row_filter:
                self.logger.warning(f"Row selector '{selector_name}' has no filter, skipping")
                continue
            
            # Apply filter for this selector
            selected_df, initial_count, final_count = self._apply_filters(df, row_filter, "row")
            
            if final_count > 0:
                all_selected_rows.append(selected_df)
                self.logger.info(f"Row selector '{selector_name}': selected {final_count} rows")
            else:
                self.logger.warning(f"Row selector '{selector_name}': no matching rows")
        
        if not all_selected_rows:
            self.logger.warning("No rows matched any selector")
            return pd.DataFrame(columns=df.columns)
        
        # Combine all selected rows (union)
        result_df = pd.concat(all_selected_rows, ignore_index=True).drop_duplicates()
        self.logger.info(f"Total rows after row selection: {len(result_df)} (from {len(df)} input rows)")
        
        # Filter to eligible participants if provided
        if eligible_participant_ids is not None:
            participant_id_field = self.config.filters.get('participant_id_field', 'participant_id')
            if participant_id_field in result_df.columns:
                before_count = len(result_df)
                result_df = result_df[result_df[participant_id_field].isin(eligible_participant_ids)].copy()
                after_count = len(result_df)
                
                if before_count != after_count:
                    self.logger.info(
                        f"Filtered to {len(eligible_participant_ids)} eligible participants: "
                        f"{before_count} → {after_count} rows"
                    )
        
        return result_df
    
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
    
    def _apply_filters(
        self,
        df: pd.DataFrame,
        filters: List[Dict[str, Any]],
        filter_type: str = "field"
    ) -> tuple:
        """
        Apply filtering conditions to DataFrame with support for any/all boolean logic.
        
        Filter Structure:
        - Simple conditions: [{"field": "consent", "op": "equals", "value": 1}]
        - AND logic (default): All conditions at same level must be true
        - OR logic: {"any": [condition1, condition2, ...]} - any condition can be true
        - AND logic (explicit): {"all": [condition1, condition2, ...]} - all conditions must be true
        - Nesting: any/all can be nested arbitrarily deep
        
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
            filter_type: Type of filter for logging ("field", "eligibility", or "participant")
            
        Returns:
            Tuple of (filtered_df, initial_count, final_count)
        """
        initial_count = len(df)
        
        for filter_config in filters:
            # Check for any/all boolean logic
            if 'any' in filter_config:
                # OR logic - collect indices matching any condition
                df = self._apply_any_filters(df, filter_config['any'], filter_type)
            elif 'all' in filter_config:
                # Explicit AND logic - recursively apply
                df, _, _ = self._apply_filters(df, filter_config['all'], filter_type)
            else:
                # Single condition
                df = self._apply_single_filter(df, filter_config, filter_type)
        
        # Single copy at the end
        return df.copy(), initial_count, len(df)
    
    def _apply_single_filter(
        self,
        df: pd.DataFrame,
        filter_config: Dict[str, Any],
        filter_type: str = "field"
    ) -> pd.DataFrame:
        """
        Apply a single filter condition to DataFrame.
        
        This method contains the actual operator logic and is called by both
        _apply_filters (AND logic) and _apply_any_filters (OR logic).
        
        Args:
            df: DataFrame to filter
            filter_config: Single filter configuration dict
            filter_type: Type of filter for logging
            
        Returns:
            Filtered DataFrame (not copied - caller handles that)
        """
        field = filter_config.get('field')
        operator = filter_config.get('operator') or filter_config.get('op')
        value = filter_config.get('value')
        
        if not field or not operator:
            self.logger.warning(f"Skipping incomplete {filter_type} filter: {filter_config}")
            return df
        
        if field not in df.columns:
            self.logger.warning(f"{filter_type.capitalize()} field '{field}' not found, skipping")
            return df
        
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
                return df
        else:
            self.logger.warning(f"Unknown operator '{operator}', skipping")
            return df
        
        after_count = len(df)
        excluded = before_count - after_count
        if excluded > 0:
            label_map = {"eligibility": "Eligibility filter", "participant": "Participant filter", "field": "Field filter"}
            label = label_map.get(filter_type, "Filter")
            self.logger.info(
                f"{label}: {field} {operator} {value} "
                f"({before_count} → {after_count}, excluded {excluded})"
            )
        
        return df
    
    def _apply_any_filters(
        self,
        df: pd.DataFrame,
        filters: List[Dict[str, Any]],
        filter_type: str = "field"
    ) -> pd.DataFrame:
        """
        Apply OR logic to a list of filter conditions.
        
        Returns rows that match ANY of the conditions (union of all matching rows).
        Supports nested any/all logic recursively.
        
        Args:
            df: DataFrame to filter
            filters: List of filter configurations (OR combined)
            filter_type: Type of filter for logging
            
        Returns:
            DataFrame with rows matching any condition (not copied - caller handles that)
        """
        if not filters:
            return df
        
        # Collect indices that match any condition
        matching_indices = set()
        
        for filter_config in filters:
            # Check for nested any/all
            if 'any' in filter_config:
                # Nested OR - recursively apply
                temp_df = self._apply_any_filters(df, filter_config['any'], filter_type)
                matching_indices.update(temp_df.index)
            elif 'all' in filter_config:
                # Nested AND - recursively apply
                temp_df, _, _ = self._apply_filters(df, filter_config['all'], filter_type)
                matching_indices.update(temp_df.index)
            else:
                # Single condition - apply and collect matching indices
                temp_df = self._apply_single_filter(df.copy(), filter_config, filter_type)
                matching_indices.update(temp_df.index)
        
        # Return rows matching any condition
        result_df = df.loc[list(matching_indices)]
        
        before_count = len(df)
        after_count = len(result_df)
        if before_count != after_count:
            self.logger.info(f"OR filter: {before_count} → {after_count} rows (matched {after_count})")
        
        return result_df
    
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
        
        Args:
            source_df: Preprocessed source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            DataFrame with mapped fields (direct, or expanded)
        """
        # Check entity pattern to determine mapping strategy
        if self.config.entity_pattern == 'expansion':
            # Use expansion pattern: wide-to-long conversion
            return self._map_expansion_pattern(source_df, **kwargs)
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
                apply_age_to_record(record, target_field, source_row, params)
            
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
    - Loading EntityMapper with YAML configuration
    - Batch processing of all entities
    - Results collection and reporting
    
    All mapping logic is driven by YAML configuration files.
    No custom code or study-specific classes are needed.
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
    

    def create_mapper(self, entity_name: str, study_id: str) -> EntityMapper:
        """
        Create entity mapper from YAML configuration.
        
        Args:
            entity_name: Name of entity to create mapper for
            study_id: Study identifier
            
        Returns:
            Configured EntityMapper instance
        """
        config_file = self.config_dir / f'{entity_name}.yaml'
        
        if not config_file.exists():
            available = [f.stem for f in self.config_dir.glob('*.yaml')]
            raise FileNotFoundError(
                f"Config file not found: {config_file}\n"
                f"Available configs: {available}"
            )
        
        config = MappingConfig.from_yaml(config_file)
        return EntityMapper(config, study_id)
    
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
        report.append(f"  Implementation: YAML-driven configuration")
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
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        self.logger.info(f"Summary report saved: {report_path}")

