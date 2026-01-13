"""
{StudyName} Study Mappers Package

CUSTOMIZATION INSTRUCTIONS:
1. Replace all instances of {StudyName} with your study name (e.g., HostSeq, BQC19)
2. Replace {STUDYNAME} with uppercase version (e.g., HOSTSEQ, BQC19)
3. Update custom function imports to match your actual functions
4. Choose your implementation level:
   - Level 2 (Custom Functions): Keep this file + transforms.py, delete base.py
   - Level 3 (Custom Mapper): Keep all files

This module provides:
- Factory function to create entity mappers
- Custom function registry for YAML-referenced functions
- Optional custom mapper class with study-specific logic
"""

from pathlib import Path
from typing import Optional

# Import core framework classes
from core.mappers import MappingConfig, EntityMapper

# LEVEL 3 ONLY: Import custom mapper class
# Comment out if using Level 2 (custom functions only)
from studies.{StudyName}.mappers.base import {StudyName}BaseMapper

# Import custom transformation functions
# CUSTOMIZE: Import your actual custom functions
from studies.{StudyName}.mappers.transforms import (
    example_custom_expansion_function,
    example_custom_age_function,
    # Add more custom function imports here
)

# Base path for study configurations
CONFIG_DIR = Path(__file__).parent.parent / 'config'

# Custom function registry
# These functions can be referenced in YAML configs using their key names
{STUDYNAME}_CUSTOM_FUNCTIONS = {
    # Expansion functions (called when entity.function matches key for pattern: custom)
    'example_custom_expansion_function': example_custom_expansion_function,
    
    # Age calculation functions (called when age_params.custom_function matches key)
    'example_custom_age_function': example_custom_age_function,
    
    # CUSTOMIZE: Add your custom functions here
    # 'construct_birth_date': construct_birth_date_from_year_month,
    # 'expand_relationships': expand_relationship_members,
}


def create_mapper(entity_name: str, study_id: str = '{StudyName}') -> EntityMapper:
    """
    Factory function to create entity mappers for {StudyName} study.
    
    This function is called by StudyDataMapper to create mappers for each entity.
    It loads the YAML configuration and creates either:
    - Level 2: EntityMapper with custom functions
    - Level 3: {StudyName}BaseMapper (custom class) with custom functions
    
    Args:
        entity_name: Name of the entity to map (e.g., 'participant', 'comorbidity')
                    Must match YAML filename: {entity_name}.yaml
        study_id: Study identifier (default: '{StudyName}')
        
    Returns:
        Configured mapper instance ready to process data
        
    Raises:
        FileNotFoundError: If YAML config file doesn't exist
    
    Example:
        >>> mapper = create_mapper('participant', '{StudyName}')
        >>> mapped_df = mapper.map(source_df)
    """
    # Build path to YAML configuration file
    config_file = CONFIG_DIR / f'{entity_name}.yaml'
    
    # Validate config file exists
    if not config_file.exists():
        available_configs = [f.stem for f in CONFIG_DIR.glob('*.yaml')]
        raise FileNotFoundError(
            f"Configuration file not found: {config_file}\n"
            f"Available entity configs: {', '.join(available_configs)}\n"
            f"Make sure you created {entity_name}.yaml in studies/{StudyName}/config/"
        )
    
    # Load YAML configuration
    config = MappingConfig.from_yaml(config_file)
    
    # CHOOSE YOUR IMPLEMENTATION LEVEL:
    
    # Level 3: Use custom mapper class with overridden methods
    # Uncomment this if you have custom mapper class in base.py
    return {StudyName}BaseMapper(
        config,
        study_id,
        custom_functions={STUDYNAME}_CUSTOM_FUNCTIONS
    )
    
    # Level 2: Use default EntityMapper with custom functions only
    # Comment out the Level 3 return above and uncomment this:
    # return EntityMapper(
    #     config,
    #     study_id,
    #     custom_functions={STUDYNAME}_CUSTOM_FUNCTIONS
    # )


# Public API
__all__ = [
    'create_mapper',  # Primary factory function used by StudyDataMapper
    '{STUDYNAME}_CUSTOM_FUNCTIONS',  # Export for testing/debugging
]
