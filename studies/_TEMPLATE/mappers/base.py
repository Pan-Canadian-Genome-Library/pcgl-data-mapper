"""
{StudyName} Base Mapper - Custom Mapper Class

CUSTOMIZATION INSTRUCTIONS:
1. Replace {StudyName} with your study name
2. Implement study-specific overrides below
3. Delete this file if you only need custom functions (Level 2)
4. Keep this file for Level 3 (custom mapper with overridden methods)

This custom mapper extends EntityMapper to provide study-specific logic:
- Complex eligibility filtering (beyond simple YAML filters)
- Custom preprocessing steps
- Study-specific validation rules
- Custom post-processing logic

Use this when YAML configuration + custom functions aren't enough.
"""

import pandas as pd
import logging
from typing import List, Dict, Any
from core.mappers import EntityMapper


class {StudyName}BaseMapper(EntityMapper):
    """
    Base mapper for {StudyName} study entities.
    
    Extends EntityMapper with study-specific implementations:
    - _filter_eligible_participants(): Complex eligibility logic
    - preprocess(): Custom preprocessing steps
    - postprocess(): Custom post-processing steps
    - validate_mapped_data(): Additional validation rules
    
    All entity mappers for {StudyName} use this class to ensure
    consistent study-specific behavior.
    """
    
    def _filter_eligible_participants(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter for eligible participants using {StudyName}-specific criteria.
        
        OVERRIDE THIS METHOD if your study has complex eligibility logic
        that can't be expressed in YAML filters.
        
        Use cases:
        - OR conditions between multiple fields
        - Nested AND/OR logic
        - Calculated eligibility criteria
        - Multi-step exclusion logic
        
        DELETE THIS METHOD if you're using YAML filters instead
        (filters.participant_eligibility in YAML).
        
        Args:
            df: DataFrame to filter (usually baseline records for REDCap)
            
        Returns:
            Filtered DataFrame with only eligible participants
        """
        initial_count = len(df)
        
        # CUSTOMIZE: Implement your eligibility logic
        # Example: Complex eligibility with OR conditions
        
        # Inclusion criteria (at least one must be true)
        inclusion_mask = (
            # Option 1: Full consent
            (df['consent_signed'] == 1) & (df['consent_date'].notna())
        ) | (
            # Option 2: Parent/guardian consent for minors
            (df['age_years'] < 18) & (df['guardian_consent'] == 1)
        ) | (
            # Option 3: Emergency enrollment
            (df['emergency_enrollment'] == 1)
        )
        
        # Exclusion criteria (all must be false)
        exclusion_mask = (
            # Explicit withdrawal
            (df['withdrawn'] == 1) |
            # Data quality issues
            (df['data_quality_flag'] == 'Poor') |
            # Study-specific exclusions
            (df['exclude_from_analysis'] == 1) |
            # Missing critical data
            (df['critical_field'].isna())
        )
        
        # Apply filters
        eligible_df = df[inclusion_mask & ~exclusion_mask].copy()
        
        excluded_count = initial_count - len(eligible_df)
        
        self.logger.info(
            f"{self.config.entity_name} eligibility filtering: "
            f"{initial_count} → {len(eligible_df)} participants "
            f"({excluded_count} excluded, {100 * len(eligible_df) / initial_count:.1f}% retained)"
        )
        
        return eligible_df
    
    def preprocess(self, source_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Preprocess source data with {StudyName}-specific steps.
        
        OVERRIDE THIS METHOD to add study-specific preprocessing
        before field mapping.
        
        IMPORTANT: Always call parent preprocess() first to apply
        standard YAML-configured preprocessing.
        
        Use cases:
        - Study-specific date format conversions
        - Custom field calculations
        - Data quality checks
        - Study-specific field transformations
        
        DELETE THIS METHOD if standard preprocessing is sufficient.
        
        Args:
            source_df: Source DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Preprocessed DataFrame
        """
        # IMPORTANT: Call parent preprocess first
        df = super().preprocess(source_df, **kwargs)
        
        # CUSTOMIZE: Add study-specific preprocessing
        self.logger.info(f"Applying {StudyName}-specific preprocessing")
        
        # Example 1: Convert study-specific date formats
        if 'study_date_field' in df.columns:
            df['study_date_field'] = pd.to_datetime(
                df['study_date_field'],
                format='%d-%b-%Y',  # e.g., "15-Jan-2023"
                errors='coerce'
            )
            self.logger.debug("Converted study-specific date format")
        
        # Example 2: Calculate derived fields
        if 'height_cm' in df.columns and 'weight_kg' in df.columns:
            # Calculate BMI if not present
            if 'bmi' not in df.columns or df['bmi'].isna().all():
                df['bmi'] = df['weight_kg'] / ((df['height_cm'] / 100) ** 2)
                self.logger.debug("Calculated BMI from height and weight")
        
        # Example 3: Clean study-specific coding
        if 'coded_field' in df.columns:
            # Remove study-specific prefixes
            df['coded_field'] = df['coded_field'].str.replace(
                r'^{STUDYNAME}_',
                '',
                regex=True
            )
        
        return df
    
    def postprocess(self, mapped_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Post-process mapped data with {StudyName}-specific steps.
        
        OVERRIDE THIS METHOD to add study-specific post-processing
        after field mapping.
        
        IMPORTANT: Always call parent postprocess() first to apply
        standard YAML-configured post-processing.
        
        Use cases:
        - Study-specific field adjustments
        - Cross-field validations and corrections
        - Study-specific data model rules
        
        DELETE THIS METHOD if standard post-processing is sufficient.
        
        Args:
            mapped_df: Mapped DataFrame
            **kwargs: Additional arguments
            
        Returns:
            Final processed DataFrame
        """
        # IMPORTANT: Call parent postprocess first
        df = super().postprocess(mapped_df, **kwargs)
        
        # CUSTOMIZE: Add study-specific post-processing
        self.logger.info(f"Applying {StudyName}-specific post-processing")
        
        # Example 1: Study-specific data model rules
        # Clear age_at_event if event didn't occur
        if 'event_occurred' in df.columns and 'age_at_event' in df.columns:
            no_event_mask = df['event_occurred'] != 'Yes'
            cleared_count = (df.loc[no_event_mask, 'age_at_event'].notna()).sum()
            
            if cleared_count > 0:
                df.loc[no_event_mask, 'age_at_event'] = None
                self.logger.info(
                    f"Cleared age_at_event for {cleared_count} records where event didn't occur"
                )
        
        # Example 2: Ensure consistent field combinations
        # If field A is populated, field B must also be populated
        if 'field_a' in df.columns and 'field_b' in df.columns:
            inconsistent = df['field_a'].notna() & df['field_b'].isna()
            if inconsistent.any():
                self.logger.warning(
                    f"Found {inconsistent.sum()} records with field_a but no field_b"
                )
        
        return df
    
    def validate_mapped_data(self, mapped_df: pd.DataFrame) -> List[str]:
        """
        Validate mapped data with {StudyName}-specific rules.
        
        OVERRIDE THIS METHOD to add study-specific validation checks
        beyond standard YAML validations.
        
        IMPORTANT: Always call parent validate() first to apply
        standard validations.
        
        Use cases:
        - Cross-field validation
        - Study-specific business rules
        - Data consistency checks
        - Study-specific value ranges
        
        DELETE THIS METHOD if standard validations are sufficient.
        
        Args:
            mapped_df: Mapped DataFrame to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        # IMPORTANT: Get parent validation errors first
        errors = super().validate_mapped_data(mapped_df)
        
        # CUSTOMIZE: Add study-specific validations
        self.logger.debug(f"Applying {StudyName}-specific validations")
        
        # Example 1: Cross-field date validation
        if 'event_date' in mapped_df.columns and 'birth_date' in mapped_df.columns:
            invalid_dates = (
                (mapped_df['event_date'].notna()) &
                (mapped_df['birth_date'].notna()) &
                (pd.to_datetime(mapped_df['event_date']) < 
                 pd.to_datetime(mapped_df['birth_date']))
            )
            if invalid_dates.any():
                errors.append(
                    f"Found {invalid_dates.sum()} records where event_date < birth_date"
                )
        
        # Example 2: Study-specific value range check
        if 'study_score' in mapped_df.columns:
            invalid_scores = (
                (mapped_df['study_score'].notna()) &
                ((mapped_df['study_score'] < 0) | (mapped_df['study_score'] > 100))
            )
            if invalid_scores.any():
                errors.append(
                    f"Found {invalid_scores.sum()} records with study_score outside 0-100 range"
                )
        
        # Example 3: Required field combinations
        if 'diagnosis_code' in mapped_df.columns and 'diagnosis_date' in mapped_df.columns:
            missing_dates = (
                mapped_df['diagnosis_code'].notna() & 
                mapped_df['diagnosis_date'].isna()
            )
            if missing_dates.any():
                errors.append(
                    f"Found {missing_dates.sum()} records with diagnosis_code but no diagnosis_date"
                )
        
        return errors


# Export for use in __init__.py
__all__ = ['{StudyName}BaseMapper']
