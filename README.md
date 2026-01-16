# PCGL Data Mapper

YAML-driven framework for mapping research data to PCGL schema. Most studies need only YAML configs, no Python code changes required.

## Installation

```bash
# Clone the repository
git clone git@github.com:Pan-Canadian-Genome-Library/pcgl-data-mapper.git
cd pcgl-data-mapper

# Install dependencies
pip install -r requirements.txt
```

## Input/Output

**Input:**
- **Source CSV file** - Your study's raw data (e.g., REDCap export, database dump)
  - Typically located in `data/source/` directory
  - Can be any CSV with column headers
  - Example: `data/source/EXAMPLE.csv`

**Output:**
- **Mapped CSV files** - One file per entity, written to specified output directory
  - Default location: `data/mapped/{StudyName}/`
  - Files: `participant.csv`, `diagnosis.csv`, `comorbidity.csv`, etc.
  - Each file contains data in PCGL schema format
- **Mapping summary** - `data/mapped/{StudyName}/mapping_summary.txt` with processing statistics and validation results
- **Log file** - `prototype_mapper.log` in the root directory (overwrites each run)

## Quick Start

```bash
# Try the EXAMPLE study first
python prototype_mapper.py --study_id EXAMPLE --input_csv data/source/EXAMPLE.csv --output_dir data/mapped/EXAMPLE/

# Create your own study from template
cp -r studies/_TEMPLATE studies/MyStudy

# Edit YAML configs in studies/MyStudy/config/

# Run your data mapper
python prototype_mapper.py --study_id MyStudy --input_csv data/source/your_data.csv --output_dir data/mapped/MyStudy/
```

## Project Structure

```
data_mapper/
├── studies/
│   ├── EXAMPLE/           # Simple working example for testing
│   ├── _TEMPLATE/         # Starting point for new studies
│   ├── HostSeq/           # Production example (15 entities)
│   └── YourStudy/
│       ├── config/        # YAML entity configs (required)
│       │   ├── participant.yaml
│       │   ├── diagnosis.yaml
│       │   └── ...
│       └── mappers/       # Custom Python code (optional)
│           ├── __init__.py
│           └── transforms.py
├── core/                  # Framework code (generic, reusable)
├── data/
│   ├── source/            # Input CSV files
│   │   └── EXAMPLE.csv    # Example input data
│   └── mapped/            # Output directory
│       └── EXAMPLE/       # Example output
└── prototype_mapper.py    # Main script
```

## Mapping Pipeline

The mapper runs 5 steps for each entity:

1. **Preprocess** - Clean data, filter rows (YAML: `preprocessing`, `filters`)
2. **Map** - Transform fields (YAML: `mappings`, `configs`, pattern: `direct`/`expansion`/`custom`)  
3. **Post-process** - Type conversion, cleanup (YAML: `post_processing`)
4. **Validate** - Quality checks (YAML: `validations`)
5. **Output** - Export CSV, log stats

See [core/README.md](core/README.md) for pipeline flowchart and details.

## YAML Configurations

Each entity has a YAML file with up to 7 sections. Most studies only need `entity` and `mappings`.

**Minimal example:**
```yaml
entity:
  name: Participant
  pattern: direct  # Options: 'direct', 'expansion', 'custom'
  fields:
    base: [submitter_participant_id, study_id]

mappings:
  # Direct copy
  - source_field: id
    target_field: submitter_participant_id
  
  # Constant value
  - target_field: study_id
    default_value: "MyStudy"
  
  # Value mapping
  - source_field: vital_status_code
    target_field: vital_status
    source_type: radio
    value_mappings:
      0: "Alive"
      1: "Deceased"
  
  # Age calculation
  - target_field: age_at_enrollment
    target_type: age
    params:
      birth_date_field: dob
      event_date_field: enrollment_date
```

**Complete config sections:**
- `entity` - Name, schema, fields, pattern (required)
- `preprocessing` - Data cleaning (optional)
- `filters` - Row filtering (optional)
- `mappings` - Field transforms (required)
- `configs` - Expansion configs for checkboxes (expansion pattern only)
- `post_processing` - Type conversion, cleanup (optional)
- `validations` - Quality checks (optional)

**Mapping patterns:**
```yaml
# Direct (1:1)
entity:
  pattern: direct

# Expansion (1:N checkboxes)
entity:
  pattern: expansion
  params:
    selection_type: radio
    skip_values: [0, -1]

# Custom (1:N user function)
entity:
  pattern: custom
  function: my_transform_function
  params:
    # any params your function needs
```

See [studies/README.md](studies/README.md) for complete YAML documentation and all supported options.

## Customizations (Optional)

Most studies work with YAML only. Only add Python code for complex logic:

**Custom functions:**
```python
# studies/MyStudy/mappers/__init__.py
CUSTOM_FUNCTIONS = {
    'construct_birth_date': my_function
}
```

Reference in YAML:
```yaml
- target_field: age_at_enrollment
  target_type: age
  params:
    birth_date_transform: construct_birth_date
```

**Custom mappers:**

For complex logic that can't be expressed in YAML, create a custom mapper class in `studies/YourStudy/mappers/base.py`:

```python
# studies/YourStudy/mappers/base.py
from core.mappers.base import EntityMapper

class StudyEntityMapper(EntityMapper):
    def _filter_eligible_participants(self, df):
        """Custom eligibility logic with complex conditions."""
        # Example: Multi-field logic with OR conditions
        consent_mask = (df['consent'] == 0) | (df['consent'].isna())
        age_mask = df['age_years'] < 18
        return df[~(consent_mask | age_mask)].copy()
    
    def preprocess(self, source_df, **kwargs):
        """Override for custom preprocessing."""
        df = super().preprocess(source_df, **kwargs)
        # Add custom preprocessing steps here
        return df
```

Then reference it in your mapper module:
```python
# studies/YourStudy/mappers/__init__.py
from .base import StudyEntityMapper

def create_mapper(config, study_id, custom_functions):
    return StudyEntityMapper(config, study_id, custom_functions)
```

**Common override methods:**
- `_filter_eligible_participants()` - Complex eligibility filtering
- `preprocess()` - Custom data cleaning before mapping
- `postprocess()` - Custom cleanup after mapping
- `_map_expansion_pattern()` - Custom wide-to-long transformations

See `studies/HostSeq/mappers/base.py` for production example.

## Troubleshooting

**Empty output**
- Check `source_field` names match CSV columns exactly (case-sensitive)
- Verify filters aren't excluding all rows
- Check required field validations

**Age calculation fails**
- Verify birth date and event date fields exist in source
- Check date formats are parseable
- Use `age_fallback_field` if that's applicable to your use case

**Records filtered out**
- Check `filters.eligible_participants` logic
- Verify `include_rows` settings for REDCap data
- Review `field_filters` conditions

Run with verbose logging: check `prototype_mapper.log` for details.

## Examples

**EXAMPLE study** - Simple working example for testing and learning
- 10 entity configs with straightforward mappings
- Uses `data/source/EXAMPLE.csv` as input
- Direct mapping entities: participant, demographic, specimen, sample, diagnosis, sociodemographic
- Expansion mapping entities: comorbidity, medication, treatment, measurement
- See `studies/EXAMPLE/config/*.yaml` for basic configuration patterns
- Run with: `python prototype_mapper.py --study_id EXAMPLE --input_csv data/source/EXAMPLE.csv --output_dir data/mapped/EXAMPLE/`

**HostSeq study** - Complete production example with 15 entities (13 base + 2 extension)
- Direct mapping entities: participant, demographic, specimen, sample, diagnosis, sociodemographic, demographic, hla
- Expansion mapping entities: comorbidity, medication, treatment, phenotype, procedure, measurement
- Custom expansion mapping entities: household
- Custom code: eligibility filtering, birth date construction, household expansion
- See `studies/HostSeq/config/*.yaml` for advanced working configs

**_TEMPLATE** - Starting point for new studies with pre-configured YAML files and inline documentation.

## Documentation

- **[studies/README.md](studies/README.md)** - YAML configuration guide
  - All 7 config sections with examples
  - Supported preprocessing/filter/validation types
  - Pattern configuration (direct, expansion, custom)
  - Study-specific customization

- **[core/README.md](core/README.md)** - Framework internals
  - Mapping pipeline flowchart
  - Key classes (EntityMapper, StudyDataMapper, MappingConfig)
  - Record transforms and utilities
  - Usage examples
