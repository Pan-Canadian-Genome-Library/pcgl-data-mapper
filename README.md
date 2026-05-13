# PCGL Data Mapper

YAML-driven framework for mapping research data to PCGL data model. 

## Installation

```bash
# Clone the repository
git clone https://github.com/Pan-Canadian-Genome-Library/pcgl-data-mapper.git
cd pcgl-data-mapper

# Install dependencies
pip install -r requirements.txt
```

## Input/Output

**Input (Two Modes):**
- **Single-file mode** :
  - One data file with all data (CSV, TSV, or TXT format)
  - Example: `data/source/EXAMPLE.csv`
  
- **Multi-file mode** :
  - Directory with multiple data files (CSV, TSV, or TXT)
  - Split by domain: e.g, `demographics.csv`, `clinical.tsv`, `lab_results.txt`
  - Automatically joins files per entity configuration
  - Example: `data/source/MyStudy/`

**Supported File Formats:**
- `.csv` - Comma-separated values
- `.tsv` - Tab-separated values  
- `.txt` - Text files (delimiter auto-detected)

**Output:**
- **Mapped CSV files** - One file per entity, written to specified output directory
  - Default location: `data/mapped/{StudyName}/`
  - Files: `participant.csv`, `diagnosis.csv`, `comorbidity.csv`, etc.
  - Each file contains data in PCGL schema format
- **Mapping summary** - `data/mapped/{StudyName}/mapping_summary.txt` with processing statistics and validation results
- **Log file** - `prototype_mapper.log` in the root directory (overwrites each run)

## Quick Start

### 1. Try the EXAMPLE study first
```bash
python prototype_mapper.py --study_id EXAMPLE --input_csv data/source/EXAMPLE.csv --output_dir data/mapped/EXAMPLE/
```

### 2. Create your own study from template
```bash
cp -r studies/_TEMPLATE studies/MyStudy
```

### 3. Edit YAML configs in `studies/MyStudy/config/`
Customize entity YAML files to match your source data fields and target PCGL schema.

### 4. Run the mapper

**Single-file mode** (one CSV with all data):
```bash
python prototype_mapper.py --study_id MyStudy --input_csv data/source/your_data.csv --output_dir data/mapped/MyStudy/
```

**Multi-file mode** (directory with multiple files):
```bash
python prototype_mapper.py --study_id MyStudy --input_dir data/source/MyStudy/ --output_dir data/mapped/MyStudy/
```

### 5. Check the output
- Mapped CSV files: `data/mapped/MyStudy/*.csv`
- Mapping summary: `data/mapped/MyStudy/mapping_summary.txt`
- Detailed logs: `prototype_mapper.log`



## Data Mapper Structure

```
data_mapper/
├── studies/
│   ├── EXAMPLE/           # Simple working example for testing
│   ├── _TEMPLATE/         # Starting point for new studies
│   ├── HostSeq/           # Production example (15 entities)
│   └── YourStudy/
│       ├── config/        # YAML entity configs (required)
│          ├── participant.yaml
│          ├── diagnosis.yaml
│          └── ...
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

1. **Preprocess** - Apply filters, merge baseline fields, then clean/generate fields (YAML: `filters`, `preprocessing`)
2. **Map** - Transform fields (YAML: `mappings`, `configs`, pattern: `direct`/`expansion`)  
3. **Post-process** - Type conversion, cleanup (YAML: `post_processing`)
4. **Validate** - Quality checks (YAML: `validations`)
5. **Output** - Export CSV, log stats

See [core/README.md](core/README.md) for pipeline flowchart and details.

## YAML Configurations

Each entity has a YAML file with up to 7 sections. Most entities only need `entity` and `mappings`.

**Minimal example:**
```yaml
entity:
  name: Participant
  pattern: direct  # Options: 'direct' or 'expansion'
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
- `filters` - Participant eligibility check and row filtering (optional)
- `preprocessing` - Data cleaning and generation applied to source data before mapping (optional)
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
```

See [studies/README.md](studies/README.md) for complete YAML documentation and all supported options.


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
- Check logics defined in `filters` 
- Verify `validations` settings 
- Review `post_process` conditions

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
  - Key classes (`EntityMapper`, `StudyDataMapper`, `MappingConfig`)
  - Record transforms and utilities
  - Usage examples
