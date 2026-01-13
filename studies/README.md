# Studies

Study-specific YAML configurations and customizations. Each study gets its own directory.

## Setup

```bash
# Copy template
cp -r studies/_TEMPLATE studies/YourStudyName
cd studies/YourStudyName
```

## Study Directory Structure

```
YourStudyName/
├── config/              # YAML entity configs (required)
│   ├── participant.yaml
│   ├── demographic.yaml
│   ├── diagnosis.yaml
│   └── ...
└── mappers/             # Custom code (optional)
    ├── __init__.py      # CUSTOM_FUNCTIONS dict
    ├── base.py          # Custom preprocessing/filtering
    └── transforms.py    # Study-specific transforms
```

## Example Studies

### _TEMPLATE
Starting template for new studies with:
- Pre-configured YAML files for all 13 standard entities
- Inline documentation and customization guidelines
- Example patterns for common mapping scenarios
- Base mapper files for custom code extensions

Copy the `_TEMPLATE` directory to create a new study, then customize the YAML files with your study's field mappings.

### HostSeq
Complete production example with:
- 15 entity configs (participant, diagnosis, comorbidity, etc.)
- Custom eligibility filtering
- Custom birth date construction
- Custom expansion logic

See `HostSeq/config/*.yaml` for entity configuration examples.



## YAML Configuration

Each entity YAML file defines how to map source data to PCGL schema. The file has up to 7 sections:

### 1. entity (required)
Entity metadata and schema field lists.

```yaml
entity:
  name: Participant           # Entity name (must match PCGL schema)
  fields:
    base:                     # Base schema fields
      - submitter_participant_id
      - study_id
    extension:                # Extension schema fields (optional)
      - custom_field
  pattern: [direct|expansion|custom]  # Mapping pattern
```

#### Pattern Configuration

**Direct pattern** (most entities): 1 input row → 1 output row
```yaml
entity:
  pattern: direct
```

**Expansion pattern** (checkboxes): 1 input row → N output rows  
```yaml
entity:
  name: Comorbidity
  pattern: expansion
  params:
    selection_type: radio      # 'checkbox' or 'radio'
    skip_values: [0, -1]       # Values to skip (e.g., 0=No, -1=Unknown)
configs:
  - source_field: diabetes_yn
    code: "E11"
    term: "Type 2 diabetes mellitus"
```

**Custom pattern** (advanced transformations): User-defined transformation logic
```yaml
entity:
  name: Household
  pattern: custom
  function: expand_household_relationships  # Function name from CUSTOM_FUNCTIONS
  params:                                    # Optional params passed to function
    relationship_field: relationship_type
```

For custom pattern, define your function in `mappers/__init__.py`:
```python
CUSTOM_FUNCTIONS = {
    'expand_household_relationships': my_expansion_function
}
```

Your function receives: `source_df`, `config`, `params`, and `**kwargs`, and must return a DataFrame.

### 2. preprocessing (optional)
Data cleaning applied to source data before mapping.

**Supported preprocessing types:**
- `clean_numeric`: Remove commas and spaces from numeric fields (e.g., "2,004" → 2004)
- `strip_whitespace`: Remove leading/trailing whitespace
- `uppercase`: Convert text to uppercase
- `lowercase`: Convert text to lowercase

**Note:** For complex preprocessing logic, override the `preprocess()` method in a custom mapper class (see Study-Specific Customization).

```yaml
preprocessing:
  # Clean numeric fields with comma separators
  - type: clean_numeric
    fields: [dob_year, household_size]
  
  # Remove whitespace from text fields
  - type: strip_whitespace
    fields: [participant_id, study_id]
  
  # Normalize case
  - type: uppercase
    fields: [country_code]
```

### 3. filters (optional)
Row-level filtering rules for which source rows to process.

**Supported filter configurations:**
- `eligible_participants`: Apply study-specific eligibility logic (define in YAML `participant_eligibility` or override `_filter_eligible_participants()` method for complex logic)
- `include_rows`: REDCap-specific filtering (baseline/repeat_instruments)
- `field_filters`: Generic field-based filtering with operators
- `participant_id_field`: Specify participant ID column name
- `merge_baseline_fields`: Merge baseline fields into repeat records

**Supported filter operators:**
- `equals`, `not_equals`: Exact match
- `in`, `not_in`: Value in list
- `is_null`, `is_not_null`: Null checks
- `greater_than`, `less_than`, `greater_equal`, `less_equal`: Numeric comparisons

```yaml
filters:
  # REDCap-specific filtering
  eligible_participants: true
  include_rows:
    baseline: true
    repeat_instruments: ["labs", "visits"]
  merge_baseline_fields: ["dob_year", "sex"]
  participant_id_field: "participant_id"
  
  # Generic field filtering (all conditions combined with AND)
  field_filters:
    - field: consent
      operator: equals
      value: 1
    - field: age_years
      operator: greater_equal
      value: 18
    - field: data_quality
      operator: is_not_null
```

### 4. mappings (required for `direct` and `expansion` pattern)
Field-level mapping logic. Each mapping defines how to transform source data into target fields.

```yaml
mappings:
  # Direct field copy
  - source_field: id
    target_field: submitter_participant_id
    
  # Constant value
  - target_field: study_id
    default_value: "MyStudy"
      
  # Age calculation
  - target_field: age_at_enrollment
    target_type: age
    params:
      birth_date_field: dob
      event_date_field: enrollment_date
      
  # ID generation
  - target_field: submitter_diagnosis_id
    target_type: identifier
    params:
      prefix_field: participant_id
      type: diagnosis
      
  # Date formatting
  - source_field: enroll_date
    target_field: enrollment_date
    target_type: date
  
  # Radio value mapping (single selection, coded values → terms)
  - source_field: vital_status_code
    target_field: vital_status
    source_type: radio
    value_mappings:
      0: "Alive"
      1: "Deceased"
      2: "Unknown"

  # Checkbox aggregation (concatenate multiple checked values)
  - source_field: null
    target_field: diagnosis_note
    source_type: checkbox
    value_mappings:
      field___1: "Clinical signs"
      field___2: "Household exposure"
      field___3: "Work exposure"
```

#### Target Field Types Reference

The `target_type` parameter specifies how to transform source data to the target type of the field:

| Type | Purpose | Example |
|------|---------|---------|
| `direct` | Copy as-is (default value if not setting) | participant_id → submitter_participant_id |
| `value` | Map codes to terms | 0 → "Alive", 1 → "Deceased" |
| `identifier` | Generate unique IDs | "PAT001_diagnosis_1" |
| `age` | Calculate age in days | birth_date + event_date → 15340 |
| `date` | Format to YYYY-MM-DD | "03/15/2020" → "2020-03-15" |
| `duration` | Days between dates | admission - discharge → 7 |
| `note` | Aggregate text | Combine multiple fields |
| `integer` | Convert to Int64 | "25" → 25 (nullable) |

### 5. configs (required for `expansion` pattern only)
Defines individual records for expansion pattern. Each config generates one record when source field value matches.

```yaml
configs:
  - source_field: com_diabetes
    code: "icd10:E11"
    term: "Type 2 diabetes mellitus"
    source_label: "Diabetes"
    
  - source_field: com_hypertension
    code: "icd10:I10"
    term: "Essential hypertension"
    source_label: "Hypertension"
    enrichments:                  # Optional field enrichments for this record
      - target_field: comorbidity_note
        source_field: hypertension_note
```
Enrichments can either create new records or enrich existing records:

**Create new records** (`create_records: true`): Generates additional records from checkbox selections
```yaml
configs:
  - source_field: com_cancer
    code: "MONDO:0004992"
    term: "Cancer"
    source_label: "Cancer diagnosis"
    enrichments:
      # Creates separate records for each cancer type
      - target_field: [comorbidity_code, comorbidity_term, comorbidity_source_text]
        source_type: checkbox
        create_records: true  # Generate N new records
        value_mappings:
          com_leukemia: ["MONDO:0005059", "Leukemia", "Leukemia"]
          com_lymphoma: ["MONDO:0005062", "Lymphoma", "Lymphoma"]
          com_sarcoma: ["MONDO:0005089", "Sarcoma", "Sarcoma"]
```

**Enrich existing record** (`create_records: false` or omitted): Aggregates values into current record
```yaml
configs:
  - source_field: com_cancer
    code: "MONDO:0004992"
    term: "Cancer"
    enrichments:
      # Concatenates checked values into note field
      - target_field: comorbidity_note
        source_type: checkbox
        create_records: false  # Aggregate into existing record (default)
        value_mappings:
          com_cancer_location___1: "Cancer location: Lungs"
          com_cancer_location___2: "Cancer location: Breast"
          com_cancer_location___3: "Cancer location: Head and neck"
```

**Use cases:**
- `create_records: true` - When each checkbox represents a distinct objects (e.g., cancer subtypes become separate comorbidity records)
- `create_records: false` - When checkboxes provide additional details for current record (e.g., treatment notes, location descriptors)

### 6. post_processing (optional)
Data cleaning applied to mapped records after field transformation.

**Supported post-processing types:**
- `convert_nullable_int`: Convert numeric columns to nullable Int64 type (preserves integers while allowing NaN)
  - `columns: auto` - Auto-detects age, duration, and count fields
  - `columns: [field1, field2]` - Explicit column list
- `filter_records`: Apply field-based filtering to mapped records (uses same operators as filters section)

```yaml
post_processing:
  # Convert to nullable integer (handles NaN values)
  - type: convert_nullable_int
    columns: [dob_year, age_at_enrollment]
    
  # Auto-detect all age/duration/integer fields
  - type: convert_nullable_int
    columns: auto
  
  # Filter out invalid records after mapping
  - type: filter_records
    field: measurement_value
    operator: is_not_null
```

### 7. validations (optional)
Quality checks applied to mapped records. Validation failures are logged but don't block output.

**Supported validation types:**
- `required`: Field must not be null
- `age_range`: Age in days must be within specified range
- `unique`: Field values must be unique across all records
- `participant_id`: Validate participant ID format

```yaml
validations:
  - type: required
    field: submitter_participant_id
    
  - type: age_range
    field: age_at_enrollment
    min_age: 0
    max_age: 43800  # 120 years in days
  
  - type: unique
    field: submitter_diagnosis_id
  
  - type: participant_id
    field: submitter_participant_id
```

## Study-Specific Customization (Optional)

Most studies only need YAML configuration. Only add custom Python code for complex logic that can't be expressed in YAML.

### When to Use Custom Code

Add custom Python code (`mappers/` directory) for:
- **Complex eligibility logic**. E.g, Multi-field conditional filtering with nested AND/OR logic
- **Custom transformations**. E.g, Birth date construction, relationship expansion, specialized calculations
- **Custom expansion functions**. E.g, Alternative wide-to-long transformation logic other then the default
- **Multi-field conditional logic**. E.g, Field values that depend on complex combinations of source fields

### Custom Functions Structure and Examples

```python
# studies/YourStudy/mappers/__init__.py
from .transforms import construct_birth_date, expand_relationships

CUSTOM_FUNCTIONS = {
    'construct_birth_date_from_year_month': construct_birth_date,
    'custom_expand_relationship': expand_relationships
}
```

Reference these functions by name in YAML configs:

```yaml
# In mappings or enrichments
- target_field: age_at_enrollment
  target_type: age
  params:
    birth_date_transform: construct_birth_date_from_year_month  # Custom function
    birth_year_field: dob_year
    birth_month_field: dob_month
```

### Custom Mapper Classes (Advanced)

Override base mapper behavior for complex preprocessing or filtering:

```python
# studies/YourStudy/mappers/base.py
from core.mappers.base import EntityMapper

class StudyEntityMapper(EntityMapper):
    def _filter_eligible_participants(self, df):
        """Custom eligibility logic with complex conditions."""
        consent_mask = (df['consent'] == 0) | (df['consent'].isna())
        age_mask = df['age_years'] < 18
        return df[~(consent_mask | age_mask)].copy()
```

See [HostSeq/mappers/base.py](HostSeq/mappers/base.py) for production example.


