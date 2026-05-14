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
└── config/              # YAML entity configs (required)
    ├── participant.yaml
    ├── demographic.yaml
    ├── diagnosis.yaml
    └── ...
```

## Example Studies

### EXAMPLE
Simple working example for testing and learning:
- 10 entity configs with straightforward field mappings
- Uses `data/source/EXAMPLE.csv` as input data
- Demonstrates both direct and expansion mapping patterns
- No custom code required - pure YAML configuration
- Ideal for first-time users to understand the tool

Run it with:
```bash
python prototype_mapper.py --study_id EXAMPLE --input_csv data/source/EXAMPLE.csv --output_dir data/mapped/EXAMPLE/
```

See `EXAMPLE/config/*.yaml` for basic configuration patterns.

### _TEMPLATE
Starting template for new studies with:
- Pre-configured YAML files for all 13 standard entities
- Inline documentation and customization guidelines
- Example patterns for common mapping scenarios
- Base mapper files for custom code extensions

Copy the `_TEMPLATE` directory to create a new study, then customize the YAML files with your study's field mappings.


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
  pattern: [direct|expansion]  # Mapping pattern
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

**Range Expansion** (for repetitive configs): Auto-generate configs with templates
```yaml
entity:
  name: Household
  pattern: expansion
configs:
  - type: range
    start: 1
    end: 16
    template:
      source_field: "household_member_{n}"
      code: "HOUSEHOLD_MEMBER"
      term: "Household member {n}"
      enrichments:
        - target_field: member_id
          id_pattern: "{study_id}-{participant_id}-M{n:02d}"
```
This generates 16 configs with `{n}` replaced by 1-16, and `{n:02d}` zero-padded (01-16).

#### Multi-File Source Configuration

If the source data was splitted across multiple CSV files, you can specify which files each entity should use.

**Two Input Modes:**

1. **Single-file mode** : All data in one CSV file
   - CLI: `--input_csv data/source/study.csv`
   - No `source_files` config needed

```yaml
entity:
  name: Participant
  fields:
    base:
      - submitter_participant_id
```

2. **Multi-file mode** : Data split across multiple CSV files
   - CLI: `--input_dir data/source/MyStudy/`
   - Specify `source_files` in entity config

```yaml
entity:
  name: Diagnosis
  source_files:
    primary: "clinical.csv"        # Main data source
    secondary:
      - file: "demographics.csv"   # Additional data to join
        join_on: participant_id    # Join key (default: participant_id)
        join_type: left            # Join type (default: left)
        columns: [age, dob_year]   # Only load these columns (optional)
      
      - file: "lab_results.csv"
        join_on: participant_id
        join_type: inner
        columns: [crp_result, wbc_count]
      
      # Union multiple files (concatenate rows, union columns)
      - file: [specimens_batch1.csv, specimens_batch2.csv]
        join_on: participant_id
        join_type: left
        # Files are concatenated (rows appended)
        # Columns are unioned (missing columns filled with null)
```

**Join Configuration Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `file` | Yes | - | Data filename (string) or list of filenames to concatenate |
| `join_on` | Yes | - | Column name(s) to join on |
| `join_type` | No | `left` | Type of join: `left`, `right`, `inner`, `outer` |
| `columns` | No | All columns | List of specific columns to load (applied after concatenation) |

**File Union (Concatenation):**

When `file` is a **list of files**, the mapper will:
1. Load all files individually
2. **Concatenate rows** vertically (append all records)
3. **Union columns** (keep all columns from all files)
4. **Fill missing values** with null/NaN where columns don't exist in some files
5. Then join the concatenated result to primary file

Use this when:
- Files have the same/similar structure but from different batches/periods
- Files share most columns but each may have extra unique columns
- You want to combine data from multiple sources into one logical table

Example:
```yaml
# specimens_batch1.csv has: specimen_id, type, volume, storage_temp
# specimens_batch2.csv has: specimen_id, type, volume, collection_site
# Result will have: specimen_id, type, volume, storage_temp, collection_site
# Batch1 rows will have null collection_site
# Batch2 rows will have null storage_temp
```

**Supported File Formats:**
- `.csv` - Comma-separated values
- `.tsv` - Tab-separated values
- `.txt` - Text files with auto-detected delimiter

**Join Types:**
- **`left`** (default): Keep all primary records, add matching secondary data
- **`right`**: Keep all secondary records, add matching primary data  
- **`inner`**: Only records that exist in both files
- **`outer`**: All records from both files

**Auto-Discovery:**

If you don't specify `source_files`, the mapper will:
1. In multi-file mode (`--input_dir`): Look for `{entity_name}.csv`
2. In single-file mode (`--input_csv`): Use the provided CSV file

```yaml
entity:
  name: Participant
  # No source_files specified
  # Multi-file mode looks for: participant.csv
  # Single-file mode uses: --input_csv file
```


### 2. filters (optional)
Participant and row level filtering rules applied **before** preprocessing.

```yaml
filters:
  participant_id_field: "id_hostseq"  # Required for participant filtering
  
  # Step 1: Participant eligibility (applied first)
  participant:
    filter:
      - field: consent
        op: equals
        value: 1
      # AND logic by default, use 'any' for OR
      - any:
          - field: status
            op: equals
            value: "active"
          - field: enrolled
            op: equals
            value: 1
  
  # Step 2: Row selection (baseline/repeat instruments)
  rows:
    - name: baseline
      filter:
        - field: redcap_repeat_instrument
          op: is_null
    - name: laboratory_results
      filter:
        - field: redcap_repeat_instrument
          op: equals
          value: laboratory_results
  
  # Step 3: Enrichment (merge baseline fields into repeat rows)
  enrich:
    merge_baseline_fields: ["dob_year", "dob_month", "age"]
```

**Supported filter operators:**
- `equals`, `not_equals`: Exact match
- `in`, `not_in`: Value in list
- `is_null`, `is_not_null`: Null checks
- `greater_than`, `less_than`, `greater_equal`, `less_equal`: Numeric comparisons
- `regex_match_any`: Field matches any pattern in list

**Filter Logic:**
- Default: All conditions at same level are combined with AND
- `any`: Nest conditions in `any` block for OR logic
- `all`: Explicit AND logic (same as default)
- Supports nested `any`/`all` for complex boolean expressions

### 3. preprocessing (optional)
Field cleaning and generation applied **after filtering and enrichment** .

**Supported preprocessing types:**
- `clean_numeric`: Remove commas and spaces from numeric fields (e.g., "2,004" → 2004)
  - Supports exact field names, wildcard patterns (`*_numeric`, `measurement_*`), or `auto` to detect all numeric fields
- `strip_whitespace`: Remove leading/trailing whitespace
- `uppercase`: Convert text to uppercase
- `lowercase`: Convert text to lowercase
- `calculate_field`: Create calculated fields using pandas expressions
  - Uses `df.eval()` for safe formula evaluation with automatic NaN propagation
- `construct_date`: Build date fields from year/month/day components
  - Uses `safe_int_conversion` for robust parsing (handles "2,019", spaces, commas)
  - Requires `year_field` and `month_field`, `day_field` is optional
  - Returns YYYY-MM-DD formatted strings

```yaml
preprocessing:
  # Construct birth date from year/month components
  - type: construct_date
    target: birth_date
    params:
      year_field: dob_year
      month_field: dob_month
      day_field: dob_day       # Optional, defaults to 15
      default_day: 15          # Used if day_field not provided
  
  # Clean specific numeric fields
  - type: clean_numeric
    fields: [dob_year, household_size]
  
  # Clean all fields ending with _numeric
  - type: clean_numeric
    fields: ['*_numeric', '*_result_numeric']
  
  # Clean all measurement fields
  - type: clean_numeric
    fields: ['measurement_*']
  
  # Auto-detect all numeric-looking fields (contains commas/spaces in numbers)
  - type: clean_numeric
    fields: auto
  
  # Remove whitespace from text fields
  - type: strip_whitespace
    fields: [participant_id, study_id]
  
  # Normalize case
  - type: uppercase
    fields: [country_code]
  
  # Calculate age in days from gestational weeks and maternal age
  - type: calculate_field
    target: age_at_2nd_trimester_days
    formula: "(maternal_age_years * 365.25) + ((gestation_week_v2 - gestation_week_v1) * 7)"
  
  # Calculate BMI from weight and height
  - type: calculate_field
    target: bmi
    formula: "weight_kg / (height_m ** 2)"
  
  # Handle nulls with conditional expressions
  - type: calculate_field
    target: age_with_default
    formula: "age_days if age_days.notna() else 0"
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
      # Concatenates checked values into note field (default behavior)
      - target_field: comorbidity_note
        source_type: checkbox
        create_records: false  # Aggregate into existing record (default)
        append: true           # Concatenate values with "|" separator (default)
        value_mappings:
          com_cancer_location___1: "Cancer location: Lungs"
          com_cancer_location___2: "Cancer location: Breast"
          com_cancer_location___3: "Cancer location: Head and neck"
      
      # Overwrite instead of append
      - target_field: comorbidity_status
        source_type: checkbox
        append: false          # Only keep last matched value (no concatenation)
        value_mappings:
          status_active: "Active"
          status_inactive: "Inactive"
```

**Checkbox append behavior:**
- `append: true` (default) - Multiple checked boxes are concatenated with "|" separator
- `append: false` - Only the last matched checkbox value is kept (values overwrite)

**Use cases:**
- `create_records: true` - When each checkbox represents a distinct object (e.g., cancer subtypes become separate comorbidity records)
- `create_records: false` - When checkboxes provide additional details for current record (e.g., treatment notes, location descriptors)
- `append: true` - When you want to collect all checked values in one field (e.g., "Lungs | Breast")
- `append: false` - When only one value should be kept (e.g., status fields)

### 6. post_processing (optional)
Data cleaning applied to mapped records after field transformation.

**Supported post-processing types:**
- `clean_numeric`: Remove commas and spaces from numeric fields in mapped output (same syntax as preprocessing)
  - Supports exact field names, wildcard patterns (`*_numeric`, `measurement_*`), or `auto` to detect all numeric fields
- `convert_nullable_int`: Convert numeric columns to nullable Int64 type (preserves integers while allowing NaN)
  - `columns: auto` - Auto-detects age, duration, and count fields
  - `columns: [field1, field2]` - Explicit column list
- `filter_records`: Apply field-based filtering to mapped records (uses same operators as filters section)

```yaml
post_processing:
  # Clean numeric fields with commas in the OUTPUT data
  - type: clean_numeric
    fields: ['*_numeric']
  
  # Auto-detect and clean all numeric fields
  - type: clean_numeric
    fields: auto
  
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
  
  # Filter records by regex pattern matching
  # Keep only records where ontology code matches allowed patterns
  - type: filter_records
    field: comorbidity_code
    operator: regex_match_any
    value:
      - "^MONDO:.*"      # MONDO ontology codes
      - "^HP:.*"         # Human Phenotype Ontology codes
      - "^ICD10:.*"      # ICD-10 codes
      - "^SNOMED:.*"     # SNOMED CT codes
```

**Filter operators for post_processing:**
- `equals`, `not_equals`: Exact match
- `in`, `not_in`: Value in/not in list
- `is_null`, `is_not_null`: Null checks
- `greater_than`, `less_than`, `greater_equal`, `less_equal`: Numeric comparisons
- `regex_match_any`: Field matches any pattern in list (supports single pattern in list)

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



