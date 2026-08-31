## MOHCCN to PCGL example mapping

A small example that maps synthetic data as csvs in MOHCCN format to PCGL format.

The aim was to map the minimum metadata fields required for a valid submission so many fields remain unmapped.

Also includes `transform_genomic_to_csvs.py` a script to transform a candig genomic json file into csvs so that it works with the `pcgl-data-mapper` software.

Current config files for the mapping are:
- `analysis`
  - maps fields present in a candig genomic json 
- `experiment`
  - maps existing candig fields, yet to map specific instruments 
- `participant`
  - sets default values for DUO permissions and modifiers
- `sample`
  - maps PCGL required fields based on MOHCCN model
- `sociodemographic`
  - largely sets fields to missing values as these are not present in MOHCCN 
- `specimen`
  - maps PCGL required fields based on MOHCCN model  

### Current Notes:
- Not sure how to indicate missing values for dates/ages if it is a required field, e.g. `socidemographic.sociodem_date_collection`, `sociodemographic.age_at_sociodem_collection`. CanDIG doesn't store dates, but it is possible that sites can use real dates that they have in their system to populate date/age fields.
- Has not been validated or tested against an active PCGL deployment
- Should work from csvs here: https://github.com/CanDIG/mohccn-synthetic-data/tree/develop/extra_small_dataset_csv/raw_data and json file here: https://github.com/CanDIG/mohccn-synthetic-data/blob/develop/extra_small_dataset_csv/genomic.json
- Yet to map files-at time of writing it was not possible to have an analysis with multiple linked samples/experiments
- Assumes DACs and Studies will be created manually before these other objects are submitted
- Objects can only be submitted one study at a time
