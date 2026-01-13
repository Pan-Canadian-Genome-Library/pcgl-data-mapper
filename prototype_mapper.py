#!/usr/bin/env python3
"""
PCGL Data Mapper - Generic Study Processor

Configuration-driven data mapping framework for processing study data using YAML 
entity configurations and the generic mapper factory pattern.

Usage:
    python prototype_mapper.py --study_id <id> --input_csv <path> --output_dir <path>
    python prototype_mapper.py -h

Example:
    python prototype_mapper.py --study_id StudyA --input_csv data/source/input.csv --output_dir data/mapped/

Adding a New Study:
    Minimal (no custom code):
        1. Create studies/StudyName/config/ with YAML entity configs
        2. Run the mapper
    
    With custom functions:
        1. Create studies/StudyName/config/ with YAML entity configs
        2. Add studies/StudyName/mappers/__init__.py with CUSTOM_FUNCTIONS or create_mapper()
        3. Run the mapper

"""

import sys
import argparse
import logging
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Import StudyDataMapper from core
from core.mappers import StudyDataMapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prototype_mapper.log', mode='w'),  # 'w' mode overwrites on each run
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for generic study data mapper.
    
    Auto-discovers entities from YAML configs, dynamically loads study-specific 
    functions, processes all entities, and generates output files and reports.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='PCGL Data Mapper - Generic Study Processor',
        epilog='Example: %(prog)s --study_id StudyA --input_csv data/source/input.csv --output_dir data/mapped/'
    )
    parser.add_argument('--study_id', help='Study identifier')
    parser.add_argument('--input_csv', type=Path, help='Path to source CSV file')
    parser.add_argument('--output_dir', type=Path, help='Output directory for mapped files')
    args = parser.parse_args()
    
    study_id = args.study_id
    input_path = args.input_csv
    output_dir = args.output_dir
    
    # Validate input
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    
    try:
        # Initialize mapper (auto-discovers entities, dynamically loads study module)
        mapper = StudyDataMapper(study_id=study_id)
        
        # Load source data
        source_df = mapper.load_source_data(input_path)
        
        # Process all entities (automatically from discovered configs)
        mapper.process_all_entities(source_df)
        
        # Save results
        mapper.save_results(output_dir)
        
        # Generate and save summary report
        mapper.save_summary_report(output_dir)
        
        logger.info(f"Data mapper completed successfully for {study_id}!")
        logger.info(f"Auto-discovered and processed {len(mapper.entities)} entities")
        
    except Exception as e:
        logger.error(f"Data mapper failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
