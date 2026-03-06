#!/usr/bin/env python3
"""
PCGL Data Mapper - Generic Study Processor

Configuration-driven data mapping framework for processing study data using YAML 
entity configurations and the generic mapper factory pattern.

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
    
    # Support both single-file (backward compatible) and multi-file (new) modes
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--input_csv', type=Path, help='Path to source CSV file (single-file mode)')
    input_group.add_argument('--input_dir', type=Path, help='Directory containing source CSV files (multi-file mode)')
    
    parser.add_argument('--output_dir', type=Path, help='Output directory for mapped files')
    parser.add_argument('--study_config_dir', type=Path, help='Root directory containing study configs (default: ./studies/)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose debug logging')
    args = parser.parse_args()
    
    # Enable debug logging if verbose flag is set
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose debug logging enabled")
    
    study_id = args.study_id
    input_csv_path = args.input_csv
    input_dir_path = args.input_dir
    output_dir = args.output_dir
    study_config_dir = args.study_config_dir
    
    # Validate input
    if input_csv_path and not input_csv_path.exists():
        logger.error(f"Input file not found: {input_csv_path}")
        sys.exit(1)
    
    if input_dir_path and not input_dir_path.exists():
        logger.error(f"Input directory not found: {input_dir_path}")
        sys.exit(1)
    
    try:
        # Initialize mapper (auto-discovers entities, dynamically loads study module)
        mapper = StudyDataMapper(study_id=study_id, study_root=study_config_dir)
        
        # Determine mode and process accordingly
        if input_csv_path:
            # Single-file mode (backward compatible)
            logger.info(f"Running in single-file mode with: {input_csv_path}")
            source_df = mapper.load_source_data(input_csv_path)
            mapper.process_all_entities(source_df)
        else:
            # Multi-file mode (new)
            logger.info(f"Running in multi-file mode with directory: {input_dir_path}")
            mapper.set_input_directory(input_dir_path)
            mapper.process_all_entities_multifile()
        
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
