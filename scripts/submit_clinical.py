#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCGL Clinical Data Submission Tool

Submits mapped CSV files to PCGL clinical data server via API endpoints.
Handles data submission, validation, and data commitment.
"""

import pandas as pd
import requests
import argparse
import os
import glob
import re
import sys
import time
import json
from datetime import datetime
from typing import Dict, List


def handle_api_error(response: requests.Response, url: str) -> None:
    """
    Handle API error responses with consistent error messaging.
    
    Args:
        response: HTTP response object
        url: API endpoint URL
        
    Raises:
        ValueError: With formatted error message
    """
    comments = [f'ERROR with {url}: Code {response.status_code}']
    
    try:
        error_data = response.json()
        if error_data.get('error'):
            comments.append(f"Error: {error_data['error']}")
        if error_data.get('message'):
            comments.append(f"Message: {error_data['message']}")
    except Exception:
        comments.append(f"Response: {response.text}")
    
    raise ValueError("\n".join(comments))


def api_request(
    method: str,
    url: str,
    token: str,
    timeout: int = 30,
    **kwargs
) -> requests.Response:
    """
    Make authenticated API request with error handling.
    
    Args:
        method: HTTP method (GET, POST, DELETE)
        url: API endpoint URL
        token: Authentication token
        timeout: Request timeout in seconds
        **kwargs: Additional arguments for requests
        
    Returns:
        Response object
        
    Raises:
        ValueError: If request fails
    """
    headers = kwargs.pop('headers', {})
    headers.setdefault('Authorization', f'Bearer {token}')
    headers.setdefault('accept', 'application/json')
    
    try:
        response = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
    except requests.exceptions.RequestException as e:
        raise ValueError(f'ERROR reaching {url}: {e}')
    
    if response.status_code not in [200, 404]:
        handle_api_error(response, url)
    
    return response


def parse_validation_errors(response_data: dict) -> List[str]:
    """
    Parse validation errors from API response.
    
    Args:
        response_data: JSON response data
        
    Returns:
        List of formatted error messages
    """
    errors = []
    inserts = response_data.get('errors', {}).get('inserts', {})
    
    for entity, entity_errors in inserts.items():
        for error in entity_errors:
            field_name = error.get('fieldName', 'N/A')
            reason = error.get('reason', 'Unknown')
            field_value = error.get('fieldValue', '')
            
            error_parts = [f"Entity: {entity}", f"Reason: {reason}", f"Field: {field_name}"]
            if field_value:
                error_parts.append(f"Value: {field_value}")
            errors.append("  " + ", ".join(error_parts))
    
    return errors


def retrieve_category_id(clinical_url: str, study_id: str, token: str) -> str:
    """
    Retrieve schema category ID for the study.
    
    Args:
        clinical_url: Base URL for clinical API
        study_id: Study identifier
        token: Authentication token
        
    Returns:
        Category ID string
        
    Raises:
        ValueError: If category ID cannot be retrieved
    """
    print(f"Retrieving category ID for study: {study_id}")
    response = api_request('GET', f"{clinical_url}/study/{study_id}", token)
    
    category_id = response.json().get('categoryId')
    if not category_id:
        raise ValueError(
            f"Study '{study_id}' schema not found. "
            f"Ensure the study is registered in the system."
        )
    
    print(f"Category ID: {category_id}")
    return str(category_id)

def check_existing_submission(
    category_id: str,
    clinical_url: str,
    study_id: str,
    token: str,
    _retries: int = 0,
    max_retries: int = 5
) -> bool:
    """
    Check for existing active submissions and delete them.
    
    Args:
        category_id: Schema category ID
        clinical_url: Base URL for clinical API
        study_id: Study identifier
        token: Authentication token
        
    Returns:
        True if check completed successfully
    """
    if _retries >= max_retries:
        raise ValueError(
            f"Failed to clear existing submissions after {max_retries} attempts. "
            "Check the server manually before retrying."
        )

    print("Checking for existing submissions...")
    url = f"{clinical_url}/submission/category/{category_id}?onlyActive=true&organization={study_id}"
    response = api_request('GET', url, token)
    
    if response.status_code == 404:
        print("No existing submission found")
        return True
    
    # Delete existing submissions
    total_records = response.json()['pagination']['totalRecords']
    if total_records > 0:
        print(f"Found {total_records} existing submission(s), deleting...")
        for record in response.json()['records']:
            print(f"Deleting submission: {record['id']}")
            api_request('DELETE', f"{clinical_url}/submission/{record['id']}", token)
        # Re-check to confirm all were removed
        return check_existing_submission(category_id, clinical_url, study_id, token, _retries + 1, max_retries)
    
    print("No active submissions")
    return True


def submit_clinical(
    clinical_url: str,
    category_id: str,
    study_id: str,
    input_directory: str,
    token: str
) -> str:
    """
    Submit clinical data files via API.
    
    Args:
        clinical_url: Base URL for clinical API
        category_id: Schema category ID
        study_id: Study identifier
        input_directory: Directory containing CSV files to submit
        token: Authentication token
        
    Returns:
        Submission ID string
    """
    print(f"Submitting clinical data from: {input_directory}")
    url = f"{clinical_url}/submission/category/{category_id}/data"
    headers = {
        "Authorization": f"Bearer {token}",
        'accept': 'application/json'
    }
    
    # Collect all CSV files
    csv_pattern = os.path.join(input_directory, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        raise ValueError(f"No CSV files found in {input_directory}")
    
    print(f"Found {len(csv_files)} CSV file(s) to submit")
    
    # Prepare file uploads
    files = []
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        print(f"  - {filename}")
        files.append((
            'files',
            (filename, open(file_path, 'rb'), 'text/csv')
        ))
    
    try:
        response = requests.post(
            url,
            headers=headers,
            files=files,
            data={"organization": study_id},
            timeout=120
        )
    except requests.exceptions.RequestException as e:
        raise ValueError(f'ERROR reaching {url}: {e}')
    finally:
        # Close all file handles
        for _, (_, file_handle, _) in files:
            file_handle.close()
    
    if response.status_code != 200:
        handle_api_error(response, url)
    
    submission_id = str(response.json()['submissionId'])
    print(f"Submission created: {submission_id}")
    return submission_id

def check_submission_status(
    clinical_url: str,
    submission_id: str,
    token: str,
    stage: str = 'validation',
    poll_interval: int = 5,
    max_wait: int = 300
) -> bool:
    """
    Poll submission status until it reaches the expected terminal state.

    Validation stage: polls until VALID (waits on OPEN).
    Commit stage:     polls until COMMITED (waits on VALID while server commits).
    Raises on INVALID in either stage.

    Status enum: OPEN, VALID, INVALID, CLOSED, COMMITED

    Args:
        clinical_url: Base URL for clinical API
        submission_id: Submission ID to check
        token: Authentication token
        stage: Either 'validation' or 'commit'
        poll_interval: Seconds between status polls
        max_wait: Maximum total seconds to wait before raising

    Returns:
        True when the expected terminal status is reached

    Raises:
        ValueError: If status is INVALID, an unexpected state, or max_wait exceeded
    """
    stage_msg = "Validating" if stage == 'validation' else "Verifying committed"
    print(f"{stage_msg} submission: {submission_id}")

    elapsed = 0
    while elapsed < max_wait:
        response = api_request('GET', f"{clinical_url}/submission/{submission_id}", token)
        status = response.json()['status']

        if status == 'INVALID':
            errors = parse_validation_errors(response.json())
            stage_error = "Validation" if stage == 'validation' else "Commit"
            raise ValueError(f"{stage_error} failed with errors:\n" + "\n".join(errors))

        if stage == 'validation':
            if status == 'VALID':
                print("Validation successful")
                return True
            if status == 'OPEN':
                print(f"Status: {status} — waiting... ({elapsed}s elapsed)")
            else:
                raise ValueError(
                    f"Submission {submission_id} reached unexpected status '{status}' "
                    f"during validation stage."
                )

        elif stage == 'commit':
            if status == 'COMMITED':
                print("Data successfully committed to database")
                return True
            if status == 'VALID':
                # Server is processing the commit, submission stays VALID until done
                print(f"Status: {status} — commit in progress... ({elapsed}s elapsed)")
            else:
                raise ValueError(
                    f"Submission {submission_id} reached unexpected status '{status}' "
                    f"during commit stage."
                )

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise ValueError(
        f"Timed out after {max_wait}s waiting for submission {submission_id} "
        f"to reach {'VALID' if stage == 'validation' else 'COMMITED'} status."
    )


def commit_clinical(
    clinical_url: str,
    category_id: str,
    submission_id: str,
    token: str
) -> bool:
    """
    Commit validated submission to database.
    
    Args:
        clinical_url: Base URL for clinical API
        category_id: Schema category ID
        submission_id: Submission ID to commit
        token: Authentication token
        
    Returns:
        True if commit successful
        
    Raises:
        ValueError: If commit request fails
    """
    print(f"Committing submission to database: {submission_id}")
    try:
        api_request('POST', f"{clinical_url}/submission/category/{category_id}/commit/{submission_id}", token, timeout=60)
        print("Submission committed successfully")
        return True
    except ValueError as e:
        raise ValueError(f"Failed to commit submission {submission_id}: {e}")


def split_entity_into_batches(
    entity_file: str,
    batch_size: int,
    output_base_dir: str,
    submission_folder: str
) -> List[str]:
    """
    Split a single entity CSV file into batches by record count.
    
    Args:
        entity_file: Path to the entity CSV file
        batch_size: Maximum number of records per batch
        output_base_dir: Base directory where batch subdirectories will be created
        submission_folder: Subfolder name for this submission (e.g., 'dry_run' or timestamp)
        
    Returns:
        List of batch directory paths, each containing one batched CSV file
    """
    filename = os.path.basename(entity_file)
    entity_name = os.path.splitext(filename)[0]
    
    # Read the entity file
    df = pd.read_csv(entity_file)
    total_records = len(df)
    
    if total_records == 0:
        print(f"{entity_name}: 0 records, skipping")
        return []
    
    # Create submission directory
    submission_dir = os.path.join(output_base_dir, submission_folder)
    os.makedirs(submission_dir, exist_ok=True)
    
    # Calculate number of batches
    num_batches = (total_records + batch_size - 1) // batch_size
    
    if num_batches == 1:
        print(f"{entity_name}: {total_records} records (1 batch)")
    else:
        print(f"{entity_name}: {total_records} records, splitting into {num_batches} batches")
    
    batch_dirs = []
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx]
        
        # Create batch directory under submission directory
        batch_dir = os.path.join(submission_dir, f"{entity_name}_batch_{i+1}")
        os.makedirs(batch_dir, exist_ok=True)
        batch_dirs.append(batch_dir)
        
        # Write batch to directory with original filename
        output_file = os.path.join(batch_dir, filename)
        batch_df.to_csv(output_file, index=False)
        print(f"  Batch {i+1}/{num_batches}: {len(batch_df)} records -> {batch_dir}")
    
    return batch_dirs


def save_submission_state(
    state_file: str,
    batch_states: Dict[str, Dict]
) -> None:
    """
    Save submission state to JSON file for resumability.
    
    Args:
        state_file: Path to state file
        batch_states: Dictionary mapping batch_dir -> {status, submission_id, error}
    """
    with open(state_file, 'w') as f:
        json.dump(batch_states, f, indent=2)


def load_submission_state(state_file: str) -> Dict[str, Dict]:
    """
    Load submission state from JSON file.
    
    Args:
        state_file: Path to state file
        
    Returns:
        Dictionary of batch states or empty dict if file doesn't exist
    """
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            return json.load(f)
    return {}


def main(args):
    """
    Main submission workflow.
    
    Args:
        args: Command-line arguments
    """
    print("=" * 80)
    print("PCGL Clinical Data Submission")
    if args.dry_run:
        print("[DRY RUN MODE - No data will be submitted]")
    print("=" * 80)
    print(f"Study ID: {args.study_id}")
    print(f"Entity: {args.entity}")
    print(f"Input Directory: {args.input_directory}")
    print(f"Batch Size: {args.batch_size} records per batch")
    if args.resume:
        print("Resume Mode: YES")
    print("=" * 80)
    
    # Create or resume submission folder
    if args.resume:
        # Find most recent submission folder to resume
        submission_base = os.path.join(args.input_directory, "submission")
        if not os.path.exists(submission_base):
            raise ValueError(f"No submission folder found at {submission_base}. Cannot resume.")
        
        submission_folders = [
            f for f in glob.glob(os.path.join(submission_base, "*"))
            if os.path.isdir(f) and os.path.basename(f) != "dry_run"
        ]
        
        if not submission_folders:
            raise ValueError("No previous submission found to resume. Run without --resume to start a new submission.")
        
        # Use most recent submission folder
        submission_dir = max(submission_folders, key=os.path.getmtime)
        submission_folder = os.path.basename(submission_dir)
        print(f"\nResuming submission from: {submission_folder}")
    else:
        # Create new submission folder
        if args.dry_run:
            submission_folder = "dry_run"
        else:
            submission_folder = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        submission_dir = os.path.join(args.input_directory, "submission", submission_folder)
        os.makedirs(submission_dir, exist_ok=True)
    
    # Initialize log data
    log_lines = []
    log_lines.append("=" * 80)
    log_lines.append("PCGL Clinical Data Submission Log")
    if args.dry_run:
        log_lines.append("[DRY RUN MODE - No data was submitted]")
    log_lines.append("=" * 80)
    log_lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_lines.append(f"Study ID: {args.study_id}")
    log_lines.append(f"Entity: {args.entity}")
    log_lines.append(f"Input Directory: {args.input_directory}")
    log_lines.append(f"Batch Size: {args.batch_size} records per batch")
    log_lines.append(f"Submission Folder: {submission_folder}")
    if args.resume:
        log_lines.append("Resume Mode: YES")
    log_lines.append("=" * 80)
    log_lines.append("")
    
    had_error = False
    try:
        # Step 1: Retrieve category ID
        category_id = retrieve_category_id(args.clinical_url, args.study_id, args.token)
        
        # Step 2: Check and delete existing submissions (skip only in dry-run mode)
        if not args.dry_run:
            check_existing_submission(category_id, args.clinical_url, args.study_id, args.token)
        else:
            print("\n[DRY RUN] Skipping existing submission check/deletion")
        
        # Step 3: Prepare batches
        if args.resume:
            # Load existing batches from submission folder
            print("\nLoading existing batches...")
            log_lines.append("\nLoading existing batches...")
            
            batch_dirs = sorted(
                glob.glob(os.path.join(submission_dir, f"{args.entity}_batch_*")),
                key=lambda p: int(m.group(1)) if (m := re.search(r'_batch_(\d+)$', os.path.basename(p))) else 0
            )

            if not batch_dirs:
                raise ValueError(
                    f"No batch directories found in {submission_dir} for entity '{args.entity}'. "
                    f"Cannot resume submission."
                )
            
            print(f"Found {len(batch_dirs)} existing batches")
            log_lines.append(f"Found {len(batch_dirs)} existing batches")
            
        else:
            # Create new batches by splitting entity file
            entity_file = os.path.join(args.input_directory, f"{args.entity}.csv")
            
            if not os.path.exists(entity_file):
                raise ValueError(f"Entity file not found: {entity_file}")
            
            print(f"\nEntity file: {entity_file}")
            log_lines.append(f"Entity file: {entity_file}")
            
            print("\nPreparing batches...")
            log_lines.append("\nPreparing batches...")
            batch_dirs = split_entity_into_batches(
                entity_file, 
                args.batch_size, 
                os.path.join(args.input_directory, "submission"), 
                submission_folder
            )
            
            if not batch_dirs:
                print("No batches to submit")
                log_lines.append("No batches to submit")
                return
        
        log_lines.append(f"Total batches: {len(batch_dirs)}")
        log_lines.append("")
        
        # Step 4: Setup state tracking for resumability
        state_file = os.path.join(submission_dir, f"{args.entity}_submission_state.json")
        batch_states = load_submission_state(state_file) if args.resume else {}
        
        # Step 5: Submit each batch
        submission_ids = []
        
        for batch_idx, batch_dir in enumerate(batch_dirs, 1):
            # Check if batch was already successfully completed
            if args.resume and batch_dir in batch_states:
                batch_state = batch_states[batch_dir]
                if batch_state.get('status') == 'completed':
                    print(f"\n[RESUME] Batch {batch_idx}/{len(batch_dirs)}: Already completed (Submission ID: {batch_state.get('submission_id')})")
                    submission_ids.append(batch_state.get('submission_id'))
                    log_lines.append(f"\nBatch {batch_idx}/{len(batch_dirs)}: [RESUMED - Already completed]")
                    log_lines.append(f"Submission ID: {batch_state.get('submission_id')}")
                    continue
                elif batch_state.get('status') == 'failed':
                    print(f"\n[RESUME] Batch {batch_idx}/{len(batch_dirs)}: Previously failed, retrying...")
                    log_lines.append(f"\nBatch {batch_idx}/{len(batch_dirs)}: [RETRY - Previously failed]")
            
            # Inter-batch pacing: each batch waits for COMMITED before proceeding
            if len(batch_dirs) > 1:
                print(f"\nBatch {batch_idx}/{len(batch_dirs)}:")
                log_lines.append(f"\nBatch {batch_idx}/{len(batch_dirs)}:")
            else:
                print(f"\nSubmitting:")
                log_lines.append(f"\nSubmitting:")
            
            log_lines.append(f"Batch directory: {batch_dir}")
            
            if args.dry_run:
                # Dry run mode - just show what would be submitted
                print(f"[DRY RUN] Would submit from: {batch_dir}")
                csv_files_in_batch = glob.glob(os.path.join(batch_dir, "*.csv"))
                for csv_file in csv_files_in_batch:
                    df = pd.read_csv(csv_file)
                    file_info = f"  - {os.path.basename(csv_file)}: {len(df)} records"
                    print(file_info)
                    log_lines.append(file_info)
                print(f"[DRY RUN] Would validate, commit, and verify")
                log_lines.append("[DRY RUN] Would validate, commit, and verify")
            else:
                # Actual submission
                try:
                    current_submission_id = None
                    submission_id = submit_clinical(args.clinical_url, category_id, args.study_id, batch_dir, args.token)
                    current_submission_id = submission_id
                    submission_ids.append(submission_id)
                    log_lines.append(f"Submission ID: {submission_id}")
                    
                    # Save state: submitted
                    batch_states[batch_dir] = {'status': 'submitted', 'submission_id': submission_id}
                    save_submission_state(state_file, batch_states)
                    
                    # Validate submission
                    check_submission_status(args.clinical_url, submission_id, args.token, 'validation')
                    log_lines.append("Status: Validated")
                    
                    # Save state: validated
                    batch_states[batch_dir]['status'] = 'validated'
                    save_submission_state(state_file, batch_states)
                    
                    # Commit to database
                    commit_clinical(args.clinical_url, category_id, submission_id, args.token)
                    log_lines.append("Status: Committing")
                    
                    # Verify final status
                    check_submission_status(args.clinical_url, submission_id, args.token, 'commit')
                    log_lines.append("Status: Committed")
                    
                    # Save state: completed
                    batch_states[batch_dir]['status'] = 'completed'
                    save_submission_state(state_file, batch_states)
                    
                    print(f"✓ Batch {batch_idx} completed (Submission ID: {submission_id})")
                    log_lines.append(f"✓ Batch {batch_idx} completed")
                    
                except ValueError as e:
                    # Save state: failed
                    # Use current_submission_id if submit_clinical succeeded before the error;
                    # fall back to the last known ID only if the submit itself never returned.
                    error_msg = str(e)
                    batch_states[batch_dir] = {
                        'status': 'failed',
                        'submission_id': current_submission_id, # type: ignore
                        'error': error_msg
                    }
                    save_submission_state(state_file, batch_states)
                    
                    print(f"✗ Batch {batch_idx} failed: {error_msg}")
                    log_lines.append(f"✗ Batch {batch_idx} failed")
                    log_lines.append(f"Error: {error_msg}")
                    
                    # Re-raise to stop processing remaining batches
                    raise
        
        print("\n" + "=" * 80)
        log_lines.append("\n" + "=" * 80)
        if args.dry_run:
            print("DRY RUN COMPLETED")
            print("=" * 80)
            print(f"Entity: {args.entity}")
            print(f"Batches: {len(batch_dirs)}")
            print(f"Batch directories created under: {submission_dir}")
            print("No data was submitted. Run without --dry-run to submit.")
            log_lines.append("DRY RUN COMPLETED")
            log_lines.append("=" * 80)
            log_lines.append(f"Entity: {args.entity}")
            log_lines.append(f"Batches: {len(batch_dirs)}")
            log_lines.append(f"Batch directories: {submission_dir}")
            log_lines.append("No data was submitted.")
        else:
            print("SUBMISSION COMPLETED SUCCESSFULLY")
            print("=" * 80)
            print(f"Entity: {args.entity}")
            print(f"Batches: {len(batch_dirs)}")
            joined_ids = ', '.join(sid for sid in submission_ids if sid is not None)
            print(f"Submission IDs: {joined_ids}")
            log_lines.append("SUBMISSION COMPLETED SUCCESSFULLY")
            log_lines.append("=" * 80)
            log_lines.append(f"Entity: {args.entity}")
            log_lines.append(f"Batches: {len(batch_dirs)}")
            log_lines.append(f"Submission IDs: {joined_ids}")
        print("=" * 80)
        log_lines.append("=" * 80)
        
    except ValueError as e:
        had_error = True
        log_lines.append("\n" + "=" * 80)
        log_lines.append("SUBMISSION FAILED")
        log_lines.append("=" * 80)
        log_lines.append(str(e))
        log_lines.append("=" * 80)
        
        print("\n" + "=" * 80)
        print("SUBMISSION FAILED")
        print("=" * 80)
        print(str(e))
        print("=" * 80)
        
    except Exception as e:
        had_error = True
        log_lines.append("\n" + "=" * 80)
        log_lines.append("UNEXPECTED ERROR")
        log_lines.append("=" * 80)
        log_lines.append(f"Error: {e}")
        log_lines.append("=" * 80)
        
        print("\n" + "=" * 80)
        print("UNEXPECTED ERROR")
        print("=" * 80)
        print(f"Error: {e}")
        print("=" * 80)
        
    finally:
        # Always write log file, even on failure
        log_file = os.path.join(submission_dir, f"{args.entity}_submission.log")
        try:
            # Append to log if resuming, otherwise create new log
            mode = 'a' if args.resume else 'w'
            with open(log_file, mode) as f:
                f.write('\n'.join(log_lines))
            print(f"\nLog file saved to: {log_file}")
        except Exception as log_error:
            print(f"\nWarning: Failed to write log file: {log_error}")
        
        # Exit with error code if there was a failure
        if had_error:
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='PCGL Clinical Data Submission Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Dry run - check what would be submitted without actually submitting
  python submit_clinical.py -cu https://submission.pcgl.org -si STUDY-01 -t YOUR_TOKEN -e participant -id data/mapped/STUDY-01/ --dry-run

  # Submit participant entity
  python submit_clinical.py -cu https://submission.pcgl.org -si STUDY-01 -t YOUR_TOKEN -e participant -id data/mapped/STUDY-01/

  # Submit demographic entity
  python submit_clinical.py -cu https://submission.pcgl.org -si STUDY-01 -t YOUR_TOKEN -e demographic -id data/mapped/STUDY-01/

  # Submit with custom batch size (500 records per batch)
  python submit_clinical.py -cu https://submission.pcgl.org -si STUDY-01 -t YOUR_TOKEN -e specimen -id data/mapped/STUDY-01/ -bs 500

  # Using environment variable for token
  export PCGL_TOKEN="your_token_here"
  python submit_clinical.py -cu https://submission.pcgl.org -si STUDY-01 -t $PCGL_TOKEN -e participant -id data/mapped/STUDY-01/

  # Resume a failed submission (uses most recent submission folder)
  python submit_clinical.py -cu https://submission.pcgl.org -si STUDY-01 -t YOUR_TOKEN -e participant -id data/mapped/STUDY-01/ --resume
        '''
    )
    
    parser.add_argument("-cu", "--clinical_url", dest="clinical_url", required=True, help="Clinical API base URL (e.g., https://submission.pcgl.org)")
    parser.add_argument("-si", "--study_id", dest="study_id", required=True, help="Study identifier (e.g., STUDY-01)")
    parser.add_argument("-t", "--token", dest="token", required=True, help="Authentication bearer token")
    parser.add_argument("-e", "--entity", dest="entity", required=True, help="Entity to submit (without .csv extension). Example: participant, demographic, specimen")
    parser.add_argument("-id", "--input-directory", dest="input_directory", required=False, default="./", help="Directory containing CSV files to submit (default: current directory)")
    parser.add_argument("-bs", "--batch-size", dest="batch_size", type=int, default=200, help="Maximum number of records per batch (default: 200)")
    parser.add_argument("-dr", "--dry-run", dest="dry_run", action="store_true", help="Dry run mode: check files, split into batches, but don't submit data")
    parser.add_argument("-r", "--resume", dest="resume", action="store_true", help="Resume mode: finds the most recent submission folder and skips already completed batches. Use this to continue after a failed submission.")
    
    args = parser.parse_args()
    main(args)