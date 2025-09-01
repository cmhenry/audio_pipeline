#!/usr/bin/env python3
"""
merge_metadata.py - Merge metadata parquet files with classified data

This script:
1. Finds all files ending with "_metadata.parquet" in a folder
2. Opens a classified parquet file (e.g., "classified_zurich_2025_january.parquet")
3. Extracts dates from metadata filenames (pattern: YYYY-MM-DD)
4. Merges entries based on meta_id, year, month, and day
5. Keeps only matches present in the classified parquet file
"""

import argparse
import re
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_metadata_files(folder_path: Path) -> list:
    """Find all files ending with '_metadata.parquet' in the given folder"""
    metadata_files = []
    
    if not folder_path.exists():
        logger.error(f"Folder does not exist: {folder_path}")
        return metadata_files
    
    for file_path in folder_path.rglob("*_metadata.parquet"):
        metadata_files.append(file_path)
    
    logger.info(f"Found {len(metadata_files)} metadata files")
    return metadata_files


def extract_date_from_filename(filename: str) -> tuple:
    """Extract year, month, day from filename containing YYYY-MM-DD pattern"""
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    match = re.search(date_pattern, filename)
    
    if match:
        date_str = match.group(1)
        year, month, day = map(int, date_str.split('-'))
        logger.debug(f"Extracted date from {filename}: {year}-{month:02d}-{day:02d}")
        return year, month, day
    else:
        logger.warning(f"No date pattern found in filename: {filename}")
        return None, None, None


def load_classified_data(classified_file: Path) -> pd.DataFrame:
    """Load the classified parquet file"""
    if not classified_file.exists():
        logger.error(f"Classified file does not exist: {classified_file}")
        return pd.DataFrame()
    
    try:
        classified_df = pd.read_parquet(classified_file)
        logger.info(f"Loaded classified data: {len(classified_df)} rows")
        return classified_df
    except Exception as e:
        logger.error(f"Error loading classified file {classified_file}: {e}")
        return pd.DataFrame()


def load_metadata_file(metadata_file: Path, target_year: int, target_month: int, target_day: int) -> pd.DataFrame:
    """Load metadata file and add date columns"""
    try:
        metadata_df = pd.read_parquet(metadata_file)
        
        # Add date columns for matching
        metadata_df['year'] = target_year
        metadata_df['month'] = target_month
        metadata_df['day'] = target_day
        
        logger.info(f"Loaded metadata from {metadata_file.name}: {len(metadata_df)} rows")
        return metadata_df
        
    except Exception as e:
        logger.error(f"Error loading metadata file {metadata_file}: {e}")
        return pd.DataFrame()


def merge_data(classified_df: pd.DataFrame, metadata_files: list) -> pd.DataFrame:
    """Merge classified data with metadata files based on meta_id, year, month, day"""
    
    if classified_df.empty:
        logger.error("Classified data is empty, cannot merge")
        return pd.DataFrame()
    
    # Ensure classified data has required columns
    required_cols = ['meta_id', 'year', 'month', 'day']
    missing_cols = [col for col in required_cols if col not in classified_df.columns]
    if missing_cols:
        logger.error(f"Classified data missing required columns: {missing_cols}")
        return pd.DataFrame()
    
    merged_data = []
    
    for metadata_file in metadata_files:
        year, month, day = extract_date_from_filename(metadata_file.name)
        
        if year is None or month is None or day is None:
            logger.warning(f"Skipping file with no date pattern: {metadata_file.name}")
            continue
        
        # Load metadata for this date
        metadata_df = load_metadata_file(metadata_file, year, month, day)
        
        if metadata_df.empty:
            logger.warning(f"No metadata loaded from {metadata_file.name}")
            continue
        
        # Filter classified data for this specific date
        date_classified = classified_df[
            (classified_df['year'] == year) &
            (classified_df['month'] == month) &
            (classified_df['day'] == day)
        ]
        
        if date_classified.empty:
            logger.info(f"No classified data for date {year}-{month:02d}-{day:02d}")
            continue
        
        # Merge on meta_id, year, month, day - keep only matches in classified data
        merged = date_classified.merge(
            metadata_df,
            on=['meta_id', 'year', 'month', 'day'],
            how='inner'  # Only keep records present in classified data
        )
        
        if not merged.empty:
            merged_data.append(merged)
            logger.info(f"Merged {len(merged)} records for {year}-{month:02d}-{day:02d}")
        else:
            logger.info(f"No matches found for {year}-{month:02d}-{day:02d}")
    
    if merged_data:
        final_df = pd.concat(merged_data, ignore_index=True)
        logger.info(f"Total merged records: {len(final_df)}")
        return final_df
    else:
        logger.warning("No data was merged")
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description='Merge metadata parquet files with classified data')
    parser.add_argument('--folder', required=True, type=str,
                        help='Folder containing metadata parquet files')
    parser.add_argument('--classified-file', required=True, type=str,
                        help='Path to classified parquet file')
    parser.add_argument('--output', required=True, type=str,
                        help='Output path for merged parquet file')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Convert paths
    folder_path = Path(args.folder)
    classified_file = Path(args.classified_file)
    output_path = Path(args.output)
    
    logger.info(f"Starting metadata merge process")
    logger.info(f"Metadata folder: {folder_path}")
    logger.info(f"Classified file: {classified_file}")
    logger.info(f"Output file: {output_path}")
    
    # Find metadata files
    metadata_files = find_metadata_files(folder_path)
    if not metadata_files:
        logger.error("No metadata files found")
        return 1
    
    # Load classified data
    classified_df = load_classified_data(classified_file)
    if classified_df.empty:
        logger.error("No classified data loaded")
        return 1
    
    # Merge data
    merged_df = merge_data(classified_df, metadata_files)
    
    if merged_df.empty:
        logger.error("No merged data produced")
        return 1
    
    # Save merged data
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged_df.to_parquet(output_path)
        logger.info(f"Merged data saved to: {output_path}")
        logger.info(f"Final merged dataset: {len(merged_df)} rows, {len(merged_df.columns)} columns")
        
        # Log column summary
        logger.info(f"Columns in merged data: {list(merged_df.columns)}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error saving merged data: {e}")
        return 1


if __name__ == "__main__":
    exit(main())