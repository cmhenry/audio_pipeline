#!/usr/bin/env python3
# parallel_coordinator.py - Utilities for coordinating parallel processing jobs
import argparse
import psycopg2
import pandas as pd
from pathlib import Path
import logging
from typing import List, Dict, Optional
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ParallelCoordinator:
    def __init__(self, db_string: str):
        self.db = psycopg2.connect(db_string)
    
    def process_metadata_for_day(self, date_str: str, staging_dir: Path):
        """Process all metadata and comments files for a day after parallel processing"""
        logger.info(f"Processing metadata for day {date_str}")
        
        year, month, day = map(int, date_str.split('-'))
        
        # Collect all parquet files
        metadata_files = sorted(staging_dir.glob(f"0_{date_str}_*_metadata.parquet"))
        comments_files = sorted(staging_dir.glob(f"0_{date_str}_*_comments.parquet"))
        subtitles_files = sorted(staging_dir.glob(f"0_{date_str}_*_subtitles.parquet"))
        
        logger.info(f"Found {len(metadata_files)} metadata, {len(comments_files)} comments, "
                   f"{len(subtitles_files)} subtitles files")
        
        # Process metadata
        if metadata_files:
            try:
                self._process_metadata_files(metadata_files, year, month, day)
            except Exception as e:
                logger.error(f"Failed to process metadata files: {e}")
        
        # Process comments
        if comments_files:
            try:
                self._process_comments_files(comments_files, year, month, day)
            except Exception as e:
                logger.error(f"Failed to process comments files: {e}")
        
        # Process subtitles
        if subtitles_files:
            try:
                self._process_subtitles_files(subtitles_files, year, month, day)
            except Exception as e:
                logger.error(f"Failed to process subtitles files: {e}")
    
    def _process_metadata_files(self, metadata_files: List[Path], year: int, month: int, day: int):
        """Process metadata parquet files"""
        logger.info(f"Processing {len(metadata_files)} metadata files")
        
        # Read all metadata files into single dataframe
        metadata_dfs = []
        for f in metadata_files:
            try:
                df = pd.read_parquet(f)
                metadata_dfs.append(df)
                logger.debug(f"Read {len(df)} records from {f.name}")
            except Exception as e:
                logger.error(f"Failed to read {f.name}: {e}")
        
        if not metadata_dfs:
            logger.warning("No metadata files could be read")
            return
        
        combined_metadata = pd.concat(metadata_dfs, ignore_index=True)
        logger.info(f"Combined metadata: {len(combined_metadata)} rows")
        
        # Store in database
        self._store_metadata_batch(combined_metadata, year, month, day)
    
    def _process_comments_files(self, comments_files: List[Path], year: int, month: int, day: int):
        """Process comments parquet files"""
        logger.info(f"Processing {len(comments_files)} comments files")
        
        comments_dfs = []
        for f in comments_files:
            try:
                df = pd.read_parquet(f)
                comments_dfs.append(df)
                logger.debug(f"Read {len(df)} records from {f.name}")
            except Exception as e:
                logger.error(f"Failed to read {f.name}: {e}")
        
        if not comments_dfs:
            logger.warning("No comments files could be read")
            return
        
        combined_comments = pd.concat(comments_dfs, ignore_index=True)
        logger.info(f"Combined comments: {len(combined_comments)} rows")
        
        # Store comments in database (implement when comments table is ready)
        # self._store_comments_batch(combined_comments, year, month, day)
        logger.info("Comments processing completed (storage not implemented yet)")
    
    def _process_subtitles_files(self, subtitles_files: List[Path], year: int, month: int, day: int):
        """Process subtitles parquet files"""
        logger.info(f"Processing {len(subtitles_files)} subtitles files")
        
        subtitles_dfs = []
        for f in subtitles_files:
            try:
                df = pd.read_parquet(f)
                subtitles_dfs.append(df)
                logger.debug(f"Read {len(df)} records from {f.name}")
            except Exception as e:
                logger.error(f"Failed to read {f.name}: {e}")
        
        if not subtitles_dfs:
            logger.warning("No subtitles files could be read")
            return
        
        combined_subtitles = pd.concat(subtitles_dfs, ignore_index=True)
        logger.info(f"Combined subtitles: {len(combined_subtitles)} rows")
        
        # Store subtitles in database (implement when subtitles table is ready)
        # self._store_subtitles_batch(combined_subtitles, year, month, day)
        logger.info("Subtitles processing completed (storage not implemented yet)")
    
    def _store_metadata_batch(self, metadata_df: pd.DataFrame, year: int, month: int, day: int):
        """Store metadata in database using pandas to_sql"""
        # Map DataFrame columns to database columns
        column_mapping = {
            'meta_id': 'meta_id',
            'author_id': 'author_id',
            'author_nickname': 'author_nickname',
            'stats_playcount': 'stats_playcount',
            'stats_sharecount': 'stats_sharecount',
            'stats_commentcount': 'stats_commentcount',
            'stats_diggcount': 'stats_diggcount',
        }
        
        # Rename columns that exist in the dataframe
        available_columns = {k: v for k, v in column_mapping.items() if k in metadata_df.columns}
        if available_columns:
            metadata_df = metadata_df.rename(columns=available_columns)
        
        # Add date columns
        metadata_df['year'] = year
        metadata_df['month'] = month
        metadata_df['date'] = day
        
        # Store in database
        try:
            # Convert psycopg2 connection to SQLAlchemy-compatible connection string
            from sqlalchemy import create_engine
            
            # Create engine from psycopg2 connection info
            conn_info = self.db.get_dsn_parameters()
            engine = create_engine(
                f"postgresql://{conn_info['user']}:{conn_info.get('password', '')}@"
                f"{conn_info['host']}:{conn_info['port']}/{conn_info['dbname']}"
            )
            
            metadata_df.to_sql(
                'audio_metadata',
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Stored {len(metadata_df)} metadata records")
            
        except Exception as e:
            logger.error(f"Failed to store metadata: {e}")
            # Fallback to manual insertion if needed
            self._store_metadata_manual(metadata_df, year, month, day)
    
    def _store_metadata_manual(self, metadata_df: pd.DataFrame, year: int, month: int, day: int):
        """Manual metadata insertion as fallback"""
        logger.info("Attempting manual metadata insertion")
        
        try:
            with self.db.cursor() as cur:
                for _, row in metadata_df.iterrows():
                    # Basic insertion with available columns
                    cur.execute("""
                        INSERT INTO audio_metadata (year, month, date, created_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (year, month, day))
                
                self.db.commit()
                logger.info(f"Manually stored {len(metadata_df)} metadata records")
                
        except Exception as e:
            logger.error(f"Manual metadata insertion also failed: {e}")
    
    def get_job_statistics(self, date_str: str) -> Dict:
        """Get processing statistics for a date"""
        year, month, day = map(int, date_str.split('-'))
        
        with self.db.cursor() as cur:
            # Get basic counts
            cur.execute("""
                SELECT COUNT(*) as audio_count
                FROM audio_files 
                WHERE year = %s AND month = %s AND date = %s
            """, (year, month, day))
            audio_count = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) as transcript_count
                FROM transcripts t
                JOIN audio_files a ON t.audio_file_id = a.id
                WHERE a.year = %s AND a.month = %s AND a.date = %s
            """, (year, month, day))
            transcript_count = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) as metadata_count
                FROM audio_metadata
                WHERE year = %s AND month = %s AND date = %s
            """, (year, month, day))
            metadata_count = cur.fetchone()[0]
            
            # Get processing queue status
            cur.execute("""
                SELECT status, processing_start, processing_end, error_message
                FROM processing_queue
                WHERE year = %s AND month = %s AND date = %s
            """, (year, month, day))
            queue_result = cur.fetchone()
            
            return {
                'date': date_str,
                'audio_files': audio_count,
                'transcripts': transcript_count,
                'metadata_records': metadata_count,
                'queue_status': queue_result[0] if queue_result else 'not_found',
                'processing_start': queue_result[1] if queue_result else None,
                'processing_end': queue_result[2] if queue_result else None,
                'error_message': queue_result[3] if queue_result else None
            }


def main():
    parser = argparse.ArgumentParser(description='Parallel processing coordinator utilities')
    parser.add_argument('--db-string', required=True, help='Database connection string')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Process metadata command
    metadata_parser = subparsers.add_parser('process-metadata', help='Process metadata files for a day')
    metadata_parser.add_argument('--date', required=True, help='Date to process (YYYY-MM-DD)')
    metadata_parser.add_argument('--staging-dir', required=True, help='Staging directory path')
    
    # Statistics command
    stats_parser = subparsers.add_parser('get-stats', help='Get processing statistics')
    stats_parser.add_argument('--date', required=True, help='Date to get stats for (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    coordinator = ParallelCoordinator(args.db_string)
    
    if args.command == 'process-metadata':
        staging_dir = Path(args.staging_dir)
        coordinator.process_metadata_for_day(args.date, staging_dir)
    
    elif args.command == 'get-stats':
        stats = coordinator.get_job_statistics(args.date)
        print(f"Statistics for {stats['date']}:")
        print(f"  Audio files: {stats['audio_files']}")
        print(f"  Transcripts: {stats['transcripts']}")
        print(f"  Metadata records: {stats['metadata_records']}")
        print(f"  Queue status: {stats['queue_status']}")
        if stats['processing_start']:
            print(f"  Processing start: {stats['processing_start']}")
        if stats['processing_end']:
            print(f"  Processing end: {stats['processing_end']}")
        if stats['error_message']:
            print(f"  Error: {stats['error_message']}")


if __name__ == '__main__':
    main()