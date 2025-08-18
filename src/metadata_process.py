import argparse
import psycopg2
import pandas as pd
import tarfile
import pyarrow.parquet as pq
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
import subprocess
import logging
import gc
from typing import List, Dict, Tuple

# Source storage_manager for rsync support
from storage_manager import create_storage_manager

# Start logging for debug purposes
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HPCTimestampedAudioProcessor:
    def __init__(self, args):
        self.staging_dir = Path(args.staging_dir)
        # Extract date info from staging directory name if needed
        # For monthly processing, we'll set these from args if provided
        self.year = getattr(args, 'year', 2025)
        self.month = getattr(args, 'month', 1) 
        self.day = getattr(args, 'day', 1)
        # self.batch_size = args.batch_size
        # self.num_workers = args.num_workers
        
        # Initialize connections
        self.db = psycopg2.connect(
            host=args.db_host,
            database="audio_pipeline",
            user="audio_user",
            password=args.db_password
        )
        
        # Storage configuration - use rsync to database host
        self.storage = create_storage_manager(
            db_host=args.db_host,
            use_dummy=getattr(args, 'use_dummy_storage', False),
            rsync_user=getattr(args, 'rsync_user', 'audio_user'),
            storage_root=getattr(args, 'storage_root', '/opt/audio_storage'),
            ssh_key_path="/secrets/"+args.ssh_keyfile
        )
        
        # Track processed files
        self.processed_count = 0
        self.failed_count = 0
        
    def process_month(self):
        """Processing stage for one month of parquet metadata files
    
        """
        
        logger.info(f"Processing month director {self.staging_dir}")
        
        try:
            logger.info("Processing metadata, subtitles, and comments...")
            self.process_month_metadata()
            
            # Update processing stats
            self._update_processing_stats()
            
            logger.info(f"Month metadata processing complete. Processed: {self.processed_count}, Failed: {self.failed_count}")
            
        except Exception as e:
            logger.error(f"Month processing failed: {e}")
            raise
  
    def process_month_metadata(self):
        """Process all metadata and comments files for the month"""
        # Collect all parquet files
        metadata_files = sorted(self.staging_dir.glob("^.*_metadata\.parquet"))
        comments_files = sorted(self.staging_dir.glob("^.*_comments\.parquet"))
        subtitles_files = sorted(self.staging_dir.glob("^.*_subtitles\.parquet"))
        
        logger.info(f"Found {len(metadata_files)} metadata, {len(comments_files)} comments, "
                   f"{len(subtitles_files)} subtitles files")
        
        # Process metadata
        if metadata_files:
            try:
                # Read all metadata files into single dataframe
                metadata_dfs = []
                for f in metadata_files:
                    try:
                        df = pd.read_parquet(f)
                        metadata_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Failed to read {f.name}: {e}")
                
                if metadata_dfs:
                    combined_metadata = pd.concat(metadata_dfs, ignore_index=True)
                    logger.info(f"Combined metadata: {len(combined_metadata)} rows")
                    
                    # Store in database
                    self._store_metadata_batch(combined_metadata)
                    
            except Exception as e:
                logger.error(f"Failed to process metadata: {e}")
        
        # Process comments similarly
        if comments_files:
            try:
                comments_dfs = []
                for f in comments_files:
                    try:
                        df = pd.read_parquet(f)
                        comments_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Failed to read {f.name}: {e}")
                
                if comments_dfs:
                    combined_comments = pd.concat(comments_dfs, ignore_index=True)
                    logger.info(f"Combined comments: {len(combined_comments)} rows")
                    
                    # Store comments
                    self._store_comments_batch(combined_comments)
                    
            except Exception as e:
                logger.error(f"Failed to process comments: {e}")
        
        # Process subtitles similarly
        if subtitles_files:
            try:
                subtitles_dfs = []
                for f in subtitles_files:
                    try:
                        df = pd.read_parquet(f)
                        subtitles_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Failed to read {f.name}: {e}")
                
                if subtitles_dfs:
                    combined_subtitles = pd.concat(subtitles_dfs, ignore_index=True)
                    logger.info(f"Combined subtitles: {len(combined_subtitles)} rows")
                    
                    # Store subtitles
                    self._store_subtitles_batch(combined_subtitles)
                    
            except Exception as e:
                logger.error(f"Failed to process subtitles: {e}")
    
    def _store_metadata_batch(self, metadata_df: pd.DataFrame):
        """Store metadata in database"""
        # Map DataFrame columns to database columns
        column_mapping = {
            'meta_id': 'meta_id',
            'author_id': 'author_id',
            'author_nickname': 'author_nickname',
            'stats_playcount': 'stats_playcount',
            # Add all other mappings...
        }
        
        # Rename columns
        metadata_df = metadata_df.rename(columns=column_mapping)
        
        # Add date columns
        metadata_df['year'] = self.year
        metadata_df['month'] = self.month
        metadata_df['date'] = self.day
        
        # Store in database
        try:
            metadata_df.to_sql(
                'audio_metadata',
                self.db,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Stored {len(metadata_df)} metadata records")
        except Exception as e:
            logger.error(f"Failed to store metadata: {e}")
    
    def _store_comments_batch(self, comments_df: pd.DataFrame):
        """Store comments in database"""
        # Map DataFrame columns to database columns
        column_mapping = {
            'comment_id': 'comment_id',
            'video_id': 'video_id',
            'author_id': 'author_id',
            'author_nickname': 'author_nickname',
            'comment_text': 'comment_text',
            'create_time': 'create_time',
            'like_count': 'like_count',
            'reply_count': 'reply_count',
            # Add other mappings as needed
        }
        
        # Rename columns
        comments_df = comments_df.rename(columns=column_mapping)
        
        # Add date columns
        comments_df['year'] = self.year
        comments_df['month'] = self.month
        comments_df['date'] = self.day
        
        # Store in database
        try:
            comments_df.to_sql(
                'comments',
                self.db,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Stored {len(comments_df)} comment records")
        except Exception as e:
            logger.error(f"Failed to store comments: {e}")
    
    def _store_subtitles_batch(self, subtitles_df: pd.DataFrame):
        """Store subtitles in database"""
        # Map DataFrame columns to database columns
        column_mapping = {
            'video_id': 'video_id',
            'subtitle_text': 'subtitle_text',
            'language': 'language',
            'start_time': 'start_time',
            'end_time': 'end_time',
            'confidence': 'confidence',
            # Add other mappings as needed
        }
        
        # Rename columns
        subtitles_df = subtitles_df.rename(columns=column_mapping)
        
        # Add date columns
        subtitles_df['year'] = self.year
        subtitles_df['month'] = self.month
        subtitles_df['date'] = self.day
        
        # Store in database
        try:
            subtitles_df.to_sql(
                'subtitles',
                self.db,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Stored {len(subtitles_df)} subtitle records")
        except Exception as e:
            logger.error(f"Failed to store subtitles: {e}")
    
    
    def _update_processing_stats(self):
        """Update processing statistics in database"""
        with self.db.cursor() as cur:
            cur.execute("""
                UPDATE processing_queue
                SET processing_end = NOW(),
                    status = CASE 
                        WHEN %s = 0 THEN 'completed'
                        WHEN %s > 0 AND %s > 0 THEN 'completed_with_errors'
                        ELSE 'processing_failed'
                    END,
                    error_message = CASE
                        WHEN %s > 0 THEN 'Failed files: ' || %s || ', Processed: ' || %s
                        ELSE NULL
                    END
                WHERE year = %s AND month = %s AND date = %s
            """, (self.failed_count, self.failed_count, self.processed_count,
                  self.failed_count, self.failed_count, self.processed_count,
                  self.year, self.month, self.day))
            
            self.db.commit()
    


def main():
    parser = argparse.ArgumentParser(description='Process metadata files for a single month')
    parser.add_argument('--staging-dir', required=True, help='Staging directory path')
    parser.add_argument('--db-host', required=True, help='Database host (also rsync target)')
    parser.add_argument('--db-password', default='audio_password', help='Database password')
    # parser.add_argument('--batch-size', type=int, default=100, help='Audio files per batch')
    # parser.add_argument('--num-workers', type=int, default=32, help='Parallel workers')
    
    # Storage options
    parser.add_argument('--rsync-user', default='audio_user', help='Username for rsync transfers')
    parser.add_argument('--storage-root', default='/opt/audio_storage', help='Root directory on target server')
    parser.add_argument('--use-dummy-storage', action='store_true', help='Use dummy storage (no actual transfers)')
    parser.add_argument('--ssh_keyfile', default='ent.pem', help='SSH identity file for cloud storage')
    
    args = parser.parse_args()
    
    processor = HPCTimestampedAudioProcessor(args)
    processor.process_month()


if __name__ == '__main__':
    main()