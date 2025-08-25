import argparse
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import tarfile
import pyarrow.parquet as pq
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
import subprocess
import logging
import gc
import re
from typing import List, Dict, Tuple
from sqlalchemy import create_engine

# Source storage_manager for rsync support
from storage_manager import create_storage_manager

# Start logging for debug purposes
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HPCTimestampedAudioProcessor:
    def __init__(self, args):
        self.staging_dir = Path(args.staging_dir + "/prepped_data")
        # Extract date info from staging directory name if needed
        # For monthly processing, we'll set these from args if provided
        self.year = 2025
        self.month = getattr(args, 'month', 1) 
        self.day = 1
        # self.batch_size = args.batch_size
        # self.num_workers = args.num_workers
        
        # Initialize connections
        # psycopg2 connection for raw SQL operations
        self.db = psycopg2.connect(
            host=args.db_host,
            database="audio_pipeline",
            user="audio_user",
            password=args.db_password
        )
        
        # SQLAlchemy engine for pandas operations
        db_url = f"postgresql://audio_user:{args.db_password}@{args.db_host}:5432/audio_pipeline"
        self.db_engine = create_engine(db_url)
        
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

    def _deduplicate_batch(self, df: pd.DataFrame, unique_columns: list, table_name: str) -> pd.DataFrame:
        """Remove duplicates within the batch, keeping the last occurrence"""
        
        original_count = len(df)
        
        # Remove duplicates based on unique constraint columns, keeping last occurrence
        df_deduped = df.drop_duplicates(subset=unique_columns, keep='last')
        
        removed_count = original_count - len(df_deduped)
        
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} duplicate rows from {table_name} batch (kept latest)")
            logger.info(f"Deduplicated {table_name}: {original_count} → {len(df_deduped)} records")
        else:
            logger.info(f"No duplicates found in {table_name} batch")
    
        return df_deduped
    
    def _extract_date_from_filename(self, filename: str) -> Tuple[int, int, int]:
        """Extract year, month, day from filename containing date in format YYYY-MM-DD"""
        # Look for date pattern YYYY-MM-DD in filename
        date_pattern = r'(\d{4})-(\d{2})-(\d{2})'
        match = re.search(date_pattern, filename)
        
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return year, month, day
        else:
            logger.warning(f"Could not extract date from filename: {filename}")
            # Return default values if no date found
            return self.year, self.month, self.day
        
    def process_month(self):
        """Processing stage for one month of parquet metadata files
    
        """
        
        logger.info(f"Processing month directory {self.staging_dir}")
        
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
        logger.info(f"Searching for parquet files in: {self.staging_dir}")
        logger.info(f"Directory exists: {self.staging_dir.exists()}")
        
        # List directory contents for debugging
        # if self.staging_dir.exists():
        #     logger.info(f"Directory contents: {list(self.staging_dir.iterdir())}")
        
        # Collect all parquet files (fix glob patterns - remove regex ^ syntax)
        metadata_files = sorted(self.staging_dir.glob("*_metadata.parquet"))
        comments_files = sorted(self.staging_dir.glob("*_comments.parquet"))
        subtitles_files = sorted(self.staging_dir.glob("*_subtitles.parquet"))
        
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
                        
                        # Extract date from filename and add columns
                        year, month, day = self._extract_date_from_filename(f.name)
                        df['year'] = year
                        df['month'] = month
                        df['date'] = day
                        
                        logger.info(f"Processed {f.name}: {len(df)} rows with date {year}-{month:02d}-{day:02d}")
                        metadata_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Failed to read {f.name}: {e}")
                
                if metadata_dfs:
                    combined_metadata = pd.concat(metadata_dfs, ignore_index=True)
                    logger.info(f"Combined metadata: {len(combined_metadata)} rows")

                    # Debug the problematic values
                    # self._debug_bigint_ranges(combined_metadata, "metadata")   

                    # Convert boolean columns before storing & sanitize ranges
                    combined_metadata = self._convert_metadata_boolean_columns(combined_metadata)
                    # combined_metadata = self._sanitize_bigint_values(combined_metadata)
                    
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
                        
                        # Extract date from filename and add columns
                        year, month, day = self._extract_date_from_filename(f.name)
                        df['year'] = year
                        df['month'] = month
                        df['date'] = day
                        
                        logger.info(f"Processed {f.name}: {len(df)} rows with date {year}-{month:02d}-{day:02d}")
                        comments_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Failed to read {f.name}: {e}")
                
                if comments_dfs:
                    combined_comments = pd.concat(comments_dfs, ignore_index=True)
                    logger.info(f"Combined comments: {len(combined_comments)} rows")
                    
                    # Deduplicate within batch before other processing
                    combined_comments = self._deduplicate_batch(
                        combined_comments, 
                        ['cid', 'meta_id', 'year', 'month', 'date'], 
                        'comments'
                    )
                    
                    # Continue with existing processing
                    combined_comments = self._convert_comment_boolean_columns(combined_comments)
                    
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
                        
                        # Extract date from filename and add columns
                        year, month, day = self._extract_date_from_filename(f.name)
                        df['year'] = year
                        df['month'] = month
                        df['date'] = day
                        
                        logger.info(f"Processed {f.name}: {len(df)} rows with date {year}-{month:02d}-{day:02d}")
                        subtitles_dfs.append(df)
                    except Exception as e:
                        logger.error(f"Failed to read {f.name}: {e}")
                
                if subtitles_dfs:
                    combined_subtitles = pd.concat(subtitles_dfs, ignore_index=True)
                    logger.info(f"Combined subtitles: {len(combined_subtitles)} rows")

                    # Debug and sanitize  
                    # self._debug_bigint_ranges(combined_subtitles, "subtitles")
                    # combined_subtitles = self._sanitize_bigint_values(combined_subtitles)
                    
                    # Store subtitles
                    self._store_subtitles_batch(combined_subtitles)
                    
            except Exception as e:
                logger.error(f"Failed to process subtitles: {e}")
    
    def _convert_metadata_boolean_columns(self, metadata_df: pd.DataFrame) -> pd.DataFrame:
        """Convert integer boolean columns to actual booleans in metadata"""
        boolean_columns = [
            'meta_secret', 'meta_privateitem', 'meta_duetenabled', 'meta_stitchenabled',
            'meta_indexenabled', 'meta_iscontentclassified', 'meta_isaigc', 'meta_isad',
            'meta_isecvideo', 'author_verified', 'author_openfavorite', 'music_collected'
        ]
        
        for col in boolean_columns:
            if col in metadata_df.columns:
                metadata_df[col] = metadata_df[col].fillna(0).astype(int).astype(bool)
        
        return metadata_df

    def _store_metadata_batch(self, metadata_df: pd.DataFrame):
        """Store metadata in database using UPSERT to handle duplicates"""
        logger.info(f"Storing {len(metadata_df)} metadata records with UPSERT...")

        # self._debug_bigint_ranges(metadata_df, "metadata")
        # metadata_df = self._sanitize_bigint_values(metadata_df)
        
        # Define the columns we want to insert (in order)
        columns = [
            'meta_id', 'year', 'month', 'date', 'poi_id', 'duetinfo_duetfromid',
            'meta_createtime', 'meta_scheduletime', 'meta_itemcommentstatus', 
            'meta_diversificationid', 'meta_categorytype', 'meta_textlanguage',
            'meta_desc', 'meta_locationcreated', 'meta_diversificationlabels',
            'meta_serverabversions', 'meta_suggestedwords', 'meta_adlabelversion',
            'meta_bainfo', 'meta_secret', 'meta_privateitem', 'meta_duetenabled',
            'meta_stitchenabled', 'meta_indexenabled', 'meta_iscontentclassified',
            'meta_isaigc', 'meta_isad', 'meta_isecvideo', 'meta_aigclabeltype',
            'meta_aigcdescription', 'author_id', 'author_uniqueid', 'author_nickname',
            'author_signature', 'author_roomid', 'author_verified', 'author_openfavorite',
            'author_commentsetting', 'author_duetsetting', 'author_stitchsetting',
            'author_downloadsetting', 'author_createtime', 'authorstats_followercount',
            'authorstats_followingcount', 'authorstats_heart', 'authorstats_heartcount',
            'authorstats_videocount', 'authorstats_diggcount', 'authorstats_friendcount',
            'music_id', 'music_title', 'music_authorname', 'music_album', 'music_duration',
            'music_schedulesearchtime', 'music_collected', 'stats_diggcount',
            'stats_sharecount', 'stats_commentcount', 'stats_playcount', 'stats_collectcount',
            'video_height', 'video_width', 'video_duration', 'video_bitrate', 'video_ratio',
            'video_encodedtype', 'video_format', 'video_videoquality', 'video_codectype',
            'video_definition', 'poi_type', 'poi_name', 'poi_address', 'poi_city',
            'poi_citycode', 'poi_province', 'poi_country', 'poi_countrycode',
            'poi_fatherpoiid', 'poi_fatherpoiname', 'poi_category', 'poi_tttypecode',
            'poi_typecode', 'poi_tttypenametiny', 'poi_tttypenamemedium', 'poi_tttypenamesuper',
            'adress_addresscountry', 'adress_addresslocality', 'adress_addressregion',
            'adress_streetaddress', 'statuscode', 'statusmsg', 'description_hash',
            'subtitle_subtitle_lang', 'bitrate_bitrate_info', 'text_extra_user_mention',
            'text_extra_hashtag_mention', 'warning_warning', 'timestamp', 'pol', 'hour',
            'country', 'processed_desc', 'raw', 'collection_timestamp'
        ]
        
        # Prepare data - only include columns that exist in the dataframe
        available_columns = [col for col in columns if col in metadata_df.columns]
        data_tuples = [tuple(row[col] if col in row else None for col in available_columns) 
                      for _, row in metadata_df.iterrows()]
        
        # Build the UPSERT query
        placeholders = ', '.join(['%s'] * len(available_columns))
        columns_str = ', '.join(available_columns)
        
        # Create UPDATE SET clause (exclude the unique constraint columns from updates)
        update_columns = [col for col in available_columns if col not in ['meta_id', 'year', 'month', 'date']]
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        upsert_query = f"""
            INSERT INTO audio_metadata ({columns_str})
            VALUES %s
            ON CONFLICT (meta_id, year, month, date) 
            DO UPDATE SET 
                {update_set},
                updated_at = NOW()
        """
        
        try:
            with self.db.cursor() as cur:
                execute_values(
                    cur, upsert_query, data_tuples,
                    template=None, page_size=1000
                )
                self.db.commit()
            logger.info(f"Successfully upserted {len(metadata_df)} metadata records")
        except Exception as e:
            logger.error(f"Failed to upsert metadata: {e}")
            self.db.rollback()
            raise
    
    def _convert_comment_boolean_columns(self, comments_df: pd.DataFrame) -> pd.DataFrame:
        """Convert integer boolean columns to actual booleans"""
        boolean_columns = [
            'is_comment_translatable', 'no_show', 'user_digged', 'user_buried',
            'is_author_digged', 'author_pin', 'music_collected'
        ]
        
        for col in boolean_columns:
            if col in comments_df.columns:
                # Convert 1/0 to True/False, handle NaN values
                comments_df[col] = comments_df[col].fillna(0).astype(int).astype(bool)
                logger.debug(f"Converted {col} to boolean")
        
        return comments_df

    def _store_comments_batch(self, comments_df: pd.DataFrame):
        """Store comments in database using UPSERT to handle duplicates"""
        logger.info(f"Storing {len(comments_df)} comment records with UPSERT...")
        
        # Define the columns we want to insert (in order)
        columns = [
            'meta_id', 'year', 'month', 'date', 'cid', 'aweme_id', 'comment_text',
            'create_time', 'digg_count', 'reply_comment_total', 'comment_language',
            'status', 'stick_position', 'is_comment_translatable', 'no_show',
            'user_digged', 'user_buried', 'is_author_digged', 'author_pin',
            'reply_id', 'reply_to_reply_id', 'reply_comment', 'reply_score',
            'show_more_score', 'uid', 'sec_uid', 'nickname', 'unique_id',
            'custom_verify', 'enterprise_verify_reason', 'account_labels',
            'label_list', 'sort_tags', 'comment_post_item_ids', 'collect_stat',
            'ad_cover_url', 'advance_feature_item_order', 'advanced_feature_info',
            'bold_fields', 'can_message_follow_status_list', 'can_set_geofencing',
            'cha_list', 'cover_url', 'events', 'followers_detail', 'geofencing',
            'homepage_bottom_toast', 'item_list', 'mutual_relation_avatars',
            'need_points', 'platform_sync_info', 'relative_users', 'search_highlight',
            'shield_edit_field_info', 'type_label', 'user_profile_guide',
            'user_tags', 'white_cover_url', 'total', 'collection_timestamp',
            'hash_unique_id'
        ]
        
        # Prepare data - only include columns that exist in the dataframe
        available_columns = [col for col in columns if col in comments_df.columns]
        data_tuples = [tuple(row[col] if col in row else None for col in available_columns) 
                      for _, row in comments_df.iterrows()]
        
        # Build the UPSERT query
        placeholders = ', '.join(['%s'] * len(available_columns))
        columns_str = ', '.join(available_columns)
        
        # Create UPDATE SET clause (exclude the unique constraint columns from updates)
        update_columns = [col for col in available_columns if col not in ['cid', 'meta_id', 'year', 'month', 'date']]
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        upsert_query = f"""
            INSERT INTO comments ({columns_str})
            VALUES %s
            ON CONFLICT (cid, meta_id, year, month, date) 
            DO UPDATE SET 
                {update_set},
                updated_at = NOW()
        """
        
        try:
            with self.db.cursor() as cur:
                execute_values(
                    cur, upsert_query, data_tuples,
                    template=None, page_size=1000
                )
                self.db.commit()
            logger.info(f"Successfully upserted {len(comments_df)} comment records")
        except Exception as e:
            logger.error(f"Failed to upsert comments: {e}")
            self.db.rollback()
            raise
    
    def _store_subtitles_batch(self, subtitles_df: pd.DataFrame):
        """Store subtitles in database using UPSERT to handle duplicates"""
        logger.info(f"Storing {len(subtitles_df)} subtitle records with UPSERT...")
        
        # Define the columns we want to insert (in order)
        columns = [
            'meta_id', 'year', 'month', 'date', 'content', 'lang', 'type', 'rest',
            'collection_timestamp', 'hash_unique_id'
        ]
        
        # Prepare data - only include columns that exist in the dataframe
        available_columns = [col for col in columns if col in subtitles_df.columns]
        data_tuples = [tuple(row[col] if col in row else None for col in available_columns) 
                      for _, row in subtitles_df.iterrows()]
        
        # Build the UPSERT query
        placeholders = ', '.join(['%s'] * len(available_columns))
        columns_str = ', '.join(available_columns)
        
        # Create UPDATE SET clause (exclude the unique constraint columns from updates)
        update_columns = [col for col in available_columns if col not in ['meta_id', 'year', 'month', 'date']]
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        upsert_query = f"""
            INSERT INTO subtitles ({columns_str})
            VALUES %s
            ON CONFLICT (meta_id, year, month, date) 
            DO UPDATE SET 
                {update_set},
                updated_at = NOW()
        """
        
        try:
            with self.db.cursor() as cur:
                execute_values(
                    cur, upsert_query, data_tuples,
                    template=None, page_size=1000
                )
                self.db.commit()
            logger.info(f"Successfully upserted {len(subtitles_df)} subtitle records")
        except Exception as e:
            logger.error(f"Failed to upsert subtitles: {e}")
            self.db.rollback()
            raise
    
    
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
    parser.add_argument('--month', help='Month to process')
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