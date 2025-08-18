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
        # Map DataFrame columns to database columns (from METADATA_NAMES.md)
        column_mapping = {
            # Core metadata
            'meta_id': 'meta_id',
            'poi_id': 'poi_id',
            'duetinfo_duetfromid': 'duetinfo_duetfromid',
            'meta_createtime': 'meta_createtime',
            'meta_scheduletime': 'meta_scheduletime',
            'meta_itemcommentstatus': 'meta_itemcommentstatus',
            'meta_diversificationid': 'meta_diversificationid',
            'meta_categorytype': 'meta_categorytype',
            'meta_textlanguage': 'meta_textlanguage',
            'meta_desc': 'meta_desc',
            'meta_locationcreated': 'meta_locationcreated',
            'meta_diversificationlabels': 'meta_diversificationlabels',
            'meta_serverabversions': 'meta_serverabversions',
            'meta_suggestedwords': 'meta_suggestedwords',
            'meta_adlabelversion': 'meta_adlabelversion',
            'meta_bainfo': 'meta_bainfo',
            'meta_secret': 'meta_secret',
            'meta_privateitem': 'meta_privateitem',
            'meta_duetenabled': 'meta_duetenabled',
            'meta_stitchenabled': 'meta_stitchenabled',
            'meta_indexenabled': 'meta_indexenabled',
            'meta_iscontentclassified': 'meta_iscontentclassified',
            'meta_isaigc': 'meta_isaigc',
            'meta_isad': 'meta_isad',
            'meta_isecvideo': 'meta_isecvideo',
            'meta_aigclabeltype': 'meta_aigclabeltype',
            'meta_aigcdescription': 'meta_aigcdescription',
            
            # Author information
            'author_id': 'author_id',
            'author_uniqueid': 'author_uniqueid',
            'author_nickname': 'author_nickname',
            'author_signature': 'author_signature',
            'author_roomid': 'author_roomid',
            'author_verified': 'author_verified',
            'author_openfavorite': 'author_openfavorite',
            'author_commentsetting': 'author_commentsetting',
            'author_duetsetting': 'author_duetsetting',
            'author_stitchsetting': 'author_stitchsetting',
            'author_downloadsetting': 'author_downloadsetting',
            'author_createtime': 'author_createtime',
            
            # Author statistics
            'authorstats_followercount': 'authorstats_followercount',
            'authorstats_followingcount': 'authorstats_followingcount',
            'authorstats_heart': 'authorstats_heart',
            'authorstats_heartcount': 'authorstats_heartcount',
            'authorstats_videocount': 'authorstats_videocount',
            'authorstats_diggcount': 'authorstats_diggcount',
            'authorstats_friendcount': 'authorstats_friendcount',
            
            # Music information
            'music_id': 'music_id',
            'music_title': 'music_title',
            'music_authorname': 'music_authorname',
            'music_album': 'music_album',
            'music_duration': 'music_duration',
            'music_schedulesearchtime': 'music_schedulesearchtime',
            'music_collected': 'music_collected',
            
            # Statistics
            'stats_diggcount': 'stats_diggcount',
            'stats_sharecount': 'stats_sharecount',
            'stats_commentcount': 'stats_commentcount',
            'stats_playcount': 'stats_playcount',
            'stats_collectcount': 'stats_collectcount',
            
            # Video specifications
            'video_height': 'video_height',
            'video_width': 'video_width',
            'video_duration': 'video_duration',
            'video_bitrate': 'video_bitrate',
            'video_ratio': 'video_ratio',
            'video_encodedtype': 'video_encodedtype',
            'video_format': 'video_format',
            'video_videoquality': 'video_videoquality',
            'video_codectype': 'video_codectype',
            'video_definition': 'video_definition',
            
            # Location/POI information
            'poi_type': 'poi_type',
            'poi_name': 'poi_name',
            'poi_address': 'poi_address',
            'poi_city': 'poi_city',
            'poi_citycode': 'poi_citycode',
            'poi_province': 'poi_province',
            'poi_country': 'poi_country',
            'poi_countrycode': 'poi_countrycode',
            'poi_fatherpoiid': 'poi_fatherpoiid',
            'poi_fatherpoiname': 'poi_fatherpoiname',
            'poi_category': 'poi_category',
            'poi_tttypecode': 'poi_tttypecode',
            'poi_typecode': 'poi_typecode',
            'poi_tttypenametiny': 'poi_tttypenametiny',
            'poi_tttypenamemedium': 'poi_tttypenamemedium',
            'poi_tttypenamesuper': 'poi_tttypenamesuper',
            
            # Address information
            'adress_addresscountry': 'adress_addresscountry',
            'adress_addresslocality': 'adress_addresslocality',
            'adress_addressregion': 'adress_addressregion',
            'adress_streetaddress': 'adress_streetaddress',
            
            # Status and messages
            'statuscode': 'statuscode',
            'statusmsg': 'statusmsg',
            'description_hash': 'description_hash',
            
            # Arrays/JSONB fields
            'subtitle_subtitle_lang': 'subtitle_subtitle_lang',
            'bitrate_bitrate_info': 'bitrate_bitrate_info',
            'text_extra_user_mention': 'text_extra_user_mention',
            'text_extra_hashtag_mention': 'text_extra_hashtag_mention',
            'warning_warning': 'warning_warning',
            
            # Processing metadata
            'timestamp': 'timestamp',
            'pol': 'pol',
            'hour': 'hour',
            'country': 'country',
            'processed_desc': 'processed_desc',
            'raw': 'raw',
            'collection_timestamp': 'collection_timestamp',
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
                self.db_engine,
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
        # Map DataFrame columns to database columns (from METADATA_NAMES.md)
        column_mapping = {
            # Core comment information
            'meta_id': 'meta_id',
            'cid': 'cid',
            'aweme_id': 'aweme_id',
            'text': 'comment_text',
            'create_time': 'create_time',
            
            # Comment engagement stats
            'digg_count': 'digg_count',
            'reply_comment_total': 'reply_comment_total',
            
            # Comment metadata
            'comment_language': 'comment_language',
            'status': 'status',
            'stick_position': 'stick_position',
            'is_comment_translatable': 'is_comment_translatable',
            'no_show': 'no_show',
            
            # User engagement flags
            'user_digged': 'user_digged',
            'user_buried': 'user_buried',
            'is_author_digged': 'is_author_digged',
            'author_pin': 'author_pin',
            
            # Reply information
            'reply_id': 'reply_id',
            'reply_to_reply_id': 'reply_to_reply_id',
            'reply_comment': 'reply_comment',
            'reply_score': 'reply_score',
            'show_more_score': 'show_more_score',
            
            # Author information
            'uid': 'uid',
            'sec_uid': 'sec_uid',
            'nickname': 'nickname',
            'unique_id': 'unique_id',
            'custom_verify': 'custom_verify',
            'enterprise_verify_reason': 'enterprise_verify_reason',
            
            # JSONB fields for complex data
            'account_labels': 'account_labels',
            'label_list': 'label_list',
            'sort_tags': 'sort_tags',
            'comment_post_item_ids': 'comment_post_item_ids',
            'collect_stat': 'collect_stat',
            'ad_cover_url': 'ad_cover_url',
            'advance_feature_item_order': 'advance_feature_item_order',
            'advanced_feature_info': 'advanced_feature_info',
            'bold_fields': 'bold_fields',
            'can_message_follow_status_list': 'can_message_follow_status_list',
            'can_set_geofencing': 'can_set_geofencing',
            'cha_list': 'cha_list',
            'cover_url': 'cover_url',
            'events': 'events',
            'followers_detail': 'followers_detail',
            'geofencing': 'geofencing',
            'homepage_bottom_toast': 'homepage_bottom_toast',
            'item_list': 'item_list',
            'mutual_relation_avatars': 'mutual_relation_avatars',
            'need_points': 'need_points',
            'platform_sync_info': 'platform_sync_info',
            'relative_users': 'relative_users',
            'search_highlight': 'search_highlight',
            'shield_edit_field_info': 'shield_edit_field_info',
            'type_label': 'type_label',
            'user_profile_guide': 'user_profile_guide',
            'user_tags': 'user_tags',
            'white_cover_url': 'white_cover_url',
            
            # Processing metadata
            'total': 'total',
            'collection_timestamp': 'collection_timestamp',
            'hash_unique_id': 'hash_unique_id',
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
                self.db_engine,
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
        # Map DataFrame columns to database columns (from METADATA_NAMES.md)
        column_mapping = {
            # Core subtitle information
            'meta_id': 'meta_id',
            'content': 'content',
            'lang': 'lang',
            'type': 'type',
            'rest': 'rest',
            
            # Processing metadata
            'collection_timestamp': 'collection_timestamp',
            'hash_unique_id': 'hash_unique_id',
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
                self.db_engine,
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