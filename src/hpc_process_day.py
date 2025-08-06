# hpc_process_day.py - Updated for timestamped file structure and rsync storage
import argparse
import psycopg2
import pandas as pd
import tarfile
import pyarrow.parquet as pq
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
import torch
import whisperx
import subprocess
import logging
import gc
from typing import List, Dict, Tuple

from storage_manager import create_storage_manager

torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HPCTimestampedAudioProcessor:
    def __init__(self, args):
        self.date_str = args.date
        self.year, self.month, self.day = map(int, self.date_str.split('-'))
        self.staging_dir = Path(args.staging_dir)
        self.temp_dir = Path(args.temp_dir)
        self.batch_size = args.batch_size
        self.num_workers = args.num_workers
        
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
        
        # Initialize WhisperX on GPU
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {self.device}")
        
        self.model = whisperx.load_model(
            "large-v2",
            device=self.device,
            compute_type="float16" if self.device == "cuda" else "float32"
        )
        
        # Track processed files
        self.processed_count = 0
        self.failed_count = 0
        
    def process_day(self):
        """Main processing pipeline for one day of timestamped files"""
        logger.info(f"Processing day {self.date_str}")
        
        try:
            # Get all tar.xz files for this day
            tar_files = sorted(self.staging_dir.glob(f"0_{self.date_str}_*.tar.xz"))
            logger.info(f"Found {len(tar_files)} tar files to process")
            
            if not tar_files:
                raise ValueError(f"No tar files found for {self.date_str}")
            
            # Process tar files in sequence (they're already time-ordered)
            for tar_file in tar_files:
                timestamp = self._extract_timestamp(tar_file.name)
                logger.info(f"Processing {tar_file.name} (timestamp: {timestamp})")
                
                try:
                    self.process_timestamp_archive(tar_file, timestamp)
                except Exception as e:
                    logger.error(f"Failed to process {tar_file.name}: {e}")
                    self.failed_count += 1
                    continue
            
            # After all audio is processed, handle metadata and comments
            logger.info("Processing metadata and comments...")
            self.process_day_metadata()
            
            # Update processing stats
            self._update_processing_stats()
            
            logger.info(f"Day processing complete. Processed: {self.processed_count}, Failed: {self.failed_count}")
            
        except Exception as e:
            logger.error(f"Day processing failed: {e}")
            raise
    
    def _extract_timestamp(self, filename: str) -> str:
        """Extract HH_MM timestamp from filename like 0_2025-01-31_23_50.tar.xz"""
        parts = filename.replace('.tar.xz', '').split('_')
        if len(parts) >= 5:
            return f"{parts[3]}_{parts[4]}"
        return "unknown"
    
    def process_timestamp_archive(self, tar_path: Path, timestamp: str):
        """Process a single timestamp's tar.xz file"""
        batch_dir = self.temp_dir / f"{self.date_str}_{timestamp}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Open tar.xz file
            with tarfile.open(tar_path, 'r:*') as tar:
                members = [m for m in tar.getmembers() if m.name.endswith('.mp3')]
                logger.info(f"Found {len(members)} MP3 files in {tar_path.name}")
                
                # Process in batches
                for i in range(0, len(members), self.batch_size):
                    batch_members = members[i:i + self.batch_size]
                    self.process_audio_batch(tar, batch_members, batch_dir, timestamp, i // self.batch_size)
                    
                    # Clear GPU memory between batches
                    if self.device == "cuda":
                        torch.cuda.empty_cache()
                        gc.collect()
                    
        finally:
            # Clean up batch directory
            import shutil
            if batch_dir.exists():
                shutil.rmtree(batch_dir)
    
    def process_audio_batch(self, tar, members: List, batch_dir: Path, timestamp: str, batch_num: int):
        """Process a batch of audio files from the tar"""
        logger.info(f"Processing batch {batch_num} with {len(members)} files")
        
        # Extract audio files
        audio_paths = []
        for member in members:
            tar.extract(member, batch_dir)
            audio_paths.append(batch_dir / member.name)
        
        # Parallel conversion to Opus
        with ProcessPoolExecutor(max_workers=min(self.num_workers, len(audio_paths))) as executor:
            opus_results = list(executor.map(self.convert_to_opus, audio_paths))
        
        # Filter successful conversions
        opus_paths = [(orig, opus) for orig, opus in opus_results if opus is not None]
        
        if not opus_paths:
            logger.warning(f"No successful conversions in batch {batch_num}")
            return
        
        # Batch transcription on GPU
        transcripts = self.batch_transcribe_gpu([p[1] for p in opus_paths])
        
        # Store results
        with self.db.cursor() as cur:
            for (orig_path, opus_path), transcript in zip(opus_paths, transcripts):
                try:
                    # Extract original filename info
                    orig_filename = orig_path.name
                    
                    # Store in database
                    cur.execute("""
                        INSERT INTO audio_files 
                        (filename, file_path, year, month, date, created_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        RETURNING id
                    """, (orig_filename, None, self.year, self.month, self.day))
                    
                    audio_id = cur.fetchone()[0]
                    
                    # Store transcript
                    cur.execute("""
                        INSERT INTO transcripts 
                        (audio_file_id, transcript_text, duration_seconds)
                        VALUES (%s, %s, %s, %s)
                    """, (audio_id, transcript['transcript'], 
                          transcript.get('duration', 0)))
                    
                    # Upload to storage via rsync
                    storage_path = self.storage.get_storage_path(
                        self.year, self.month, self.day, timestamp, opus_path.name
                    )
                    if self.storage.upload_file(opus_path, storage_path):
                        cur.execute("""
                            UPDATE audio_files 
                            SET file_path = %s 
                            WHERE id = %s
                        """, (storage_path, audio_id))
                    
                    self.processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to store {orig_filename}: {e}")
                    self.failed_count += 1
                    
            self.db.commit()
        
        # Clean up audio files immediately
        for orig_path, opus_path in opus_paths:
            orig_path.unlink(missing_ok=True)
            opus_path.unlink(missing_ok=True)
    
    @staticmethod
    def convert_to_opus(mp3_path: Path) -> Tuple[Path, Path]:
        """Convert MP3 to Opus format"""
        try:
            opus_path = mp3_path.with_suffix('.opus')
            
            cmd = [
                'ffmpeg', '-i', str(mp3_path),
                '-c:a', 'libopus',
                '-b:a', '32k',
                '-application', 'voip',
                '-vbr', 'on',
                '-compression_level', '10',
                '-ac', '1',  # Convert to mono
                '-ar', '16000',  # 16kHz sample rate
                '-y', str(opus_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"FFmpeg error for {mp3_path.name}: {result.stderr}")
                return mp3_path, None
                
            return mp3_path, opus_path
            
        except Exception as e:
            logger.error(f"Conversion error for {mp3_path.name}: {e}")
            return mp3_path, None
    
    def batch_transcribe_gpu(self, audio_paths: List[Path]) -> List[Dict]:
        """Transcribe batch of audio files on GPU"""
        results = []
        
        for audio_path in audio_paths:
            try:
                # Load audio
                audio = whisperx.load_audio(str(audio_path))
                
                # Transcribe
                result = self.model.transcribe(
                    audio
                )
                
                # Extract transcript
                transcript_text = ' '.join([s['text'].strip() for s in result.get('segments', [])])
                # word_count = len(transcript_text.split())
                
                results.append({
                    'transcript': transcript_text,
                    # 'word_count': word_count,
                    'duration': len(audio) / 16000  # Assuming 16kHz
                })
                
            except Exception as e:
                logger.error(f"Transcription error for {audio_path.name}: {e}")
                results.append({
                    'transcript': '',
                    # 'word_count': 0,
                    'duration': 0
                })
        
        return results
    
    def process_day_metadata(self):
        """Process all metadata and comments files for the day"""
        # Collect all parquet files
        metadata_files = sorted(self.staging_dir.glob(f"0_{self.date_str}_*_metadata.parquet"))
        comments_files = sorted(self.staging_dir.glob(f"0_{self.date_str}_*_comments.parquet"))
        subtitles_files = sorted(self.staging_dir.glob(f"0_{self.date_str}_*_subtitles.parquet"))
        
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
                    
                    # Store comments (you'll need to create a comments table)
                    # self._store_comments_batch(combined_comments)
                    
            except Exception as e:
                logger.error(f"Failed to process comments: {e}")
    
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
    parser = argparse.ArgumentParser(description='Process audio files for a single day')
    parser.add_argument('--date', required=True, help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--staging-dir', required=True, help='Staging directory path')
    parser.add_argument('--temp-dir', required=True, help='Temporary directory path')
    parser.add_argument('--db-host', required=True, help='Database host (also rsync target)')
    parser.add_argument('--db-password', default='audio_password', help='Database password')
    parser.add_argument('--batch-size', type=int, default=100, help='Audio files per batch')
    parser.add_argument('--num-workers', type=int, default=32, help='Parallel workers')
    
    # Storage options
    parser.add_argument('--rsync-user', default='audio_user', help='Username for rsync transfers')
    parser.add_argument('--storage-root', default='/opt/audio_storage', help='Root directory on target server')
    parser.add_argument('--use-dummy-storage', action='store_true', help='Use dummy storage (no actual transfers)')
    parser.add_argument('--ssh_keyfile', default='ent.pem', help='SSH identity file for cloud storage')
    
    args = parser.parse_args()
    
    processor = HPCTimestampedAudioProcessor(args)
    processor.process_day()


if __name__ == '__main__':
    main()