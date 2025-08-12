#!/usr/bin/env python3
"""
streaming_tar_processor.py - Stream processing of tar.gz files with async storage
Processes tar files in batches without full extraction, with background uploads
"""

import argparse
import psycopg2
import tarfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import torch
import whisperx
import subprocess
import logging
import gc
import os
import time
from typing import List, Dict, Tuple, Iterator
import tempfile
import shutil

from streaming_storage_manager import create_async_storage_manager

torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingTarProcessor:
    """Stream processor for tar files with async uploads"""
    
    def __init__(self, args):
        self.date_str = args.date
        self.year, self.month, self.day = map(int, self.date_str.split('-'))
        self.tar_file = Path(args.tar_file)
        self.timestamp = args.timestamp
        self.temp_dir = Path(args.temp_dir)
        self.stream_batch_size = args.stream_batch_size  # Files to extract at once
        self.process_batch_size = args.process_batch_size  # Files to process in parallel
        self.num_workers = args.num_workers
        
        # Initialize database connection
        self.db = psycopg2.connect(
            host=args.db_host,
            database="audio_pipeline",
            user="audio_user",
            password=args.db_password
        )
        
        # Initialize async storage manager
        self.storage = create_async_storage_manager(
            db_host=args.db_host,
            use_dummy=getattr(args, 'use_dummy_storage', False),
            rsync_user=getattr(args, 'rsync_user', 'audio_user'),
            storage_root=getattr(args, 'storage_root', '/opt/audio_storage'),
            ssh_key_path="/secrets/"+args.ssh_keyfile,
            max_concurrent_uploads=args.max_concurrent_uploads,
            upload_timeout=args.upload_timeout
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
        self.upload_count = 0
        
        # Create streaming work directory
        self.stream_work_dir = self.temp_dir / f"stream_{self.date_str}_{self.timestamp}"
        self.stream_work_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Streaming processor initialized: stream_batch={self.stream_batch_size}, "
                   f"process_batch={self.process_batch_size}, uploads={args.max_concurrent_uploads}")
    
    def process_tar_file_streaming(self):
        """Process tar file using streaming approach"""
        logger.info(f"Starting streaming processing of {self.tar_file.name}")
        
        if not self.tar_file.exists():
            raise FileNotFoundError(f"Tar file not found: {self.tar_file}")
        
        try:
            # Stream process the tar file
            total_members = self._count_mp3_members()
            logger.info(f"Found {total_members} MP3 files to process")
            
            processed_batches = 0
            
            # Process in streaming batches
            for batch_files in self._stream_extract_batches():
                if not batch_files:
                    continue
                    
                batch_num = processed_batches + 1
                logger.info(f"Processing streaming batch {batch_num}: {len(batch_files)} files")
                
                # Process this batch
                self._process_streaming_batch(batch_files, batch_num)
                
                # Clean up extracted files immediately
                self._cleanup_batch_files(batch_files)
                
                # Clear GPU memory between batches
                if self.device == "cuda":
                    torch.cuda.empty_cache()
                    gc.collect()
                
                processed_batches += 1
                
                # Log progress
                if processed_batches % 5 == 0:
                    stats = self.storage.get_stats()
                    logger.info(f"Progress: {processed_batches} batches, {self.processed_count} processed, "
                              f"uploads: {stats['completed']}/{stats['queued']}")
            
            # Wait for all uploads to complete
            logger.info("Processing complete, waiting for uploads to finish...")
            final_stats = self.storage.wait_for_completion(timeout=600)  # 10 minute timeout
            
            logger.info(f"Streaming processing completed: {self.processed_count} processed, "
                       f"{self.failed_count} failed, Upload stats: {final_stats}")
            
        except Exception as e:
            logger.error(f"Streaming processing failed: {e}")
            raise
        finally:
            # Cleanup and shutdown
            self._cleanup_work_directory()
            self.storage.shutdown()
    
    def _count_mp3_members(self) -> int:
        """Count MP3 files in tar without extracting"""
        try:
            with tarfile.open(self.tar_file, 'r:*') as tar:
                return len([m for m in tar.getmembers() if m.name.endswith('.mp3')])
        except Exception as e:
            logger.error(f"Failed to count members: {e}")
            return 0
    
    def _stream_extract_batches(self) -> Iterator[List[Path]]:
        """Generator that yields batches of extracted files"""
        try:
            with tarfile.open(self.tar_file, 'r:*') as tar:
                mp3_members = [m for m in tar.getmembers() if m.name.endswith('.mp3')]
                
                # Process in batches to avoid memory issues
                for i in range(0, len(mp3_members), self.stream_batch_size):
                    batch_members = mp3_members[i:i + self.stream_batch_size]
                    
                    # Extract this batch to temporary directory
                    batch_files = []
                    batch_dir = self.stream_work_dir / f"batch_{i // self.stream_batch_size}"
                    batch_dir.mkdir(exist_ok=True)
                    
                    try:
                        for member in batch_members:
                            # Extract individual file
                            tar.extract(member, batch_dir)
                            extracted_path = batch_dir / member.name
                            
                            # Ensure the file was extracted successfully
                            if extracted_path.exists():
                                batch_files.append(extracted_path)
                            else:
                                logger.warning(f"Failed to extract {member.name}")
                        
                        if batch_files:
                            yield batch_files
                        else:
                            # Clean up empty batch directory
                            shutil.rmtree(batch_dir, ignore_errors=True)
                            
                    except Exception as e:
                        logger.error(f"Failed to extract batch starting at {i}: {e}")
                        # Clean up partial batch
                        shutil.rmtree(batch_dir, ignore_errors=True)
                        continue
                        
        except Exception as e:
            logger.error(f"Failed to stream extract from tar: {e}")
            return
    
    def _process_streaming_batch(self, audio_files: List[Path], batch_num: int):
        """Process a batch of extracted audio files"""
        logger.debug(f"Processing batch {batch_num} with {len(audio_files)} files")
        
        # Process files in smaller sub-batches for better resource management
        for i in range(0, len(audio_files), self.process_batch_size):
            sub_batch = audio_files[i:i + self.process_batch_size]
            self._process_audio_sub_batch(sub_batch, batch_num, i // self.process_batch_size)
    
    def _process_audio_sub_batch(self, audio_paths: List[Path], batch_num: int, sub_batch_num: int):
        """Process a sub-batch of audio files"""
        logger.debug(f"Processing sub-batch {batch_num}.{sub_batch_num} with {len(audio_paths)} files")
        
        # Convert MP3 files to Opus format in parallel
        opus_paths = self._batch_convert_to_opus(audio_paths)
        
        if not opus_paths:
            logger.warning(f"No successful conversions in sub-batch {batch_num}.{sub_batch_num}")
            return
        
        # Batch transcription on GPU
        transcripts = self._batch_transcribe_gpu([p[1] for p in opus_paths])
        
        # Store results in database and queue uploads
        batch_processed = 0
        batch_failed = 0
        
        with self.db.cursor() as cur:
            for (orig_path, opus_path), transcript in zip(opus_paths, transcripts):
                try:
                    # Extract original filename info
                    orig_filename = orig_path.name
                    audio_id = f"{self.date_str}_{self.timestamp}_{orig_filename}"
                    
                    # Store in database
                    cur.execute("""
                        INSERT INTO audio_files 
                        (filename, file_path, year, month, date, created_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        RETURNING id
                    """, (orig_filename, None, self.year, self.month, self.day))
                    
                    db_audio_id = cur.fetchone()[0]
                    
                    # Store transcript
                    cur.execute("""
                        INSERT INTO transcripts 
                        (audio_file_id, transcript_text, duration_seconds)
                        VALUES (%s, %s, %s)
                    """, (db_audio_id, transcript['transcript'], 
                          transcript.get('duration', 0)))
                    
                    # Queue async upload (non-blocking)
                    storage_path = self.storage.get_storage_path(
                        self.year, self.month, self.day, self.timestamp, opus_path.name
                    )
                    
                    if self.storage.queue_upload(opus_path, storage_path, str(db_audio_id)):
                        # Update database with pending upload (will be updated when upload completes)
                        # For now, we'll update it immediately with the path
                        cur.execute("""
                            UPDATE audio_files 
                            SET file_path = %s 
                            WHERE id = %s
                        """, (storage_path, db_audio_id))
                        
                        batch_processed += 1
                        self.upload_count += 1
                    else:
                        logger.error(f"Failed to queue upload for {orig_filename}")
                        batch_failed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to store {orig_filename}: {e}")
                    batch_failed += 1
            
            # Commit all database changes for this sub-batch
            self.db.commit()
        
        # Update counters
        self.processed_count += batch_processed
        self.failed_count += batch_failed
        
        logger.debug(f"Sub-batch {batch_num}.{sub_batch_num} completed: "
                    f"{batch_processed} processed, {batch_failed} failed")
    
    def _batch_convert_to_opus(self, audio_paths: List[Path]) -> List[Tuple[Path, Path]]:
        """Convert batch of MP3 files to Opus format in parallel"""
        with ProcessPoolExecutor(max_workers=min(self.num_workers, len(audio_paths))) as executor:
            results = list(executor.map(self._convert_to_opus, audio_paths))
        
        # Filter successful conversions
        successful_conversions = [(orig, opus) for orig, opus in results if opus is not None]
        
        failed_count = len(results) - len(successful_conversions)
        if failed_count > 0:
            logger.warning(f"{failed_count} audio conversions failed")
            
        return successful_conversions
    
    @staticmethod
    def _convert_to_opus(mp3_path: Path) -> Tuple[Path, Path]:
        """Convert MP3 to Opus format - same settings as original"""
        try:
            opus_path = mp3_path.with_suffix('.opus')
            
            cmd = [
                'ffmpeg', '-i', str(mp3_path),
                '-c:a', 'libopus',
                '-b:a', '32k',
                '-application', 'voip',
                '-vbr', 'on',
                '-compression_level', '5',
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
    
    def _transcribe_audio_file(self, audio_path: Path) -> Dict:
        """Transcribe a single audio file using WhisperX"""
        try:
            # Load audio
            audio = whisperx.load_audio(str(audio_path))
            
            # Transcribe
            result = self.model.transcribe(audio)
            
            # Extract transcript
            transcript_text = ' '.join([s['text'].strip() for s in result.get('segments', [])])
            
            return {
                'transcript': transcript_text,
                'duration': len(audio) / 16000  # Assuming 16kHz
            }
            
        except Exception as e:
            logger.error(f"Transcription error for {audio_path.name}: {e}")
            return {
                'transcript': '',
                'duration': 0
            }
    
    def _batch_transcribe_gpu(self, audio_paths: List[Path]) -> List[Dict]:
        """Transcribe batch of audio files on GPU"""
        results = []
        
        for audio_path in audio_paths:
            result = self._transcribe_audio_file(audio_path)
            results.append(result)
        
        return results
    
    def _cleanup_batch_files(self, batch_files: List[Path]):
        """Clean up extracted files from a batch"""
        for file_path in batch_files:
            try:
                # Remove the actual audio files (both mp3 and opus)
                file_path.unlink(missing_ok=True)
                
                # Also remove opus file if it exists
                opus_path = file_path.with_suffix('.opus')
                opus_path.unlink(missing_ok=True)
                
                # Remove parent directory if empty
                parent_dir = file_path.parent
                if parent_dir != self.stream_work_dir and parent_dir.exists():
                    try:
                        parent_dir.rmdir()  # Only removes if empty
                    except OSError:
                        pass  # Directory not empty, that's fine
                        
            except Exception as e:
                logger.debug(f"Failed to cleanup {file_path}: {e}")
    
    def _cleanup_work_directory(self):
        """Clean up the entire streaming work directory"""
        try:
            if self.stream_work_dir.exists():
                shutil.rmtree(self.stream_work_dir)
                logger.debug(f"Cleaned up work directory: {self.stream_work_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup work directory: {e}")


def main():
    parser = argparse.ArgumentParser(description='Stream process a single tar.xz file with async uploads')
    parser.add_argument('--date', required=True, help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--tar-file', required=True, help='Path to tar.xz file to process')
    parser.add_argument('--timestamp', required=True, help='Timestamp (HH_MM)')
    parser.add_argument('--temp-dir', required=True, help='Temporary directory path')
    parser.add_argument('--db-host', required=True, help='Database host (also rsync target)')
    parser.add_argument('--db-password', default='audio_password', help='Database password')
    
    # Streaming configuration
    parser.add_argument('--stream-batch-size', type=int, default=100, 
                       help='Files to extract from tar at once')
    parser.add_argument('--process-batch-size', type=int, default=50, 
                       help='Files to process in parallel within a stream batch')
    parser.add_argument('--num-workers', type=int, default=8, help='Parallel workers for conversion')
    
    # Async upload configuration
    parser.add_argument('--max-concurrent-uploads', type=int, default=6, 
                       help='Maximum concurrent upload threads')
    parser.add_argument('--upload-timeout', type=int, default=180, 
                       help='Upload timeout in seconds')
    
    # Storage options
    parser.add_argument('--rsync-user', default='audio_user', help='Username for rsync transfers')
    parser.add_argument('--storage-root', default='/opt/audio_storage', help='Root directory on target server')
    parser.add_argument('--use-dummy-storage', action='store_true', help='Use dummy storage (no actual transfers)')
    parser.add_argument('--ssh_keyfile', default='ent.pem', help='SSH identity file for cloud storage')
    
    args = parser.parse_args()
    
    processor = StreamingTarProcessor(args)
    processor.process_tar_file_streaming()
    
    logger.info(f"Streaming processing completed. Total: {processor.processed_count} processed, "
                f"{processor.failed_count} failed, {processor.upload_count} uploads queued")


if __name__ == '__main__':
    main()