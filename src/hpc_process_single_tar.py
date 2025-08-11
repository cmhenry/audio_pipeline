#!/usr/bin/env python3
# hpc_process_single_tar.py - Process individual tar.xz files for parallel processing
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
from typing import List, Dict, Tuple

from storage_manager import create_storage_manager

torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SingleTarProcessor:
    def __init__(self, args):
        self.date_str = args.date
        self.year, self.month, self.day = map(int, self.date_str.split('-'))
        self.tar_file = Path(args.tar_file)
        self.timestamp = args.timestamp
        self.temp_dir = Path(args.temp_dir)
        self.batch_size = args.batch_size
        self.num_workers = args.num_workers
        
        # Initialize database connection
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
        
    def process_tar_file(self):
        """Process a single tar.xz file"""
        logger.info(f"Processing tar file: {self.tar_file.name}")
        logger.info(f"Timestamp: {self.timestamp}, Date: {self.date_str}")
        
        if not self.tar_file.exists():
            raise FileNotFoundError(f"Tar file not found: {self.tar_file}")
        
        # Create working directory
        work_dir = self.temp_dir / f"{self.date_str}_{self.timestamp}"
        work_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Extract audio files from tar
            audio_files = self.extract_audio_files_from_tar(self.tar_file, work_dir)
            
            if not audio_files:
                logger.warning(f"No audio files found in {self.tar_file.name}")
                return
            
            logger.info(f"Extracted {len(audio_files)} audio files")
            
            # Process in batches
            for i in range(0, len(audio_files), self.batch_size):
                batch_files = audio_files[i:i + self.batch_size]
                logger.info(f"Processing batch {i//self.batch_size + 1}: {len(batch_files)} files")
                
                self.process_audio_batch(batch_files, self.timestamp, i // self.batch_size)
                
                # Clear GPU memory between batches
                if self.device == "cuda":
                    torch.cuda.empty_cache()
                    gc.collect()
            
            logger.info(f"Completed processing {self.tar_file.name}: {self.processed_count} processed, {self.failed_count} failed")
            
        except Exception as e:
            logger.error(f"Failed to process {self.tar_file.name}: {e}")
            raise
        finally:
            # Clean up working directory
            import shutil
            if work_dir.exists():
                shutil.rmtree(work_dir)
    
    def extract_audio_files_from_tar(self, tar_path: Path, work_dir: Path) -> List[Path]:
        """Extract MP3 files from tar.xz archive and return their paths"""
        audio_files = []
        try:
            with tarfile.open(tar_path, 'r:*') as tar:
                members = [m for m in tar.getmembers() if m.name.endswith('.mp3')]
                logger.info(f"Found {len(members)} MP3 files in {tar_path.name}")
                
                # Extract all audio files
                for member in members:
                    tar.extract(member, work_dir)
                    audio_files.append(work_dir / member.name)
                    
        except Exception as e:
            logger.error(f"Failed to extract from {tar_path}: {e}")
            raise
            
        return audio_files
    
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
    
    def batch_convert_to_opus(self, audio_paths: List[Path]) -> List[Tuple[Path, Path]]:
        """Convert batch of MP3 files to Opus format in parallel"""
        with ProcessPoolExecutor(max_workers=min(self.num_workers, len(audio_paths))) as executor:
            results = list(executor.map(self.convert_to_opus, audio_paths))
        
        # Filter successful conversions
        successful_conversions = [(orig, opus) for orig, opus in results if opus is not None]
        
        failed_count = len(results) - len(successful_conversions)
        if failed_count > 0:
            logger.warning(f"{failed_count} audio conversions failed")
            
        return successful_conversions
    
    def transcribe_audio_file(self, audio_path: Path) -> Dict:
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
    
    def batch_transcribe_gpu(self, audio_paths: List[Path]) -> List[Dict]:
        """Transcribe batch of audio files on GPU"""
        results = []
        
        for audio_path in audio_paths:
            result = self.transcribe_audio_file(audio_path)
            results.append(result)
        
        return results
    
    def process_audio_batch(self, audio_paths: List[Path], timestamp: str, batch_num: int):
        """Process a batch of audio files"""
        logger.info(f"Processing batch {batch_num} with {len(audio_paths)} files")
        
        # Convert MP3 files to Opus format in parallel
        opus_paths = self.batch_convert_to_opus(audio_paths)
        
        if not opus_paths:
            logger.warning(f"No successful conversions in batch {batch_num}")
            return
        
        logger.info(f"Successfully converted {len(opus_paths)} files to Opus")
        
        # Batch transcription on GPU
        transcripts = self.batch_transcribe_gpu([p[1] for p in opus_paths])
        
        # Store results in database
        batch_processed = 0
        batch_failed = 0
        
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
                        VALUES (%s, %s, %s)
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
                    
                    batch_processed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to store {orig_filename}: {e}")
                    batch_failed += 1
            
            # Commit all changes for this batch
            self.db.commit()
        
        # Update counters
        self.processed_count += batch_processed
        self.failed_count += batch_failed
        
        logger.info(f"Batch {batch_num} completed: {batch_processed} processed, {batch_failed} failed")
        
        # Clean up audio files immediately
        for orig_path, opus_path in opus_paths:
            orig_path.unlink(missing_ok=True)
            opus_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description='Process a single tar.xz file')
    parser.add_argument('--date', required=True, help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--tar-file', required=True, help='Path to tar.xz file to process')
    parser.add_argument('--timestamp', required=True, help='Timestamp (HH_MM)')
    parser.add_argument('--temp-dir', required=True, help='Temporary directory path')
    parser.add_argument('--db-host', required=True, help='Database host (also rsync target)')
    parser.add_argument('--db-password', default='audio_password', help='Database password')
    parser.add_argument('--batch-size', type=int, default=500, help='Audio files per batch')
    parser.add_argument('--num-workers', type=int, default=8, help='Parallel workers')
    
    # Storage options
    parser.add_argument('--rsync-user', default='audio_user', help='Username for rsync transfers')
    parser.add_argument('--storage-root', default='/opt/audio_storage', help='Root directory on target server')
    parser.add_argument('--use-dummy-storage', action='store_true', help='Use dummy storage (no actual transfers)')
    parser.add_argument('--ssh_keyfile', default='ent.pem', help='SSH identity file for cloud storage')
    
    args = parser.parse_args()
    
    processor = SingleTarProcessor(args)
    processor.process_tar_file()
    
    logger.info(f"Processing completed. Total: {processor.processed_count} processed, {processor.failed_count} failed")


if __name__ == '__main__':
    main()