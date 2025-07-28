#!/usr/bin/env python3
"""
storage_manager.py - Storage management for audio files using rsync
Replaces cloud storage with rsync-based file transfer to database host
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import time

logger = logging.getLogger(__name__)


class RsyncStorageManager:
    """Manages audio file storage using rsync to database server"""
    
    def __init__(self, db_host: str, storage_root: str = "/opt/audio_storage", 
                 rsync_user: str = "audio_user", max_retries: int = 3):
        """
        Initialize rsync storage manager
        
        Args:
            db_host: Database server hostname/IP (target for rsync)
            storage_root: Root directory on target server for audio files
            rsync_user: Username for rsync connections
            max_retries: Maximum retry attempts for failed transfers
        """
        self.db_host = db_host
        self.storage_root = storage_root
        self.rsync_user = rsync_user
        self.max_retries = max_retries
        
        # Rsync options for optimized transfer
        self.rsync_options = [
            '--archive',           # Archive mode (preserves permissions, timestamps)
            '--compress',          # Compress during transfer
            '--partial',           # Keep partial transfers
            '--partial-dir=.rsync-partial',  # Partial transfer directory
            '--timeout=600',       # 10 minute timeout
            '--contimeout=30',     # 30 second connection timeout
            '--quiet'              # Reduce output
        ]
        
        self._test_connection()
    
    def _test_connection(self):
        """Test rsync connection to target server"""
        try:
            cmd = ['rsync'] + self.rsync_options + [
                f"{self.rsync_user}@{self.db_host}:{self.storage_root}/",
                '/tmp/rsync_test_dummy'
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"Rsync connection to {self.db_host} successful")
            else:
                logger.warning(f"Rsync connection test failed: {result.stderr}")
                
        except Exception as e:
            logger.warning(f"Could not test rsync connection: {e}")
    
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """
        Upload single file using rsync
        
        Args:
            local_path: Local file path
            remote_path: Remote path relative to storage_root
            
        Returns:
            bool: True if upload successful
        """
        if not local_path.exists():
            logger.error(f"Local file does not exist: {local_path}")
            return False
        
        # Construct full remote path
        full_remote_path = f"{self.rsync_user}@{self.db_host}:{self.storage_root}/{remote_path}"
        
        # Ensure remote directory exists
        remote_dir = str(Path(remote_path).parent)
        if not self._ensure_remote_directory(remote_dir):
            logger.error(f"Failed to create remote directory: {remote_dir}")
            return False
        
        # Perform rsync transfer with retries
        for attempt in range(self.max_retries):
            try:
                cmd = ['rsync'] + self.rsync_options + [
                    str(local_path),
                    full_remote_path
                ]
                
                logger.debug(f"Rsync command: {' '.join(cmd)}")
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 minute timeout
                )
                
                if result.returncode == 0:
                    logger.info(f"Uploaded {local_path.name} to {remote_path}")
                    return True
                else:
                    logger.warning(f"Rsync attempt {attempt + 1} failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                logger.warning(f"Rsync timeout on attempt {attempt + 1} for {local_path.name}")
            except Exception as e:
                logger.warning(f"Rsync error on attempt {attempt + 1}: {e}")
            
            # Wait before retry
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"Failed to upload {local_path.name} after {self.max_retries} attempts")
        return False
    
    def upload_batch(self, file_pairs: List[Tuple[Path, str]], max_workers: int = 4) -> List[bool]:
        """
        Upload multiple files in parallel
        
        Args:
            file_pairs: List of (local_path, remote_path) tuples
            max_workers: Maximum parallel transfers
            
        Returns:
            List[bool]: Success status for each file
        """
        if not file_pairs:
            return []
        
        logger.info(f"Starting batch upload of {len(file_pairs)} files with {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.upload_file, local_path, remote_path)
                for local_path, remote_path in file_pairs
            ]
            
            results = [future.result() for future in futures]
        
        successful = sum(results)
        logger.info(f"Batch upload complete: {successful}/{len(file_pairs)} successful")
        
        return results
    
    def _ensure_remote_directory(self, remote_dir: str) -> bool:
        """Ensure remote directory exists"""
        if remote_dir in ['', '.']:
            return True
            
        try:
            # Use ssh to create directory
            cmd = [
                'ssh',
                f"{self.rsync_user}@{self.db_host}",
                f"mkdir -p {self.storage_root}/{remote_dir}"
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.debug(f"Created remote directory: {remote_dir}")
                return True
            else:
                logger.error(f"Failed to create remote directory {remote_dir}: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating remote directory {remote_dir}: {e}")
            return False
    
    def get_storage_path(self, year: int, month: int, day: int, timestamp: str, filename: str) -> str:
        """
        Generate standardized storage path
        
        Args:
            year: Year
            month: Month
            day: Day 
            timestamp: Timestamp (HH_MM format)
            filename: Audio filename
            
        Returns:
            str: Relative path for storage
        """
        return f"audio/{year}/{month:02d}/{day:02d}/{timestamp}/{filename}"
    
    def verify_upload(self, remote_path: str) -> bool:
        """
        Verify file exists on remote server
        
        Args:
            remote_path: Remote path relative to storage_root
            
        Returns:
            bool: True if file exists
        """
        try:
            cmd = [
                'ssh',
                f"{self.rsync_user}@{self.db_host}",
                f"test -f {self.storage_root}/{remote_path}"
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=30
            )
            
            return result.returncode == 0
            
        except Exception as e:
            logger.error(f"Error verifying upload {remote_path}: {e}")
            return False
    
    def get_remote_file_size(self, remote_path: str) -> Optional[int]:
        """
        Get size of remote file
        
        Args:
            remote_path: Remote path relative to storage_root
            
        Returns:
            Optional[int]: File size in bytes, None if error
        """
        try:
            cmd = [
                'ssh',
                f"{self.rsync_user}@{self.db_host}",
                f"stat -c %s {self.storage_root}/{remote_path}"
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return int(result.stdout.strip())
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error getting remote file size {remote_path}: {e}")
            return None
    
    def cleanup_failed_transfers(self):
        """Clean up any partial transfer files on remote server"""
        try:
            cmd = [
                'ssh',
                f"{self.rsync_user}@{self.db_host}",
                f"find {self.storage_root} -name '.rsync-partial' -type d -exec rm -rf {{}} + 2>/dev/null || true"
            ]
            
            subprocess.run(cmd, timeout=60)
            logger.info("Cleaned up partial transfer files")
            
        except Exception as e:
            logger.warning(f"Could not clean up partial transfers: {e}")


class DummyStorageManager:
    """Dummy storage manager for testing without actual transfers"""
    
    def __init__(self, db_host: str, **kwargs):
        self.db_host = db_host
        logger.info(f"Using dummy storage manager (no actual transfers to {db_host})")
    
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        logger.info(f"DUMMY: Would upload {local_path.name} to {remote_path}")
        return True
    
    def upload_batch(self, file_pairs: List[Tuple[Path, str]], max_workers: int = 4) -> List[bool]:
        logger.info(f"DUMMY: Would upload {len(file_pairs)} files")
        return [True] * len(file_pairs)
    
    def get_storage_path(self, year: int, month: int, day: int, timestamp: str, filename: str) -> str:
        return f"audio/{year}/{month:02d}/{day:02d}/{timestamp}/{filename}"
    
    def verify_upload(self, remote_path: str) -> bool:
        return True
    
    def cleanup_failed_transfers(self):
        pass


def create_storage_manager(db_host: str, use_dummy: bool = False, **kwargs) -> RsyncStorageManager:
    """
    Factory function to create storage manager
    
    Args:
        db_host: Database server hostname/IP
        use_dummy: Use dummy manager for testing
        **kwargs: Additional arguments for RsyncStorageManager
        
    Returns:
        Storage manager instance
    """
    if use_dummy:
        return DummyStorageManager(db_host, **kwargs)
    else:
        return RsyncStorageManager(db_host, **kwargs)