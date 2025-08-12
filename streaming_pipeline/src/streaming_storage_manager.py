#!/usr/bin/env python3
"""
streaming_storage_manager.py - Asynchronous storage management for streaming pipeline
Extends the existing storage manager with async upload capabilities
"""

import os
import asyncio
import subprocess
import logging
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import time
import queue
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class UploadTask:
    """Represents a file upload task"""
    local_path: Path
    remote_path: str
    audio_id: str
    timestamp: float
    retries: int = 0

class AsyncRsyncStorageManager:
    """Asynchronous storage manager with upload queuing"""
    
    def __init__(self, db_host: str, storage_root: str = "/mnt/storage/audio_storage", 
                 rsync_user: str = "audio_user", ssh_key_path: Optional[str] = None,
                 max_retries: int = 3, max_concurrent_uploads: int = 8,
                 upload_timeout: int = 300):
        """
        Initialize async rsync storage manager
        
        Args:
            db_host: Database server hostname/IP (target for rsync)
            storage_root: Root directory on target server for audio files
            rsync_user: Username for rsync connections
            ssh_key_path: Path to SSH identity file (optional)
            max_retries: Maximum retry attempts for failed transfers
            max_concurrent_uploads: Maximum concurrent upload threads
            upload_timeout: Timeout for individual uploads in seconds
        """
        self.db_host = db_host
        self.storage_root = storage_root
        self.rsync_user = rsync_user
        self.ssh_key_path = ssh_key_path
        self.max_retries = max_retries
        self.upload_timeout = upload_timeout
        
        # Build SSH command with options
        ssh_options = []
        if self.ssh_key_path:
            ssh_options.extend(['-i', os.path.expanduser(self.ssh_key_path)])
        ssh_options.extend([
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'BatchMode=yes',
            '-o', 'ConnectTimeout=30'  # Faster connection timeout
        ])
        self.ssh_cmd = 'ssh ' + ' '.join(ssh_options)
        
        # Rsync options optimized for async operations
        self.rsync_options = [
            '-e', self.ssh_cmd,
            '--archive',
            '--compress',
            '--partial',
            '--partial-dir=.rsync-partial',
            f'--timeout={upload_timeout}',
            '--quiet',
            '--no-whole-file',  # Use delta transfer for better performance
            '--compress-level=6'  # Balanced compression
        ]
        
        # Async upload management
        self.upload_queue = queue.Queue()
        self.upload_results = {}  # audio_id -> (success, error_message)
        self.upload_executor = ThreadPoolExecutor(max_workers=max_concurrent_uploads, 
                                                thread_name_prefix="upload")
        self.active_uploads = {}  # audio_id -> Future
        self.upload_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'queued': 0,
            'completed': 0,
            'failed': 0,
            'retries': 0
        }
        
        self._test_connection()
        logger.info(f"Async storage manager initialized with {max_concurrent_uploads} upload threads")
    
    def _test_connection(self):
        """Test rsync connection to target server"""
        try:
            # Quick SSH test
            ssh_test_cmd = self._build_ssh_command(['echo', 'connection_test'])
            
            result = subprocess.run(
                ssh_test_cmd,
                capture_output=True,
                text=True,
                timeout=15  # Shorter timeout for responsiveness
            )
            
            if result.returncode == 0:
                logger.info(f"SSH connection to {self.db_host} successful")
            else:
                logger.warning(f"SSH connection test failed: {result.stderr}")
                
        except Exception as e:
            logger.warning(f"Could not test connection: {e}")
    
    def _build_ssh_command(self, remote_cmd: List[str]) -> List[str]:
        """Build SSH command with identity file"""
        cmd = ['ssh']
        if self.ssh_key_path:
            cmd.extend(['-i', os.path.expanduser(self.ssh_key_path)])
        cmd.extend([
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'BatchMode=yes',
            '-o', 'ConnectTimeout=30',
            f"{self.rsync_user}@{self.db_host}"
        ])
        cmd.extend(remote_cmd)
        return cmd
    
    def queue_upload(self, local_path: Path, remote_path: str, audio_id: str) -> bool:
        """
        Queue a file for asynchronous upload
        
        Args:
            local_path: Local file path
            remote_path: Remote path relative to storage_root
            audio_id: Unique identifier for tracking this upload
            
        Returns:
            bool: True if successfully queued
        """
        if not local_path.exists():
            logger.error(f"Cannot queue non-existent file: {local_path}")
            return False
            
        task = UploadTask(
            local_path=local_path,
            remote_path=remote_path,
            audio_id=audio_id,
            timestamp=time.time()
        )
        
        with self.upload_lock:
            # Submit upload task
            future = self.upload_executor.submit(self._execute_upload, task)
            self.active_uploads[audio_id] = future
            self.stats['queued'] += 1
            
        logger.debug(f"Queued upload for {audio_id}: {local_path.name} -> {remote_path}")
        return True
    
    def _execute_upload(self, task: UploadTask) -> Tuple[bool, Optional[str]]:
        """Execute a single upload task with retries"""
        for attempt in range(self.max_retries):
            try:
                success, error_msg = self._perform_rsync(task.local_path, task.remote_path)
                
                if success:
                    with self.upload_lock:
                        self.upload_results[task.audio_id] = (True, None)
                        self.stats['completed'] += 1
                        if task.audio_id in self.active_uploads:
                            del self.active_uploads[task.audio_id]
                    
                    logger.debug(f"Upload successful for {task.audio_id}: {task.local_path.name}")
                    return True, None
                else:
                    if attempt < self.max_retries - 1:
                        retry_delay = min(2 ** attempt, 30)  # Cap at 30 seconds
                        logger.warning(f"Upload attempt {attempt + 1} failed for {task.audio_id}: {error_msg}, retrying in {retry_delay}s")
                        time.sleep(retry_delay)
                        with self.upload_lock:
                            self.stats['retries'] += 1
                    else:
                        logger.error(f"Upload failed after {self.max_retries} attempts for {task.audio_id}: {error_msg}")
                        
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Upload exception for {task.audio_id} on attempt {attempt + 1}: {error_msg}")
        
        # All attempts failed
        with self.upload_lock:
            self.upload_results[task.audio_id] = (False, error_msg)
            self.stats['failed'] += 1
            if task.audio_id in self.active_uploads:
                del self.active_uploads[task.audio_id]
        
        return False, error_msg
    
    def _perform_rsync(self, local_path: Path, remote_path: str) -> Tuple[bool, Optional[str]]:
        """Perform the actual rsync operation"""
        # Ensure remote directory exists
        remote_dir = str(Path(remote_path).parent)
        if not self._ensure_remote_directory(remote_dir):
            return False, f"Failed to create remote directory: {remote_dir}"
        
        # Construct full remote path
        full_remote_path = f"{self.rsync_user}@{self.db_host}:{self.storage_root}/{remote_path}"
        
        # Execute rsync
        cmd = ['rsync'] + self.rsync_options + [
            str(local_path),
            full_remote_path
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.upload_timeout
            )
            
            if result.returncode == 0:
                return True, None
            else:
                return False, result.stderr.strip()
                
        except subprocess.TimeoutExpired:
            return False, f"Upload timeout after {self.upload_timeout}s"
        except Exception as e:
            return False, str(e)
    
    def _ensure_remote_directory(self, remote_dir: str) -> bool:
        """Ensure remote directory exists"""
        if remote_dir in ['', '.']:
            return True
            
        try:
            cmd = self._build_ssh_command([f"mkdir -p {self.storage_root}/{remote_dir}"])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            return result.returncode == 0
            
        except Exception as e:
            logger.error(f"Error creating remote directory {remote_dir}: {e}")
            return False
    
    def get_storage_path(self, year: int, month: int, day: int, timestamp: str, filename: str) -> str:
        """Generate standardized storage path"""
        return f"audio/{year}/{month:02d}/{day:02d}/{timestamp}/{filename}"
    
    def check_upload_status(self, audio_id: str) -> Tuple[str, Optional[str]]:
        """
        Check status of an upload
        
        Args:
            audio_id: Audio ID to check
            
        Returns:
            Tuple of (status, error_message) where status is:
            - 'pending': Upload is queued/in progress
            - 'completed': Upload successful
            - 'failed': Upload failed
            - 'not_found': Audio ID not found
        """
        with self.upload_lock:
            if audio_id in self.upload_results:
                success, error_msg = self.upload_results[audio_id]
                return 'completed' if success else 'failed', error_msg
            elif audio_id in self.active_uploads:
                return 'pending', None
            else:
                return 'not_found', None
    
    def wait_for_completion(self, timeout: Optional[int] = None) -> Dict[str, int]:
        """
        Wait for all queued uploads to complete
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Dict with completion statistics
        """
        logger.info("Waiting for all uploads to complete...")
        start_time = time.time()
        
        while True:
            with self.upload_lock:
                active_count = len(self.active_uploads)
                if active_count == 0:
                    break
                
                # Log progress periodically
                elapsed = time.time() - start_time
                if elapsed > 30 and int(elapsed) % 30 == 0:  # Every 30 seconds
                    logger.info(f"Still waiting for {active_count} uploads, elapsed: {elapsed:.1f}s")
            
            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                logger.warning(f"Upload completion timeout after {timeout}s")
                break
                
            time.sleep(1)
        
        # Cancel any remaining uploads
        with self.upload_lock:
            for audio_id, future in list(self.active_uploads.items()):
                future.cancel()
                self.upload_results[audio_id] = (False, "Cancelled due to timeout")
            self.active_uploads.clear()
        
        return self.get_stats()
    
    def get_stats(self) -> Dict[str, int]:
        """Get upload statistics"""
        with self.upload_lock:
            return {
                'queued': self.stats['queued'],
                'completed': self.stats['completed'],
                'failed': self.stats['failed'],
                'retries': self.stats['retries'],
                'pending': len(self.active_uploads)
            }
    
    def cleanup_completed_results(self, max_age_seconds: int = 3600):
        """Clean up old completed upload results"""
        current_time = time.time()
        with self.upload_lock:
            # This is a simplified cleanup - in production you'd want to track timestamps
            if len(self.upload_results) > 10000:  # Prevent memory bloat
                # Keep only recent results
                self.upload_results.clear()
                logger.info("Cleaned up old upload results")
    
    def shutdown(self):
        """Shutdown the storage manager and wait for uploads to complete"""
        logger.info("Shutting down async storage manager...")
        
        # Wait for uploads to complete (with timeout)
        stats = self.wait_for_completion(timeout=300)  # 5 minute timeout
        
        # Shutdown executor
        self.upload_executor.shutdown(wait=True)
        
        logger.info(f"Storage manager shutdown complete. Final stats: {stats}")


class AsyncDummyStorageManager:
    """Dummy async storage manager for testing"""
    
    def __init__(self, db_host: str, **kwargs):
        self.db_host = db_host
        self.upload_results = {}
        logger.info(f"Using async dummy storage manager (no actual transfers to {db_host})")
    
    def queue_upload(self, local_path: Path, remote_path: str, audio_id: str) -> bool:
        logger.debug(f"DUMMY: Queued upload {audio_id}: {local_path.name} -> {remote_path}")
        self.upload_results[audio_id] = (True, None)
        return True
    
    def check_upload_status(self, audio_id: str) -> Tuple[str, Optional[str]]:
        if audio_id in self.upload_results:
            return 'completed', None
        return 'not_found', None
    
    def wait_for_completion(self, timeout: Optional[int] = None) -> Dict[str, int]:
        return {'queued': 0, 'completed': len(self.upload_results), 'failed': 0, 'retries': 0, 'pending': 0}
    
    def get_storage_path(self, year: int, month: int, day: int, timestamp: str, filename: str) -> str:
        return f"audio/{year}/{month:02d}/{day:02d}/{timestamp}/{filename}"
    
    def get_stats(self) -> Dict[str, int]:
        return {'queued': 0, 'completed': len(self.upload_results), 'failed': 0, 'retries': 0, 'pending': 0}
    
    def shutdown(self):
        logger.info("Dummy storage manager shutdown")


def create_async_storage_manager(db_host: str, use_dummy: bool = False, **kwargs):
    """
    Factory function to create async storage manager
    
    Args:
        db_host: Database server hostname/IP
        use_dummy: Use dummy manager for testing
        **kwargs: Additional arguments for AsyncRsyncStorageManager
        
    Returns:
        Async storage manager instance
    """
    if use_dummy:
        return AsyncDummyStorageManager(db_host, **kwargs)
    else:
        return AsyncRsyncStorageManager(db_host, **kwargs)