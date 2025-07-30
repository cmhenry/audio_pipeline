#!/usr/bin/env python3
"""
globus_flow_manager.py - Manage Globus transfers for audio pipeline
Uses TransferClient directly with refresh tokens for authentication
"""

import json
import os
import sys
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer, TransferClient, TransferData


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GlobusTransferManager:
    """Manages Globus transfers for audio pipeline using TransferClient directly"""
    
    def __init__(self, token_file: str = "~/.globus_refresh_tokens.json"):
        """
        Initialize Globus Transfer manager using refresh tokens
        
        Args:
            token_file: Path to JSON file containing refresh tokens
        """
        # Load refresh tokens
        token_path = os.path.expanduser(token_file)
        try:
            with open(token_path, 'r') as f:
                tokens = json.load(f)
        except FileNotFoundError:
            raise ValueError(
                f"Token file not found: {token_path}\n"
                "Run the one-time setup script to generate refresh tokens first."
            )
        
        # Extract tokens
        self.client_id = tokens['CLIENT_ID']
        transfer_refresh_token = tokens.get('TRANSFER_REFRESH_TOKEN')
        
        if not transfer_refresh_token:
            raise ValueError(
                "No TRANSFER_REFRESH_TOKEN found in token file.\n"
                "Please regenerate tokens with transfer scope."
            )
        
        # Create native app auth client
        auth_client = NativeAppAuthClient(self.client_id)
        
        # Create refresh token authorizer for Transfer API
        transfer_authorizer = RefreshTokenAuthorizer(
            transfer_refresh_token,
            auth_client
        )
        
        # Initialize Transfer client
        self.transfer_client = TransferClient(authorizer=transfer_authorizer)
        
        logger.info("Initialized Globus Transfer Manager with refresh tokens")
    
    def list_and_filter_files(self, endpoint_id: str, path: str, date_str: str) -> List[Dict]:
        """
        List files and filter for date-specific audio files
        
        Args:
            endpoint_id: Globus endpoint UUID
            path: Path to list files from
            date_str: Date string (YYYY-MM-DD) to filter files
            
        Returns:
            List of file dictionaries that match criteria
        """
        filtered_files = []
        
        try:
            
            ls_result = self.transfer_client.operation_ls(
                endpoint_id,
                path=path
            )
            
            # Filter for date
            logger.info(f"Filtering for {date_str}")
            for item in ls_result:
                if item['name'].contains(date_str):
                    filtered_files.append(item)

            # Filter for .tar.xz and .parquet files
            # for item in ls_result:
            #     if item['type'] == 'file' and (
            #         item['name'].endswith('.tar.xz') or 
            #         item['name'].endswith('.parquet')
            #     ):
            #         filtered_files.append(item)
                    
        except Exception as e:
            logger.error(f"Filter failed: {e}")
                    
        logger.info(f"Found {len(filtered_files)} files for {date_str}")
        return filtered_files
    
    def run_transfer(self, date_str: str, source_endpoint: str, dest_endpoint: str,
                    source_path: str, dest_path: str, transfer_label: str = None,
                    monitor: bool = False) -> Dict[str, Any]:
        """
        List files and run transfer
        
        Args:
            date_str: Date string (YYYY-MM-DD)
            source_endpoint: Source Globus endpoint ID
            dest_endpoint: Destination Globus endpoint ID
            source_path: Source path on endpoint
            dest_path: Destination path on endpoint
            transfer_label: Label for the transfer
            monitor: Whether to monitor transfer until completion
            
        Returns:
            Dict containing task_id and status
        """
        if not transfer_label:
            transfer_label = f"Audio_{date_str}"
        
        # Ensure paths end with /
        if not source_path.endswith('/'):
            source_path += '/'
        if not dest_path.endswith('/'):
            dest_path += '/'
        
        try:
            # List and filter files
            files = self.list_and_filter_files(source_endpoint, source_path, date_str)
            
            if not files:
                return {
                    'success': False,
                    'error': f"No files found for date {date_str}",
                    'files_found': 0
                }
            
            # Create transfer
            transfer_data = TransferData(
                self.transfer_client,
                source_endpoint,
                dest_endpoint,
                label=transfer_label,
                sync_level="checksum",
                verify_checksum=False,
                preserve_timestamp=True,
                skip_source_errors=True,
                delete_destination_extra=False
            )
            
            # Add files to transfer
            for file_item in files:
                transfer_data.add_item(
                    source_path + file_item['name'],
                    dest_path + file_item['name']
                )
            
            # Submit transfer
            logger.info(f"Submitting transfer of {len(files)} files...")
            transfer_result = self.transfer_client.submit_transfer(transfer_data)
            task_id = transfer_result["task_id"]
            
            logger.info(f"Transfer submitted! Task ID: {task_id}")
            
            result = {
                'success': True,
                'task_id': task_id,
                'files_found': len(files),
                'status': 'ACTIVE'
            }
            
            if monitor:
                # Monitor transfer until completion
                final_status = self.monitor_transfer(task_id)
                result.update(final_status)
            
            return result
            
        except Exception as e:
            if e.info.consent_required:
                logger.error(
                    "Got a ConsentRequired error with scopes:",
                    e.info.consent_required.required_scopes,
                )
                return {
                    'success': False,
                    'error': str(e)
                }
            else:
                logger.error(f"Error during transfer: {e}")
                return {
                    'success': False,
                    'error': str(e)
                }
    
    def monitor_transfer(self, task_id: str, max_wait: int = 600, check_interval: int = 30) -> Dict[str, Any]:
        """
        Monitor a transfer task until completion
        
        Args:
            task_id: Globus transfer task ID
            max_wait: Maximum wait time in seconds
            check_interval: Check interval in seconds
            
        Returns:
            Dict containing final status
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                task = self.transfer_client.get_task(task_id)
                status = task["status"]
                
                logger.info(f"Transfer task {task_id}: {status}")
                
                if status == "SUCCEEDED":
                    return {
                        'status': 'SUCCEEDED',
                        'files_transferred': task.get('files_transferred', 0),
                        'bytes_transferred': task.get('bytes_transferred', 0)
                    }
                elif status == "FAILED":
                    return {
                        'status': 'FAILED',
                        'error': task.get('nice_status_details', 'Transfer failed')
                    }
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error checking transfer status: {e}")
                return {
                    'status': 'ERROR',
                    'error': str(e)
                }
        
        return {
            'status': 'TIMEOUT',
            'error': f"Transfer timed out after {max_wait} seconds"
        }
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get status of a transfer task
        
        Args:
            task_id: Globus transfer task ID
            
        Returns:
            Dict containing task status and details
        """
        try:
            task = self.transfer_client.get_task(task_id)
            
            return {
                'success': True,
                'task_id': task_id,
                'status': task['status'],
                'files_transferred': task.get('files_transferred', 0),
                'bytes_transferred': task.get('bytes_transferred', 0),
                'label': task.get('label', ''),
                'nice_status_details': task.get('nice_status_details', '')
            }
            
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Command-line interface for Globus Transfer operations"""
    parser = argparse.ArgumentParser(description='Globus Transfer Manager for Audio Pipeline')
    parser.add_argument('--token-file', default='~/.globus_refresh_tokens.json',
                       help='Path to refresh tokens JSON file')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Run transfer
    run_parser = subparsers.add_parser('run', help='Run transfer')
    run_parser.add_argument('--date', required=True, help='Date (YYYY-MM-DD)')
    run_parser.add_argument('--source-endpoint', required=True, help='Source endpoint ID')
    run_parser.add_argument('--dest-endpoint', required=True, help='Destination endpoint ID')
    run_parser.add_argument('--source-path', required=True, help='Source path')
    run_parser.add_argument('--dest-path', required=True, help='Destination path')
    run_parser.add_argument('--label', help='Transfer label')
    run_parser.add_argument('--monitor', action='store_true', help='Monitor transfer until completion')
    
    # Check status
    status_parser = subparsers.add_parser('status', help='Check transfer status')
    status_parser.add_argument('task_id', help='Transfer task ID')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        manager = GlobusTransferManager(token_file=args.token_file)
        
        if args.command == 'run':
            result = manager.run_transfer(
                args.date,
                args.source_endpoint,
                args.dest_endpoint,
                args.source_path,
                args.dest_path,
                args.label,
                args.monitor
            )
            
            if result['success']:
                # Print in format that bash script expects
                print(f"Transfer submitted! Task ID: {result['task_id']}")
                print(f"Files found: {result['files_found']}")
                
                if args.monitor:
                    print(f"Final status: {result['status']}")
                    if result['status'] == 'SUCCEEDED':
                        print(f"Files transferred: {result.get('files_transferred', 'unknown')}")
                
                # Also print JSON result for parsing
                print(f"\nResult: {json.dumps(result, indent=2)}")
            else:
                print(f"Transfer failed: {result['error']}")
                sys.exit(1)
                
        elif args.command == 'status':
            result = manager.get_task_status(args.task_id)
            
            if result['success']:
                print(f"Task ID: {result['task_id']}")
                print(f"Status: {result['status']}")
                print(f"Files transferred: {result['files_transferred']}")
                if result['nice_status_details']:
                    print(f"Details: {result['nice_status_details']}")
                print(f"\nResult: {json.dumps(result, indent=2)}")
            else:
                print(f"Failed to get status: {result['error']}")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()