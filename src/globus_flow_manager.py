#!/usr/bin/env python3
"""
globus_flow_manager.py - Manage Globus Flows for audio pipeline transfers
"""

import json
import os
import sys
import time
import argparse
import logging
from pathlib import Path
import requests
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GlobusFlowManager:
    """Manages Globus Flow operations for audio transfers"""
    
    def __init__(self, access_token: Optional[str] = None):
        """
        Initialize Globus Flow manager
        
        Args:
            access_token: Globus access token (can also be set via GLOBUS_ACCESS_TOKEN env var)
        """
        self.access_token = access_token or os.getenv('GLOBUS_ACCESS_TOKEN')
        if not self.access_token:
            raise ValueError("Access token required. Set GLOBUS_ACCESS_TOKEN environment variable or pass as parameter")
        
        self.base_url = "https://flows.globus.org/v1"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        # Flow ID will be set after deployment
        self.flow_id = os.getenv('AUDIO_TRANSFER_FLOW_ID')
    
    def deploy_flow(self, flow_definition_path: str, title: str = "Audio Pipeline Transfer Flow") -> str:
        """
        Deploy a new Globus Flow
        
        Args:
            flow_definition_path: Path to flow definition JSON file
            title: Flow title
            
        Returns:
            str: Flow ID
        """
        try:
            with open(flow_definition_path, 'r') as f:
                flow_definition = json.load(f)
            
            payload = {
                "definition": flow_definition,
                "title": title,
                "description": "Automated file discovery, filtering, and transfer for TikTok audio pipeline",
                "keywords": ["audio", "pipeline", "transfer", "tiktok"],
                "visible_to": ["public"],
                "runnable_by": ["all_authenticated_users"]
            }
            
            response = requests.post(
                f"{self.base_url}/flows",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 201:
                flow_data = response.json()
                flow_id = flow_data['id']
                logger.info(f"Flow deployed successfully: {flow_id}")
                logger.info(f"Set environment variable: export AUDIO_TRANSFER_FLOW_ID={flow_id}")
                return flow_id
            else:
                logger.error(f"Failed to deploy flow: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error deploying flow: {e}")
            return None
    
    def run_transfer_flow(self, date_str: str, source_endpoint: str, dest_endpoint: str,
                         source_path: str, dest_path: str, transfer_label: str = None) -> Dict[str, Any]:
        """
        Run the audio transfer flow
        
        Args:
            date_str: Date string (YYYY-MM-DD)
            source_endpoint: Source Globus endpoint ID
            dest_endpoint: Destination Globus endpoint ID
            source_path: Source path on endpoint
            dest_path: Destination path on endpoint
            transfer_label: Label for the transfer
            
        Returns:
            Dict containing run_id and initial status
        """
        if not self.flow_id:
            raise ValueError("Flow ID not set. Deploy flow first or set AUDIO_TRANSFER_FLOW_ID environment variable")
        
        if not transfer_label:
            transfer_label = f"Audio_{date_str}"
        
        # Ensure paths end with /
        if not source_path.endswith('/'):
            source_path += '/'
        if not dest_path.endswith('/'):
            dest_path += '/'
        
        payload = {
            "body": {
                "input": {
                    "date_str": date_str,
                    "source_endpoint": source_endpoint,
                    "dest_endpoint": dest_endpoint,
                    "source_path": source_path,
                    "dest_path": dest_path,
                    "transfer_label": transfer_label
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/flows/{self.flow_id}/run",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 201:
                run_data = response.json()
                logger.info(f"Flow run initiated: {run_data['run_id']}")
                return {
                    'run_id': run_data['run_id'],
                    'status': 'ACTIVE',
                    'success': True
                }
            else:
                logger.error(f"Failed to start flow: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
                
        except Exception as e:
            logger.error(f"Error running flow: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """
        Get status of a flow run
        
        Args:
            run_id: Flow run ID
            
        Returns:
            Dict containing run status and details
        """
        try:
            response = requests.get(
                f"{self.base_url}/runs/{run_id}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                run_data = response.json()
                
                # Extract key information
                status = run_data.get('status', 'UNKNOWN')
                details = run_data.get('details', {})
                
                result = {
                    'run_id': run_id,
                    'status': status,
                    'success': True
                }
                
                # Add result data if available
                if 'result' in details:
                    result['result'] = details['result']
                    
                    # Extract transfer task ID if available
                    if isinstance(details['result'], dict) and 'task_id' in details['result']:
                        result['task_id'] = details['result']['task_id']
                        result['files_found'] = details['result'].get('files_found', 0)
                
                return result
            else:
                logger.error(f"Failed to get run status: {response.status_code} - {response.text}")
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
                
        except Exception as e:
            logger.error(f"Error getting run status: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def monitor_run(self, run_id: str, max_wait: int = 600, check_interval: int = 30) -> Dict[str, Any]:
        """
        Monitor a flow run until completion
        
        Args:
            run_id: Flow run ID
            max_wait: Maximum wait time in seconds
            check_interval: Check interval in seconds
            
        Returns:
            Dict containing final status and results
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            status_data = self.get_run_status(run_id)
            
            if not status_data['success']:
                return status_data
            
            status = status_data['status']
            logger.info(f"Flow run {run_id}: {status}")
            
            if status in ['SUCCEEDED', 'FAILED']:
                return status_data
            
            time.sleep(check_interval)
        
        return {
            'success': False,
            'error': f"Flow run timed out after {max_wait} seconds",
            'status': 'TIMEOUT'
        }
    
    def list_flows(self) -> Dict[str, Any]:
        """List all flows accessible to the user"""
        try:
            response = requests.get(
                f"{self.base_url}/flows",
                headers=self.headers
            )
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'flows': response.json()
                }
            else:
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Command-line interface for Globus Flow operations"""
    parser = argparse.ArgumentParser(description='Globus Flow Manager for Audio Pipeline')
    parser.add_argument('--access-token', help='Globus access token (or set GLOBUS_ACCESS_TOKEN env var)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Deploy flow
    deploy_parser = subparsers.add_parser('deploy', help='Deploy flow definition')
    deploy_parser.add_argument('flow_file', help='Path to flow definition JSON file')
    deploy_parser.add_argument('--title', default='Audio Pipeline Transfer Flow', help='Flow title')
    
    # Run transfer
    run_parser = subparsers.add_parser('run', help='Run transfer flow')
    run_parser.add_argument('--date', required=True, help='Date (YYYY-MM-DD)')
    run_parser.add_argument('--source-endpoint', required=True, help='Source endpoint ID')
    run_parser.add_argument('--dest-endpoint', required=True, help='Destination endpoint ID')
    run_parser.add_argument('--source-path', required=True, help='Source path')
    run_parser.add_argument('--dest-path', required=True, help='Destination path')
    run_parser.add_argument('--label', help='Transfer label')
    run_parser.add_argument('--monitor', action='store_true', help='Monitor run until completion')
    
    # Check status
    status_parser = subparsers.add_parser('status', help='Check run status')
    status_parser.add_argument('run_id', help='Flow run ID')
    
    # List flows
    list_parser = subparsers.add_parser('list', help='List flows')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        manager = GlobusFlowManager(args.access_token)
        
        if args.command == 'deploy':
            flow_id = manager.deploy_flow(args.flow_file, args.title)
            if flow_id:
                print(f"Flow deployed: {flow_id}")
                print(f"Set environment variable: export AUDIO_TRANSFER_FLOW_ID={flow_id}")
            else:
                sys.exit(1)
                
        elif args.command == 'run':
            result = manager.run_transfer_flow(
                args.date,
                args.source_endpoint,
                args.dest_endpoint,
                args.source_path,
                args.dest_path,
                args.label
            )
            
            if result['success']:
                print(f"Flow run started: {result['run_id']}")
                
                if args.monitor:
                    print("Monitoring flow execution...")
                    final_result = manager.monitor_run(result['run_id'])
                    
                    if final_result['success']:
                        print(f"Flow completed: {final_result['status']}")
                        if 'result' in final_result:
                            print(f"Result: {json.dumps(final_result['result'], indent=2)}")
                    else:
                        print(f"Flow monitoring failed: {final_result['error']}")
                        sys.exit(1)
            else:
                print(f"Flow run failed: {result['error']}")
                sys.exit(1)
                
        elif args.command == 'status':
            result = manager.get_run_status(args.run_id)
            
            if result['success']:
                print(f"Run ID: {result['run_id']}")
                print(f"Status: {result['status']}")
                if 'result' in result:
                    print(f"Result: {json.dumps(result['result'], indent=2)}")
            else:
                print(f"Failed to get status: {result['error']}")
                sys.exit(1)
                
        elif args.command == 'list':
            result = manager.list_flows()
            
            if result['success']:
                flows = result['flows']
                print(f"Found {len(flows)} flows:")
                for flow in flows:
                    print(f"  {flow['id']}: {flow.get('title', 'Untitled')}")
            else:
                print(f"Failed to list flows: {result['error']}")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()