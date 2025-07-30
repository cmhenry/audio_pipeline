#!/usr/bin/env python3
"""
globus_flow_manager.py - Manage Globus Flows for audio pipeline transfers
Uses official Globus SDK for robust API interactions
"""

import json
import os
import sys
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from globus_sdk import FlowsClient, SpecificFlowClient, FlowsAPIError
from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GlobusFlowManager:
    """Manages Globus Flow operations for audio transfers using official SDK"""
    
    def __init__(self, token_file: str = "~/.globus_refresh_tokens.json"):
        """
        Initialize Globus Flow manager using refresh tokens
        
        Args:
            token_file: Path to JSON file containing refresh tokens (default: ~/.globus_refresh_tokens.json)
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
        client_id = tokens['CLIENT_ID']
        self.tokens = tokens # Store all for later
        flows_refresh_token = tokens['FLOWS_REFRESH_TOKEN']
        if not flows_refresh_token:
            raise ValueError(
                "No FLOWS_REFRESH_TOKEN found in token file.\n"
                "Please regenerate tokens with Flows API scope."
            )
        
        # Create native app auth client
        self.auth_client = NativeAppAuthClient(client_id)
        
        # Create refresh token authorizer for Flows API
        # Flows uses the auth.globus.org refresh token with flows scope
        flows_authorizer = RefreshTokenAuthorizer(
            flows_refresh_token,
            self.auth_client,
            # scopes=["urn:globus:auth:scope:flows.globus.org:all"]
        )
        
        # Initialize SDK clients
        self.flows_client = FlowsClient(authorizer=flows_authorizer)
        self.authorizer = flows_authorizer  # Store for creating specific clients
        
        # Flow ID will be set after deployment
        self.flow_id = "bbe2c78f-b7e4-490c-99de-f2b49b6cbb42"
        self._specific_client = None
        
        logger.info("Initialized Globus Flow Manager with refresh tokens")
    
    @property 
    def specific_client(self) -> SpecificFlowClient:
        """Get or create SpecificFlowClient for the current flow"""
        if not self.flow_id:
            raise ValueError("Flow ID not set. Deploy flow first or set AUDIO_TRANSFER_FLOW_ID environment variable")
        
        if not self._specific_client:
            # Use the flow-specific refresh token
            flow_specific_token = self.tokens.get('FLOW_SPECIFIC_REFRESH_TOKEN')
            
            if not flow_specific_token:
                # Try using the flow ID as the resource server key
                flow_specific_token = self.tokens.get(self.flow_id)
                
            if not flow_specific_token:
                raise ValueError(
                    f"No refresh token found for flow {self.flow_id}.\n"
                    "Regenerate tokens after deploying your flow to include its scope."
                )
            
            # Create authorizer with flow-specific token
            flow_authorizer = RefreshTokenAuthorizer(
                flow_specific_token,
                self.auth_client
            )
            
            self._specific_client = SpecificFlowClient(self.flow_id, authorizer=flow_authorizer)
          
        return self._specific_client
    
    def deploy_flow(self, flow_definition_path: str, title: str = "Audio Pipeline Transfer Flow") -> Optional[str]:
        """
        Deploy a new Globus Flow using the SDK
        
        Args:
            flow_definition_path: Path to flow definition JSON file
            title: Flow title
            
        Returns:
            str: Flow ID if successful, None if failed
        """
        try:
            with open(flow_definition_path, 'r') as f:
                flow_definition = json.load(f)
            
            # Extract input schema if present in definition
            input_schema = flow_definition.pop('InputSchema', None)
            
            # Create flow using SDK
            response = self.flows_client.create_flow(
                title=title,
                definition=flow_definition,
                input_schema=input_schema,
                description="Automated file discovery, filtering, and transfer for audio pipeline",
                keywords=["audio", "pipeline", "transfer"],
                flow_viewers=['public'],
                flow_starters=['all_authenticated_users']
            )
            
            flow_id = response['id']
            logger.info(f"Flow deployed successfully: {flow_id}")
            logger.info(f"Set environment variable: export AUDIO_TRANSFER_FLOW_ID={flow_id}")
            
            # Update internal flow ID
            self.flow_id = flow_id
            self._specific_client = None  # Reset specific client
            
            return flow_id
                
        except FlowsAPIError as e:
            logger.error(f"Flows API error during deployment: {e}")
            return None
        except Exception as e:
            logger.error(f"Error deploying flow: {e}")
            return None
    
    def run_transfer_flow(self, date_str: str, source_endpoint: str, dest_endpoint: str,
                         source_path: str, dest_path: str, transfer_label: str = None) -> Dict[str, Any]:
        """
        Run the audio transfer flow using SDK
        
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
        if not transfer_label:
            transfer_label = f"Audio_{date_str}"
        
        # Ensure paths end with /
        if not source_path.endswith('/'):
            source_path += '/'
        if not dest_path.endswith('/'):
            dest_path += '/'
        
        flow_input = {
            "date_str": date_str,
            "source_endpoint": source_endpoint,
            "dest_endpoint": dest_endpoint,
            "source_path": source_path,
            "dest_path": dest_path,
            "transfer_label": transfer_label
        }
        
        try:
            # Run flow using SDK
            response = self.specific_client.run_flow(
                body={"input": flow_input},
                label=f"Audio Pipeline - {date_str}",
                tags=["audio", "pipeline", date_str]
            )
            
            run_id = response['run_id']
            logger.info(f"Flow run initiated: {run_id}")
            
            return {
                'run_id': run_id,
                'status': 'ACTIVE',
                'success': True
            }
            
        except FlowsAPIError as e:
            logger.error(f"Flows API error during run: {e}")
            return {
                'success': False,
                'error': f"Flows API error: {e}"
            }
        except Exception as e:
            logger.error(f"Error running flow: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """
        Get status of a flow run using SDK
        
        Args:
            run_id: Flow run ID
            
        Returns:
            Dict containing run status and details
        """
        try:
            # Get run status using SDK
            response = self.flows_client.get_run(run_id)
            
            # Extract key information
            status = response.get('status', 'UNKNOWN')
            details = response.get('details', {})
            
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
            
        except FlowsAPIError as e:
            logger.error(f"Flows API error getting run status: {e}")
            return {
                'success': False,
                'error': f"Flows API error: {e}"
            }
        except Exception as e:
            logger.error(f"Error getting run status: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_run_logs(self, run_id: str, limit: int = 100) -> Dict[str, Any]:
        """
        Get execution logs for a flow run
        
        Args:
            run_id: Flow run ID
            limit: Maximum number of log entries to retrieve
            
        Returns:
            Dict containing logs and metadata
        """
        try:
            response = self.flows_client.get_run_logs(run_id, limit=limit)
            return {
                'success': True,
                'logs': response.get('logs', []),
                'has_next_page': response.get('has_next_page', False)
            }
        except FlowsAPIError as e:
            logger.error(f"Flows API error getting run logs: {e}")
            return {
                'success': False,
                'error': f"Flows API error: {e}"
            }
        except Exception as e:
            logger.error(f"Error getting run logs: {e}")
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
                # Get logs for completed runs to provide more context
                logs_data = self.get_run_logs(run_id, limit=10)
                if logs_data['success']:
                    status_data['recent_logs'] = logs_data['logs']
                
                return status_data
            
            time.sleep(check_interval)
        
        return {
            'success': False,
            'error': f"Flow run timed out after {max_wait} seconds",
            'status': 'TIMEOUT'
        }
    
    def list_flows(self) -> Dict[str, Any]:
        """List all flows accessible to the user using SDK"""
        try:
            response = self.flows_client.list_flows()
            return {
                'success': True,
                'flows': response.get('flows', [])
            }
        except FlowsAPIError as e:
            logger.error(f"Flows API error listing flows: {e}")
            return {
                'success': False,
                'error': f"Flows API error: {e}"
            }
        except Exception as e:
            logger.error(f"Error listing flows: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def validate_flow(self, flow_definition_path: str) -> Dict[str, Any]:
        """
        Validate a flow definition without deploying it
        
        Args:
            flow_definition_path: Path to flow definition JSON file
            
        Returns:
            Dict containing validation results
        """
        try:
            with open(flow_definition_path, 'r') as f:
                flow_definition = json.load(f)
            
            # Extract input schema if present
            input_schema = flow_definition.pop('InputSchema', None)
            
            response = self.flows_client.validate_flow(
                definition=flow_definition,
                input_schema=input_schema
            )
            
            return {
                'success': True,
                'valid': response.get('valid', False),
                'errors': response.get('errors', [])
            }
            
        except FlowsAPIError as e:
            logger.error(f"Flows API error during validation: {e}")
            return {
                'success': False,
                'error': f"Flows API error: {e}"
            }
        except Exception as e:
            logger.error(f"Error validating flow: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Command-line interface for Globus Flow operations"""
    parser = argparse.ArgumentParser(description='Globus Flow Manager for Audio Pipeline')
    parser.add_argument('--token-file', default='~/.globus_refresh_tokens.json',
                       help='Path to refresh tokens JSON file')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Deploy flow
    deploy_parser = subparsers.add_parser('deploy', help='Deploy flow definition')
    deploy_parser.add_argument('flow_file', help='Path to flow definition JSON file')
    deploy_parser.add_argument('--title', default='Audio Pipeline Transfer Flow', help='Flow title')
    
    # Validate flow
    validate_parser = subparsers.add_parser('validate', help='Validate flow definition')
    validate_parser.add_argument('flow_file', help='Path to flow definition JSON file')
    
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
    
    # Get logs
    logs_parser = subparsers.add_parser('logs', help='Get run logs')
    logs_parser.add_argument('run_id', help='Flow run ID')
    logs_parser.add_argument('--limit', type=int, default=100, help='Maximum log entries')
    
    # List flows
    list_parser = subparsers.add_parser('list', help='List flows')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        manager = GlobusFlowManager(token_file=args.token_file)
        
        if args.command == 'deploy':
            flow_id = manager.deploy_flow(args.flow_file, args.title)
            if flow_id:
                print(f"Flow deployed: {flow_id}")
                print(f"Set environment variable: export AUDIO_TRANSFER_FLOW_ID={flow_id}")
            else:
                sys.exit(1)
        
        elif args.command == 'validate':
            result = manager.validate_flow(args.flow_file)
            if result['success']:
                if result['valid']:
                    print("✓ Flow definition is valid")
                else:
                    print("✗ Flow definition has errors:")
                    for error in result['errors']:
                        print(f"  - {error}")
                    sys.exit(1)
            else:
                print(f"Validation failed: {result['error']}")
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
                        if 'recent_logs' in final_result and final_result['recent_logs']:
                            print("Recent logs:")
                            for log in final_result['recent_logs'][-5:]:  # Show last 5 logs
                                print(f"  {log.get('timestamp', '')}: {log.get('message', '')}")
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
        
        elif args.command == 'logs':
            result = manager.get_run_logs(args.run_id, args.limit)
            
            if result['success']:
                logs = result['logs']
                print(f"Retrieved {len(logs)} log entries:")
                for log in logs:
                    timestamp = log.get('timestamp', 'Unknown')
                    message = log.get('message', 'No message')
                    level = log.get('level', 'INFO')
                    print(f"[{timestamp}] {level}: {message}")
                
                if result.get('has_next_page'):
                    print("\n(More logs available - use pagination to retrieve)")
            else:
                print(f"Failed to get logs: {result['error']}")
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