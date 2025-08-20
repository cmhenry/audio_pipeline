#!/usr/bin/env python3
"""
db_utils.py - Database utilities for HPC audio pipeline
Replaces psql commands with Python equivalents
"""

import sys
import os
import json
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages PostgreSQL connection with retry logic"""
    
    def __init__(self, connection_string=None):
        if connection_string is None:
            # Build from environment variables or defaults
            self.connection_string = self._build_connection_string()
        else:
            self.connection_string = connection_string
        
        self.conn = None
        self.connect()
    
    def _build_connection_string(self):
        """Build connection string from environment variables"""
        host = os.getenv('DB_HOST', '172.23.76.3')
        port = os.getenv('DB_PORT', '5432')
        database = os.getenv('DB_NAME', 'audio_pipeline')
        user = os.getenv('DB_USER', 'audio_user')
        password = os.getenv('DB_PASSWORD', 'audio_password')
        
        return f"host={host} port={port} dbname={database} user={user} password={password}"
    
    def connect(self):
        """Establish database connection with retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(self.connection_string)
                self.conn.autocommit = True
                logger.info("Database connection established")
                return
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
    
    def execute(self, query, params=None):
        """Execute a query and return results"""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if cur.description:
                    return cur.fetchall()
                return None
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    def execute_scalar(self, query, params=None):
        """Execute a query and return single value"""
        result = self.execute(query, params)
        if result and len(result) > 0:
            # Return first column of first row
            return list(result[0].values())[0]
        return None
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


class ProcessingQueueManager:
    """Manages processing queue operations"""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def update_transfer_status(self, year, month, day, status, **kwargs):
        """Update transfer status for a specific date"""
        # Build SET clauses dynamically
        set_clauses = ["status = %s", "updated_at = NOW()"]
        params = [status]
        
        # Add optional fields based on status and kwargs
        if status == 'transferring' and 'transfer_start' not in kwargs:
            set_clauses.append("transfer_start = NOW()")
        
        if 'transfer_task_id' in kwargs:
            set_clauses.append("transfer_task_id = %s")
            params.append(kwargs['transfer_task_id'])
        
        if 'transfer_end' in kwargs or status == 'ready_to_process':
            set_clauses.append("transfer_end = NOW()")
        
        if 'error_message' in kwargs:
            set_clauses.append("error_message = %s")
            params.append(kwargs['error_message'])
        
        # Build the complete query
        query = f"""
            UPDATE processing_queue 
            SET {', '.join(set_clauses)}
            WHERE year = %s AND month = %s AND date = %s
        """
        
        # Add WHERE clause parameters
        params.extend([year, month, day])
        
        try:
            result = self.db.execute(query, params)
            logger.info(f"Updated {year}-{month:02d}-{day:02d} to status: {status}")
            return result
        except Exception as e:
            logger.error(f"Failed to update transfer status: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise
    
    def update_processing_status(self, year, month, day, status, **kwargs):
        """Update processing status for a specific date"""
        query = """
            UPDATE processing_queue 
            SET status = %s
        """
        params = [status]
        
        # Add fields based on status
        if status == 'processing':
            query += ", processing_start = NOW()"
            if 'slurm_job_id' in kwargs:
                query += ", slurm_job_id = %s"
                params.append(kwargs['slurm_job_id'])
        
        elif status in ['completed', 'processing_failed', 'completed_with_errors']:
            query += ", processing_end = NOW()"
            if 'error_message' in kwargs:
                query += ", error_message = %s"
                params.append(kwargs['error_message'])
        
        query += " WHERE year = %s AND month = %s AND date = %s"
        params.extend([year, month, day])
        
        self.db.execute(query, params)
        logger.info(f"Updated {year}-{month:02d}-{day:02d} to status: {status}")
    
    def get_pending_dates(self, limit=5):
        """Get pending dates for processing"""
        query = """
            SELECT DISTINCT 
                year || '-' || LPAD(month::text, 2, '0') || '-' || LPAD(date::text, 2, '0') as date_str,
                year, month, date, location
            FROM processing_queue
            WHERE status = 'pending'
            ORDER BY year, month, date
            LIMIT %s
        """
        
        results = self.db.execute(query, [limit])
        return results if results else []
    
    def get_location(self, year, month, day):
        """Get location for a specific date"""
        query = """
            SELECT location 
            FROM processing_queue 
            WHERE year = %s AND month = %s AND date = %s
        """
        
        return self.db.execute_scalar(query, [year, month, day])
    
    def get_folder_name(self, year, month, location='zurich'):
        """Get folder name using database function"""
        query = "SELECT get_folder_name(%s, %s, %s)"
        return self.db.execute_scalar(query, [year, month, location])
    
    def check_job_exists(self, year, month, day):
        """Check if a job is already running for this date"""
        query = """
            SELECT slurm_job_id, status
            FROM processing_queue
            WHERE year = %s AND month = %s AND date = %s
                AND status IN ('transferring', 'processing')
        """
        
        result = self.db.execute(query, [year, month, day])
        return result[0] if result else None
    
    def add_queue_entry(self, year, month, day, location, skip_existing=True):
        """Add entry to processing queue"""
        if skip_existing:
            # Check if entry already exists
            query = """
                SELECT id FROM processing_queue 
                WHERE year = %s AND month = %s AND date = %s AND location = %s
            """
            
            if self.db.execute(query, [year, month, day, location]):
                logger.info(f"Entry already exists: {year}-{month:02d}-{day:02d} {location}")
                return False
        
        # Insert new entry
        query = """
            INSERT INTO processing_queue (year, month, date, location, status, created_at)
            VALUES (%s, %s, %s, %s, 'pending', NOW())
        """
        
        self.db.execute(query, [year, month, day, location])
        logger.info(f"Added queue entry: {year}-{month:02d}-{day:02d} {location}")
        return True


def main():
    """Command-line interface for database operations"""
    parser = argparse.ArgumentParser(description='Database utilities for audio pipeline')
    parser.add_argument('--db-string', help='Database connection string',
                        default=os.getenv('DB_CREDS'))
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Update transfer status
    transfer_parser = subparsers.add_parser('update-transfer', help='Update transfer status')
    transfer_parser.add_argument('date', help='Date (YYYY-MM-DD)')
    transfer_parser.add_argument('status', choices=['transferring', 'ready_to_process', 'transfer_failed'])
    transfer_parser.add_argument('--task-id', help='Globus task ID')
    transfer_parser.add_argument('--error', help='Error message')
    
    # Update processing status
    process_parser = subparsers.add_parser('update-processing', help='Update processing status')
    process_parser.add_argument('date', help='Date (YYYY-MM-DD)')
    process_parser.add_argument('status', choices=['processing', 'completed', 'processing_failed', 'completed_with_errors'])
    process_parser.add_argument('--job-id', type=int, help='Slurm job ID')
    process_parser.add_argument('--error', help='Error message')
    
    # Get pending dates
    pending_parser = subparsers.add_parser('get-pending', help='Get pending dates')
    pending_parser.add_argument('--limit', type=int, default=5, help='Number of dates to return')
    
    # Get location
    location_parser = subparsers.add_parser('get-location', help='Get location for date')
    location_parser.add_argument('date', help='Date (YYYY-MM-DD)')
    
    # Get folder name
    folder_parser = subparsers.add_parser('get-folder', help='Get folder name')
    folder_parser.add_argument('year', type=int, help='Year')
    folder_parser.add_argument('month', type=int, help='Month')
    folder_parser.add_argument('--location', default='zurich', help='Location')
    
    # Check job exists
    check_parser = subparsers.add_parser('check-job', help='Check if job exists')
    check_parser.add_argument('date', help='Date (YYYY-MM-DD)')
    
    # Load queue entries
    load_parser = subparsers.add_parser('load-queue', help='Load queue entries from file')
    load_parser.add_argument('file', help='File with queue entries (year,month,date,location per line)')
    load_parser.add_argument('--force', action='store_true', help='Overwrite existing entries')
    
    # Test connection
    test_parser = subparsers.add_parser('test-connection', help='Test database connection')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize database connection
    try:
        db = DatabaseConnection(args.db_string)
        queue_mgr = ProcessingQueueManager(db)
        
        # Parse date if provided
        if hasattr(args, 'date') and args.date:
            year, month, day = map(int, args.date.split('-'))
        
        # Execute command
        if args.command == 'update-transfer':
            kwargs = {}
            if args.task_id:
                kwargs['transfer_task_id'] = args.task_id
            if args.error:
                kwargs['error_message'] = args.error
            if args.status == 'ready_to_process':
                kwargs['transfer_end'] = True
                
            queue_mgr.update_transfer_status(year, month, day, args.status, **kwargs)
            
        elif args.command == 'update-processing':
            kwargs = {}
            if args.job_id:
                kwargs['slurm_job_id'] = args.job_id
            if args.error:
                kwargs['error_message'] = args.error
                
            queue_mgr.update_processing_status(year, month, day, args.status, **kwargs)
            
        elif args.command == 'get-pending':
            results = queue_mgr.get_pending_dates(args.limit)
            # Print just the date strings, one per line, for easy parsing in bash
            for row in results:
                print(row['date_str'])
                
        elif args.command == 'get-location':
            location = queue_mgr.get_location(year, month, day)
            if location:
                print(location)
            else:
                print("zurich")  # Default
                
        elif args.command == 'get-folder':
            folder = queue_mgr.get_folder_name(args.year, args.month, args.location)
            if folder:
                print(folder)
                
        elif args.command == 'check-job':
            result = queue_mgr.check_job_exists(year, month, day)
            if result:
                print(f"Job exists: ID={result['slurm_job_id']}, Status={result['status']}")
                sys.exit(1)  # Exit with error to indicate job exists
            else:
                sys.exit(0)  # Success - no job exists
                
        elif args.command == 'load-queue':
            # Load queue entries from file
            from pathlib import Path
            
            file_path = Path(args.file)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                sys.exit(1)
            
            # Import the parsing function from load_processing_queue.py
            sys.path.insert(0, str(file_path.parent))
            try:
                from load_processing_queue import parse_queue_file
                entries = parse_queue_file(file_path)
                
                if not entries:
                    logger.error("No valid entries found in file")
                    sys.exit(1)
                
                loaded_count = 0
                for entry in entries:
                    if queue_mgr.add_queue_entry(
                        entry['year'], entry['month'], entry['date'], entry['location'],
                        skip_existing=not args.force
                    ):
                        loaded_count += 1
                
                print(f"Loaded {loaded_count} entries into processing queue")
                
            except ImportError:
                logger.error("Could not import queue file parser. Make sure load_processing_queue.py is in the same directory.")
                sys.exit(1)
                
        elif args.command == 'test-connection':
            print(f"Database connection successful: {db.connection_string}")
            print("Ready to execute operations")
        
        db.close()
        
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()