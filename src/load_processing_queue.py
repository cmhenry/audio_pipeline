#!/usr/bin/env python3
"""
load_processing_queue.py - Load processing queue entries from text file
"""

import argparse
import psycopg2
import logging
from pathlib import Path
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_queue_file(file_path: Path):
    """Parse processing queue file and return list of entries"""
    entries = []
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = [p.strip() for p in line.split(',')]
                if len(parts) != 4:
                    logger.error(f"Line {line_num}: Expected 4 fields (year,month,date,location), got {len(parts)}")
                    continue
                
                try:
                    year = int(parts[0])
                    month_str = parts[1].lower()
                    day = int(parts[2])
                    location = parts[3]
                    
                    # Convert month name to number
                    month_map = {
                        'january': 1, 'jan': 1,
                        'february': 2, 'feb': 2,
                        'march': 3, 'mar': 3,
                        'april': 4, 'apr': 4,
                        'may': 5,
                        'june': 6, 'jun': 6,
                        'july': 7, 'jul': 7,
                        'august': 8, 'aug': 8,
                        'september': 9, 'sep': 9, 'sept': 9,
                        'october': 10, 'oct': 10,
                        'november': 11, 'nov': 11,
                        'december': 12, 'dec': 12
                    }
                    
                    month = month_map.get(month_str)
                    if month is None:
                        # Try parsing as integer
                        try:
                            month = int(month_str)
                            if month < 1 or month > 12:
                                raise ValueError(f"Month must be 1-12, got {month}")
                        except ValueError:
                            logger.error(f"Line {line_num}: Invalid month '{month_str}'")
                            continue
                    
                    # Validate date ranges
                    if year < 2000 or year > 2030:
                        logger.error(f"Line {line_num}: Year {year} out of range (2000-2030)")
                        continue
                        
                    if day < 1 or day > 31:
                        logger.error(f"Line {line_num}: Day {day} out of range (1-31)")
                        continue
                    
                    entries.append({
                        'year': year,
                        'month': month,
                        'date': day,
                        'location': location
                    })
                    
                    logger.info(f"Parsed: {year}-{month:02d}-{day:02d} {location}")
                    
                except ValueError as e:
                    logger.error(f"Line {line_num}: Parse error - {e}")
                    continue
                    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return []
    
    return entries


def load_entries_to_database(entries, db_connection_string, skip_existing=True):
    """Load entries into PostgreSQL processing queue"""
    if not entries:
        logger.warning("No entries to load")
        return 0
    
    try:
        conn = psycopg2.connect(db_connection_string)
        cur = conn.cursor()
        
        loaded_count = 0
        skipped_count = 0
        
        for entry in entries:
            year = entry['year']
            month = entry['month']
            date = entry['date']
            location = entry['location']
            
            if skip_existing:
                # Check if entry already exists
                cur.execute("""
                    SELECT id FROM processing_queue 
                    WHERE year = %s AND month = %s AND date = %s AND location = %s
                """, (year, month, date, location))
                
                if cur.fetchone():
                    logger.info(f"Skipping existing entry: {year}-{month:02d}-{date:02d} {location}")
                    skipped_count += 1
                    continue
            
            # Insert new entry
            cur.execute("""
                INSERT INTO processing_queue (year, month, date, location, status, created_at)
                VALUES (%s, %s, %s, %s, 'pending', NOW())
            """, (year, month, date, location))
            
            logger.info(f"Loaded: {year}-{month:02d}-{date:02d} {location}")
            loaded_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Database loading complete: {loaded_count} loaded, {skipped_count} skipped")
        return loaded_count
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 0


def create_sample_file(file_path: Path):
    """Create a sample processing queue file"""
    sample_content = """# Processing Queue Sample File
# Format: year, month, date, location
# Month can be name (january) or number (1)
# Lines starting with # are ignored

2025, january, 25, zurich
2025, january, 26, zurich
2025, january, 27, zurich
2025, january, 28, zurich
2025, january, 29, zurich
"""
    
    try:
        with open(file_path, 'w') as f:
            f.write(sample_content)
        logger.info(f"Created sample file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create sample file: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Load processing queue entries from text file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load from file
  python load_processing_queue.py --file queue_entries.txt --db-string "host=VM_IP port=5432 dbname=audio_pipeline user=audio_user password=PASSWORD"
  
  # Create sample file
  python load_processing_queue.py --create-sample sample_queue.txt
  
  # Load with force overwrite
  python load_processing_queue.py --file queue_entries.txt --db-string "..." --force

File format:
  year, month, date, location
  2025, january, 25, zurich
  2025, 1, 26, zurich
  2025, feb, 27, zurich
        """
    )
    
    parser.add_argument('--file', type=str, help='Input file with queue entries')
    parser.add_argument('--db-string', type=str, help='PostgreSQL connection string')
    parser.add_argument('--create-sample', type=str, help='Create sample file at specified path')
    parser.add_argument('--force', action='store_true', help='Overwrite existing entries')
    parser.add_argument('--dry-run', action='store_true', help='Parse file but do not load to database')
    
    args = parser.parse_args()
    
    # Create sample file
    if args.create_sample:
        sample_path = Path(args.create_sample)
        if create_sample_file(sample_path):
            print(f"Sample file created: {sample_path}")
            print("Edit the file and then load it with:")
            print(f"python {sys.argv[0]} --file {sample_path} --db-string 'host=VM_IP port=5432 dbname=audio_pipeline user=audio_user password=PASSWORD'")
        return
    
    # Validate required arguments
    if not args.file:
        parser.error("--file is required (or use --create-sample to create a sample file)")
    
    if not args.dry_run and not args.db_string:
        parser.error("--db-string is required (or use --dry-run to test parsing)")
    
    # Parse input file
    file_path = Path(args.file)
    entries = parse_queue_file(file_path)
    
    if not entries:
        logger.error("No valid entries found in file")
        return
    
    logger.info(f"Parsed {len(entries)} entries from {file_path}")
    
    # Dry run - just show what would be loaded
    if args.dry_run:
        print("\nDry run - entries that would be loaded:")
        for entry in entries:
            print(f"  {entry['year']}-{entry['month']:02d}-{entry['date']:02d} {entry['location']}")
        print(f"\nTotal: {len(entries)} entries")
        return
    
    # Load to database
    loaded_count = load_entries_to_database(
        entries, 
        args.db_string, 
        skip_existing=not args.force
    )
    
    if loaded_count > 0:
        print(f"\nSuccess: Loaded {loaded_count} entries into processing queue")
        print("You can now start the master scheduler to begin processing")
    else:
        print("\nNo entries were loaded")


if __name__ == '__main__':
    main()