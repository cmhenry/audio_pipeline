#!/bin/bash
# load_test_queue.sh - Quick script to load test queue entries

set -e

# Configuration
DB_HOST="172.23.76.3"
DB_CREDS="host=${DB_HOST} port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
SCRIPT_DIR="$(dirname "$0")/src"

echo "=== Audio Pipeline Queue Loader ==="
echo "Loading test queue entries into processing queue"
echo

# Check if test file exists
TEST_FILE="test_queue_5days.txt"
if [ ! -f "$TEST_FILE" ]; then
    echo "Creating test file: $TEST_FILE"
    cat > "$TEST_FILE" << 'EOF'
# Test Processing Queue - 5 Days for Pipeline Testing
# Format: year, month, date, location
# Starting with January 25th as requested

2025, january, 25, zurich
2025, january, 26, zurich
2025, january, 27, zurich
2025, january, 28, zurich
2025, january, 29, zurich
EOF
    echo "✓ Created $TEST_FILE with 5 test days"
fi

# Test database connection first
echo "Testing database connection..."
if python "$SCRIPT_DIR/db_utils.py" --db-string "$DB_CREDS" test-connection; then
    echo "✓ Database connection successful"
else
    echo "✗ Database connection failed"
    echo "Please check your database configuration and ensure the database is running"
    exit 1
fi

echo

# Load the queue entries
echo "Loading queue entries from $TEST_FILE..."
if python "$SCRIPT_DIR/db_utils.py" --db-string "$DB_CREDS" load-queue "$TEST_FILE"; then
    echo "✓ Queue entries loaded successfully"
else
    echo "✗ Failed to load queue entries"
    exit 1
fi

echo

# Show pending entries
echo "Current pending entries in processing queue:"
python "$SCRIPT_DIR/db_utils.py" --db-string "$DB_CREDS" get-pending --limit 10

echo
echo "=== Pipeline Ready ==="
echo "You can now start the master scheduler to begin processing:"
echo "  ./hpc/0_master_scheduler.sh"
echo
echo "Or manually trigger a specific date:"
echo "  sbatch --export=DATE=2025-01-25 ./hpc/1_globus_transfer_job.sh"