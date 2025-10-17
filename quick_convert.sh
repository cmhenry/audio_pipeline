#!/bin/bash
#SBATCH --job-name=mp3_to_opus
#SBATCH --output=/data/cohenr/logs/convert/convert_%A_%a.out
#SBATCH --error=/data/cohenr/error/convert/convert_%A_%a.err
#SBATCH --array=1-950%500           # Adjust: total_archives / ARCHIVES_PER_TASK
#SBATCH --cpus-per-task=8       # For parallel conversion
#SBATCH --mem=32G               # 4GB per concurrent conversion * 8
#SBATCH --time=08:00:00         # Increased for multiple archives per task

# Configuration
FILELIST="/data/cohenr/utilities/may_archive_list.txt"     # Input: list of archive paths
OUTPUT_BASE="/shares/bdm.ipz.uzh/staging/may/"   # Where to store converted files
PARALLEL_JOBS=8                 # Number of concurrent conversions
OPUS_BITRATE="64k"            # Adjust quality as needed (64k-256k)
ARCHIVES_PER_TASK=12           # Number of archives each array task processes

# Create logs directory if it doesn't exist
# mkdir -p logs

# Calculate which archives this task should process
START_LINE=$(( (SLURM_ARRAY_TASK_ID - 1) * ARCHIVES_PER_TASK + 1 ))
END_LINE=$(( SLURM_ARRAY_TASK_ID * ARCHIVES_PER_TASK ))

echo "========================================"
echo "Job ID: ${SLURM_JOB_ID}, Array Task: ${SLURM_ARRAY_TASK_ID}"
echo "Processing archives ${START_LINE} to ${END_LINE} from filelist"
echo "Started at: $(date)"
echo "========================================"

# Get list of archives for this task
ARCHIVES_TO_PROCESS=$(sed -n "${START_LINE},${END_LINE}p" "$FILELIST")

if [ -z "$ARCHIVES_TO_PROCESS" ]; then
    echo "Error: No archives found for array index ${SLURM_ARRAY_TASK_ID}"
    exit 1
fi

# Count how many archives we're actually processing
NUM_ARCHIVES=$(echo "$ARCHIVES_TO_PROCESS" | wc -l)
echo "Found ${NUM_ARCHIVES} archive(s) to process"
echo ""

# Initialize overall statistics
TOTAL_ARCHIVES_PROCESSED=0
TOTAL_ARCHIVES_FAILED=0
TOTAL_FILES_CONVERTED=0
TOTAL_FILES_FAILED=0

# Use local scratch if available for better I/O performance
if [ -n "$TMPDIR" ]; then
    WORK_DIR="$TMPDIR/work_$"
else
    WORK_DIR="/tmp/work_$"
fi
mkdir -p "$WORK_DIR"

# Function to convert a single MP3 to Opus
convert_mp3_to_opus() {
    local mp3_file="$1"
    # local relative_path="$2"
    # local output_file="${OUTPUT_DIR}/${relative_path%.mp3}.opus"
    
    # Extract just the filename, ignore the nested path structure
    local filename=$(basename "$relative_path")
    local output_file="${OUTPUT_DIR}/${filename%.mp3}.opus"
    
    # Convert using ffmpeg
    if ffmpeg -i "$mp3_file" -c:a libopus -b:a "$OPUS_BITRATE" -application voip -compression_level 5 "$output_file" -y 2>/dev/null; then
        echo "  Converted: $filename"
        return 0
    else
        echo "  ERROR: Failed to convert $mp3_file" >&2
        return 1
    fi
}

export -f convert_mp3_to_opus
export OPUS_BITRATE

# Process each archive assigned to this task
# Process each archive assigned to this task
while IFS= read -r ARCHIVE; do
    echo "----------------------------------------"
    echo "Processing archive: $ARCHIVE"
    echo "Started: $(date)"
    
    # Extract the base name of the archive (without .tar.gz)
    ARCHIVE_BASENAME=$(basename "$ARCHIVE" .tar.gz)
    
    # Create output directory preserving structure
    RELATIVE_PATH=$(dirname "$ARCHIVE" | sed "s|^/path/to/input||")
    OUTPUT_DIR="${OUTPUT_BASE}/${ARCHIVE_BASENAME}"
    mkdir -p "$OUTPUT_DIR"
    export OUTPUT_DIR
    
    echo "Output directory: $OUTPUT_DIR"
    
    # Track statistics for this archive
    ARCHIVE_TOTAL_FILES=0
    ARCHIVE_SUCCESS_COUNT=0
    ARCHIVE_FAIL_COUNT=0

    # Create a unique extraction directory for this archive
    ARCHIVE_WORK_DIR="${WORK_DIR}/extract_$(basename "$ARCHIVE" .tar.gz)_$$"
    mkdir -p "$ARCHIVE_WORK_DIR"

    # Extract the entire archive at once
    echo "Extracting entire archive to working directory..."
    if ! tar -xf "$ARCHIVE" -C "$ARCHIVE_WORK_DIR" 2>&1; then
        echo "ERROR: Failed to extract archive"
        TOTAL_ARCHIVES_FAILED=$((TOTAL_ARCHIVES_FAILED + 1))
        rm -rf "$ARCHIVE_WORK_DIR"
        continue
    fi

    # Find all MP3 files in the extracted directory
    echo "Searching for MP3 files in extracted content..."
    mapfile -t MP3_FILES < <(find "$ARCHIVE_WORK_DIR" -type f -iname "*.mp3")
    ARCHIVE_TOTAL_FILES=${#MP3_FILES[@]}

    echo "Found $ARCHIVE_TOTAL_FILES MP3 files to convert"

    if [ "$ARCHIVE_TOTAL_FILES" -eq 0 ]; then
        echo "Warning: No MP3 files found in archive"
        rm -rf "$ARCHIVE_WORK_DIR"
        TOTAL_ARCHIVES_PROCESSED=$((TOTAL_ARCHIVES_PROCESSED + 1))
        continue
    fi

    # Process files in parallel
    for mp3_file in "${MP3_FILES[@]}"; do
        # Wait if we have too many background jobs
        while [ $(jobs -r | wc -l) -ge $PARALLEL_JOBS ]; do
            sleep 0.1
        done
        
        (
            # Convert the file
            if convert_mp3_to_opus "$mp3_file"; then
                echo "1" > "${WORK_DIR}/success_$$_${RANDOM}.tmp"
            else
                echo "1" > "${WORK_DIR}/fail_$$_${RANDOM}.tmp"
            fi
        ) &
    done

    # Wait for all background jobs to complete
    wait

    # Count successes and failures for this archive
    ARCHIVE_SUCCESS_COUNT=$(find "$WORK_DIR" -name "success_*.tmp" 2>/dev/null | wc -l)
    ARCHIVE_FAIL_COUNT=$(find "$WORK_DIR" -name "fail_*.tmp" 2>/dev/null | wc -l)

    # Clean up extracted files and temp files for this archive
    rm -rf "$ARCHIVE_WORK_DIR"
    rm -f "${WORK_DIR}"/success_*.tmp "${WORK_DIR}"/fail_*.tmp
    
    # Update overall statistics
    TOTAL_FILES_CONVERTED=$((TOTAL_FILES_CONVERTED + ARCHIVE_SUCCESS_COUNT))
    TOTAL_FILES_FAILED=$((TOTAL_FILES_FAILED + ARCHIVE_FAIL_COUNT))
    
    if [ "$ARCHIVE_FAIL_COUNT" -gt 0 ]; then
        echo "Archive status: COMPLETED WITH ERRORS"
        TOTAL_ARCHIVES_FAILED=$((TOTAL_ARCHIVES_FAILED + 1))
    else
        echo "Archive status: SUCCESS"
        TOTAL_ARCHIVES_PROCESSED=$((TOTAL_ARCHIVES_PROCESSED + 1))
    fi
    
    echo "Archive stats - Total: $ARCHIVE_TOTAL_FILES, Success: $ARCHIVE_SUCCESS_COUNT, Failed: $ARCHIVE_FAIL_COUNT"
    echo "Finished: $(date)"
    
done <<< "$ARCHIVES_TO_PROCESS"

# Cleanup working directory
rm -rf "$WORK_DIR"

# Report overall results
echo ""
echo "========================================"
echo "BATCH COMPLETE - Overall Statistics"
echo "========================================"
echo "Archives processed successfully: $TOTAL_ARCHIVES_PROCESSED"
echo "Archives with failures: $TOTAL_ARCHIVES_FAILED"
echo "Total files converted: $TOTAL_FILES_CONVERTED"
echo "Total files failed: $TOTAL_FILES_FAILED"
echo "Finished at: $(date)"
echo "========================================"

# Exit with error if any archives had failures
if [ "$TOTAL_ARCHIVES_FAILED" -gt 0 ]; then
    echo "WARNING: Some archives had conversion failures"
    exit 1
fi

exit 0