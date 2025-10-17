#!/bin/bash
#SBATCH --job-name=parallel_extract
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --array=0-949
#SBATCH --time=24:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G

# Parallel Extract and Convert MP3s to Opus using Slurm Array Jobs
# Usage: sbatch parallel_extract_convert.sh <archive_directory> <output_directory>

set -euo pipefail

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <archive_directory> <output_directory>"
    exit 1
fi

ARCHIVE_DIR="$1"
OUTPUT_DIR="$2"

# Validate inputs
if [ ! -d "$ARCHIVE_DIR" ]; then
    echo "Error: Archive directory '$ARCHIVE_DIR' does not exist"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Number of parallel jobs per task (use available CPUs)
PARALLEL_JOBS=${SLURM_CPUS_PER_TASK:-8}

# Number of archives per array task
ARCHIVES_PER_TASK=12

# Get task ID (default to 0 if not in Slurm environment)
TASK_ID=${SLURM_ARRAY_TASK_ID:-0}

echo "====================================="
echo "Task ID: $TASK_ID"
echo "Archive directory: $ARCHIVE_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Parallel jobs: $PARALLEL_JOBS"
echo "Archives per task: $ARCHIVES_PER_TASK"
echo "Started at: $(date)"
echo "====================================="

# Function to convert MP3 to Opus
convert_mp3_to_opus() {
    local mp3_file="$1"
    local output_dir="$2"
    local archive_name="$3"

    # Get base filename without extension
    local base_name=$(basename "$mp3_file" .mp3)

    # Create archive-specific output directory
    local archive_output_dir="$output_dir/$archive_name"
    mkdir -p "$archive_output_dir"

    # Output opus file path
    local opus_file="$archive_output_dir/${base_name}.opus"

    # Skip if already converted
    if [ -f "$opus_file" ]; then
        echo "Skipping $mp3_file (already converted)"
        return 0
    fi

    # Convert using ffmpeg
    ffmpeg -i "$mp3_file" \
        -c:a libopus \
        -b:a 64k \
        -application voip \
        -compression_level 5 \
        -ar 16000 \
        "$opus_file" \
        -y \
        -loglevel error \
        2>&1 || {
            echo "Error converting $mp3_file" >&2
            return 1
        }

    echo "Converted: $mp3_file -> $opus_file"
    return 0
}

export -f convert_mp3_to_opus

# Find all tar archives (.tar and .tar.gz) and sort them for consistent ordering
mapfile -t ALL_ARCHIVES < <(find "$ARCHIVE_DIR" \( -name "*.tar" -o -name "*.tar.gz" \) -type f | sort)

TOTAL_ARCHIVES=${#ALL_ARCHIVES[@]}

echo "Total archives found: $TOTAL_ARCHIVES"

# Calculate which archives this task should process
START_IDX=$((TASK_ID * ARCHIVES_PER_TASK))
END_IDX=$((START_IDX + ARCHIVES_PER_TASK))

# Ensure we don't go past the end
if [ $START_IDX -ge $TOTAL_ARCHIVES ]; then
    echo "Task ID $TASK_ID: No archives to process (start index $START_IDX >= total $TOTAL_ARCHIVES)"
    exit 0
fi

if [ $END_IDX -gt $TOTAL_ARCHIVES ]; then
    END_IDX=$TOTAL_ARCHIVES
fi

echo "Task ID $TASK_ID: Processing archives $START_IDX to $((END_IDX - 1))"

# Process assigned archives
for (( i=START_IDX; i<END_IDX; i++ )); do
    tar_file="${ALL_ARCHIVES[$i]}"

    echo ""
    echo "----------------------------------------"
    echo "Processing archive $((i + 1))/$TOTAL_ARCHIVES: $tar_file"
    echo "----------------------------------------"

    # Get archive name without extension
    archive_name=$(basename "$tar_file" .tar.gz)
    archive_name=$(basename "$archive_name" .tar)

    # Create temporary directory for extraction
    temp_dir=$(mktemp -d -t extract_${archive_name}_${TASK_ID}_XXXXXX)

    echo "Extracting to temporary directory: $temp_dir"

    # Extract tar archive
    tar -xf "$tar_file" -C "$temp_dir" 2>&1 || {
        echo "Error extracting $tar_file" >&2
        rm -rf "$temp_dir"
        continue
    }

    # Find all MP3 files in the extracted directory
    mp3_count=$(find "$temp_dir" -name "*.mp3" -type f | wc -l)
    echo "Found $mp3_count MP3 files in $archive_name"

    if [ "$mp3_count" -eq 0 ]; then
        echo "No MP3 files found in $tar_file"
        rm -rf "$temp_dir"
        continue
    fi

    # Convert all MP3 files in parallel
    find "$temp_dir" -name "*.mp3" -type f | \
        xargs -P "$PARALLEL_JOBS" -I {} bash -c \
            "convert_mp3_to_opus '{}' '$OUTPUT_DIR' '$archive_name'"

    # Clean up temporary directory
    echo "Cleaning up temporary directory: $temp_dir"
    rm -rf "$temp_dir"

    echo "Completed processing archive: $archive_name"
done

echo ""
echo "====================================="
echo "Task ID $TASK_ID: All assigned archives processed"
echo "Completed at: $(date)"
echo "====================================="
