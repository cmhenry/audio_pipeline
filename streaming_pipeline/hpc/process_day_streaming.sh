#!/bin/bash
#SBATCH --job-name=stream_master_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --partition=standard
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --output=/scratch/cohenr/logs/stream_master_%j.out
#SBATCH --error=/scratch/cohenr/error/stream_master_%j.err

module load singularityce

# process_day_streaming.sh - Master controller for streaming parallel processing
DATE_STR=$1

# Container paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/streaming_pipeline/src"
PIPELINE_DIR="/data/cohenr/audio_pipeline/streaming_pipeline/hpc"

# Database connection
DB_HOST="172.23.76.3"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Paths
STAGING_DIR="/shares/bdm.ipz.uzh/audio_storage/staging/${DATE_STR}"

echo "=============================================="
echo "Streaming Pipeline Master Controller Started"
echo "=============================================="
echo "Date: ${DATE_STR}"
echo "Staging directory: ${STAGING_DIR}"
echo "Master Job ID: ${SLURM_JOB_ID}"
echo "Node: $(hostname)"
echo "=============================================="

# Validate date format and staging directory
if [[ ! "$DATE_STR" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

if [ ! -d "$STAGING_DIR" ]; then
    echo "Error: Staging directory does not exist: $STAGING_DIR"
    exit 1
fi

# Update database status to processing (streaming)
echo "Updating processing status in database..."
singularity run \
    --bind /data/cohenr/audio_pipeline/src:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    python3 /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing --job-id ${SLURM_JOB_ID}

# Discover all tar.xz files for this date
echo "Discovering tar.xz files..."
TAR_FILES=($(find "$STAGING_DIR" -name "0_${DATE_STR}_*.tar.xz" | sort))
NUM_FILES=${#TAR_FILES[@]}

echo "Found ${NUM_FILES} tar.xz files to process"
echo "Files:"
for i in "${!TAR_FILES[@]}"; do
    echo "  $((i+1)). $(basename ${TAR_FILES[$i]})"
done

if [ "$NUM_FILES" -eq 0 ]; then
    echo "No tar.xz files found for date $DATE_STR"
    singularity run \
        --bind /data/cohenr/audio_pipeline/src:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        python3 /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "No tar.xz files found"
    exit 1
fi

# Submit streaming worker jobs for each tar file
echo "=============================================="
echo "Submitting streaming worker jobs..."
echo "=============================================="

WORKER_JOB_IDS=()
MAX_CONCURRENT_JOBS=8  # Reduced for streaming (uses more resources per job)
SUBMITTED_JOBS=0

for tar_file in "${TAR_FILES[@]}"; do
    # Extract timestamp from filename (e.g., 0_2025-01-31_23_50.tar.xz -> 23_50)
    filename=$(basename "$tar_file")
    timestamp=$(echo "$filename" | sed 's/0_[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}_\([0-9]\{2\}_[0-9]\{2\}\)\.tar\.xz/\1/')
    
    echo "Submitting streaming worker for ${filename} (timestamp: ${timestamp})"
    
    # Submit streaming worker job
    job_output=$(sbatch --parsable "${PIPELINE_DIR}/process_single_tar_streaming.sh" "$DATE_STR" "$tar_file" "$timestamp")
    worker_job_id=$(echo "$job_output" | tail -n1)
    WORKER_JOB_IDS+=($worker_job_id)
    
    echo "  ✓ Submitted streaming job $worker_job_id"
    
    SUBMITTED_JOBS=$((SUBMITTED_JOBS + 1))
    
    # Throttle job submission to avoid overwhelming the scheduler
    if [ $((SUBMITTED_JOBS % MAX_CONCURRENT_JOBS)) -eq 0 ] && [ $SUBMITTED_JOBS -lt $NUM_FILES ]; then
        echo "Throttling: submitted $SUBMITTED_JOBS jobs, waiting 45 seconds for resource availability..."
        sleep 45
    fi
done

echo "=============================================="
echo "Submitted ${#WORKER_JOB_IDS[@]} streaming worker jobs"
echo "Job IDs: ${WORKER_JOB_IDS[*]}"
echo "=============================================="

# Monitor streaming worker jobs
echo "Monitoring streaming worker job completion..."
CHECK_INTERVAL=90  # Check every 1.5 minutes (streaming jobs are longer)
MAX_WAIT_TIME=10800  # 3 hours maximum wait
elapsed_time=0
last_report=0

while [ $elapsed_time -lt $MAX_WAIT_TIME ]; do
    # Check status of all worker jobs
    completed_jobs=0
    failed_jobs=0
    running_jobs=0
    pending_jobs=0
    
    for job_id in "${WORKER_JOB_IDS[@]}"; do
        # Get job state
        job_state=$(squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "COMPLETED")
        
        case "$job_state" in
            "COMPLETED")
                completed_jobs=$((completed_jobs + 1))
                ;;
            "FAILED"|"CANCELLED"|"TIMEOUT"|"NODE_FAIL"|"OUT_OF_MEMORY")
                failed_jobs=$((failed_jobs + 1))
                ;;
            "RUNNING")
                running_jobs=$((running_jobs + 1))
                ;;
            "PENDING"|"CONFIGURING")
                pending_jobs=$((pending_jobs + 1))
                ;;
        esac
    done
    
    total_finished=$((completed_jobs + failed_jobs))
    
    # Report progress every 5 minutes
    if [ $((elapsed_time - last_report)) -ge 300 ]; then
        echo "Progress Update ($(date)):"
        echo "  ✓ Completed: ${completed_jobs}"
        echo "  ✗ Failed: ${failed_jobs}"
        echo "  ⚡ Running: ${running_jobs}"
        echo "  ⏳ Pending: ${pending_jobs}"
        echo "  📊 Total finished: ${total_finished}/${#WORKER_JOB_IDS[@]}"
        echo "  ⏱️  Elapsed: ${elapsed_time}s / ${MAX_WAIT_TIME}s"
        last_report=$elapsed_time
    fi
    
    # Check if all jobs are finished
    if [ $total_finished -eq ${#WORKER_JOB_IDS[@]} ]; then
        echo "=============================================="
        echo "All streaming worker jobs finished!"
        echo "Final Results:"
        echo "  ✓ Completed: ${completed_jobs}"
        echo "  ✗ Failed: ${failed_jobs}"
        echo "  📊 Total: ${#WORKER_JOB_IDS[@]}"
        echo "=============================================="
        break
    fi
    
    sleep $CHECK_INTERVAL
    elapsed_time=$((elapsed_time + CHECK_INTERVAL))
done

# Handle timeout
if [ $elapsed_time -ge $MAX_WAIT_TIME ]; then
    echo "=============================================="
    echo "❌ TIMEOUT: Not all jobs completed within $MAX_WAIT_TIME seconds"
    echo "Cancelling remaining jobs..."
    for job_id in "${WORKER_JOB_IDS[@]}"; do
        scancel "$job_id" 2>/dev/null
    done
    singularity run \
        --bind /data/cohenr/audio_pipeline/src:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        python3 /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "Streaming worker jobs timed out"
    echo "=============================================="
    exit 1
fi

# Process metadata and finalize
echo "Processing metadata and comments for the day..."
echo "(Using regular metadata processing since streaming only affects audio processing)"
singularity run \
    --bind /data/cohenr/audio_pipeline/src:/opt/audio_pipeline/src \
    --bind ${STAGING_DIR}:/staging \
    ${PIPELINE_UTILS_SIF} \
    python3 /opt/audio_pipeline/src/parallel_coordinator.py process-metadata \
    --date "$DATE_STR" \
    --staging-dir "/staging" \
    --db-string "$DB_CREDS"

# Check final processing status and update database
echo "=============================================="
if [ $failed_jobs -eq 0 ]; then
    echo "🎉 All streaming worker jobs completed successfully!"
    singularity run \
        --bind /data/cohenr/audio_pipeline/src:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        python3 /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed
    
    # Clean up staging directory
    echo "Cleaning up staging directory..."
    rm -rf "$STAGING_DIR"
    
    exit_code=0
    echo "✅ Streaming processing completed successfully for $DATE_STR"
else
    echo "⚠️  Some streaming worker jobs failed: $failed_jobs failures out of ${#WORKER_JOB_IDS[@]} jobs"
    singularity run \
        --bind /data/cohenr/audio_pipeline/src:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        python3 /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed_with_errors --error "$failed_jobs streaming worker jobs failed"
    
    exit_code=1
    echo "⚠️  Streaming processing completed with errors for $DATE_STR"
fi

echo "=============================================="
echo "Streaming Pipeline Master Controller Finished"
echo "Date: $DATE_STR"
echo "Duration: ${elapsed_time} seconds"
echo "Results: ${completed_jobs} successful, ${failed_jobs} failed"
echo "=============================================="

exit $exit_code