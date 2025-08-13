#!/bin/bash
#SBATCH --job-name=parallel_master_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --partition=standard
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=14:00:00
#SBATCH --output=/scratch/cohenr/logs/parallel_master_%j.out
#SBATCH --error=/scratch/cohenr/error/parallel_master_%j.err

module load singularityce

# process_day_parallel.sh - Master controller for parallel tar.xz processing
DATE_STR=$1

# Container paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/src"
PIPELINE_DIR="/data/cohenr/audio_pipeline/hpc"

# Database connection
DB_HOST="172.23.76.3"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Paths
STAGING_DIR="/shares/bdm.ipz.uzh/audio_storage/staging/${DATE_STR}"

echo "Starting parallel processing for date: ${DATE_STR}"
echo "Staging directory: ${STAGING_DIR}"

# Validate date format and staging directory
if [[ ! "$DATE_STR" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

if [ ! -d "$STAGING_DIR" ]; then
    echo "Error: Staging directory does not exist: $STAGING_DIR"
    exit 1
fi

# Update database status to processing
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing --job-id ${SLURM_JOB_ID}

# Discover all tar.xz files for this date
TAR_FILES=($(find "$STAGING_DIR" -name "0_${DATE_STR}_*.tar.xz" | sort))
NUM_FILES=${#TAR_FILES[@]}

echo "Found ${NUM_FILES} tar.xz files to process"

if [ "$NUM_FILES" -eq 0 ]; then
    echo "No tar.xz files found for date $DATE_STR"
    singularity run \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "No tar.xz files found"
    exit 1
fi

# Submit worker jobs for each tar file
WORKER_JOB_IDS=()
MAX_CONCURRENT_JOBS=12  # Adjust based on available GPU nodes
SUBMITTED_JOBS=0

for tar_file in "${TAR_FILES[@]}"; do
    # Extract timestamp from filename (e.g., 0_2025-01-31_23_50.tar.xz -> 23_50)
    filename=$(basename "$tar_file")
    timestamp=$(echo "$filename" | sed 's/0_[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}_\([0-9]\{2\}_[0-9]\{2\}\)\.tar\.xz/\1/')
    
    echo "Submitting worker job for ${filename} (timestamp: ${timestamp})"
    
    # Submit worker job
    job_output=$(sbatch --parsable "${PIPELINE_DIR}/process_single_tar.sh" "$DATE_STR" "$tar_file" "$timestamp")
    worker_job_id=$(echo "$job_output" | tail -n1)
    WORKER_JOB_IDS+=($worker_job_id)
    
    echo "Submitted job $worker_job_id for $filename"
    
    SUBMITTED_JOBS=$((SUBMITTED_JOBS + 1))
    
    # Throttle job submission to avoid overwhelming the scheduler
    if [ $((SUBMITTED_JOBS % MAX_CONCURRENT_JOBS)) -eq 0 ] && [ $SUBMITTED_JOBS -lt $NUM_FILES ]; then
        echo "Throttling: submitted $SUBMITTED_JOBS jobs, waiting 30 seconds..."
        sleep 30
    fi
done

echo "Submitted ${#WORKER_JOB_IDS[@]} worker jobs: ${WORKER_JOB_IDS[*]}"

# Monitor worker jobs
echo "Monitoring worker job completion..."
CHECK_INTERVAL=60  # Check every minute
MAX_WAIT_TIME=7200  # 2 hours maximum wait
elapsed_time=0

while [ $elapsed_time -lt $MAX_WAIT_TIME ]; do
    # Check status of all worker jobs
    completed_jobs=0
    failed_jobs=0
    running_jobs=0
    
    for job_id in "${WORKER_JOB_IDS[@]}"; do
        # Get job state (COMPLETED, FAILED, RUNNING, PENDING, etc.)
        job_state=$(squeue -j "$job_id" -h -o "%T" 2>/dev/null || echo "COMPLETED")
        
        case "$job_state" in
            "COMPLETED")
                completed_jobs=$((completed_jobs + 1))
                ;;
            "FAILED"|"CANCELLED"|"TIMEOUT"|"NODE_FAIL")
                failed_jobs=$((failed_jobs + 1))
                ;;
            "RUNNING"|"PENDING"|"CONFIGURING")
                running_jobs=$((running_jobs + 1))
                ;;
        esac
    done
    
    total_finished=$((completed_jobs + failed_jobs))
    echo "Progress: ${completed_jobs} completed, ${failed_jobs} failed, ${running_jobs} running (${total_finished}/${#WORKER_JOB_IDS[@]} finished)"
    
    # Check if all jobs are finished
    if [ $total_finished -eq ${#WORKER_JOB_IDS[@]} ]; then
        echo "All worker jobs finished"
        break
    fi
    
    sleep $CHECK_INTERVAL
    elapsed_time=$((elapsed_time + CHECK_INTERVAL))
done

# Final status check
if [ $elapsed_time -ge $MAX_WAIT_TIME ]; then
    echo "Timeout: Not all jobs completed within $MAX_WAIT_TIME seconds"
    # Cancel any remaining jobs
    for job_id in "${WORKER_JOB_IDS[@]}"; do
        scancel "$job_id" 2>/dev/null
    done
    singularity run \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "Worker jobs timed out"
    exit 1
fi

# Process metadata and finalize
echo "Processing metadata and comments for the day..."
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${STAGING_DIR}:/staging \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/parallel_coordinator.py process-metadata \
    --date "$DATE_STR" \
    --staging-dir "/staging" \
    --db-string "$DB_CREDS"

# Check final processing status and update database
if [ $failed_jobs -eq 0 ]; then
    echo "All worker jobs completed successfully"
    singularity run \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed
    
    # Clean up staging directory
    echo "Cleaning up staging directory..."
    rm -rf "$STAGING_DIR"
    
    exit_code=0
else
    echo "Some worker jobs failed: $failed_jobs failures out of ${#WORKER_JOB_IDS[@]} jobs"
    singularity run \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed_with_errors --error "$failed_jobs worker jobs failed"
    
    exit_code=1
fi

echo "Parallel processing completed for $DATE_STR"
exit $exit_code