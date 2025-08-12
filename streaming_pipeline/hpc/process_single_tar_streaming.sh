#!/bin/bash
#SBATCH --job-name=stream_tar_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --gpus=H100:1
#SBATCH --mem=96G
#SBATCH --time=02:00:00
#SBATCH --output=/scratch/cohenr/logs/stream_tar_%j.out
#SBATCH --error=/scratch/cohenr/error/stream_tar_%j.err

module load singularityce

# process_single_tar_streaming.sh - Worker job for streaming tar processing
DATE_STR=$1
TAR_FILE=$2
TIMESTAMP=$3

# Container paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/streaming_audio_processing.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/streaming_pipeline/src"

# Database connection
DB_HOST="172.23.76.3"
DB_PASSWORD="audio_password"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Set up paths
STAGING_DIR="/shares/bdm.ipz.uzh/audio_storage/staging/${DATE_STR}"
TEMP_DIR="/scratch/cohenr/stream_temp_${SLURM_JOB_ID}_${TIMESTAMP}"

# Storage configuration for rsync
RSYNC_USER="ubuntu"
STORAGE_ROOT="/mnt/storage/audio_storage"
SECRETS_DIR="/data/cohenr/audio_pipeline_secrets"

echo "========================================="
echo "Streaming Tar Processing Job Started"
echo "========================================="
echo "Processing tar file: $TAR_FILE"
echo "Date: $DATE_STR, Timestamp: $TIMESTAMP"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Temporary directory: $TEMP_DIR"
echo "Container: $AUDIO_PROCESSING_SIF"
echo "========================================="

# Validate inputs
if [ -z "$DATE_STR" ] || [ -z "$TAR_FILE" ] || [ -z "$TIMESTAMP" ]; then
    echo "Error: Missing required parameters"
    echo "Usage: $0 <date> <tar_file> <timestamp>"
    exit 1
fi

if [ ! -f "$TAR_FILE" ]; then
    echo "Error: Tar file does not exist: $TAR_FILE"
    exit 1
fi

# Check available resources
echo "Resource Information:"
echo "CPU cores available: $(nproc)"
echo "Memory available: $(free -h | grep '^Mem:' | awk '{print $7}')"
echo "Disk space in temp: $(df -h $(dirname $TEMP_DIR) | tail -1 | awk '{print $4}')"

# Check GPU status
echo "GPU Status:"
nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu --format=csv,noheader

# Create unique temp directory for this job
mkdir -p "$TEMP_DIR"
echo "Created temp directory: $TEMP_DIR"

# Check container exists
if [ ! -f "$AUDIO_PROCESSING_SIF" ]; then
    echo "Error: Streaming container not found: $AUDIO_PROCESSING_SIF"
    echo "Please build streaming containers first"
    exit 1
fi

# Run the streaming tar processing with GPU support
echo "Starting streaming processing at $(date)..."
echo "Command: singularity run --nv ..."

start_time=$(date +%s)

singularity run --nv \
    --bind ${SCRIPT_DIR}:/opt/streaming_pipeline/src \
    --bind ${STAGING_DIR}:/staging \
    --bind ${TEMP_DIR}:/temp \
    --bind ${SECRETS_DIR}:/secrets \
    ${AUDIO_PROCESSING_SIF} \
    python3 /opt/streaming_pipeline/src/streaming_tar_processor.py \
    --date "$DATE_STR" \
    --tar-file "$TAR_FILE" \
    --timestamp "$TIMESTAMP" \
    --temp-dir "/temp" \
    --db-host "$DB_HOST" \
    --db-password "$DB_PASSWORD" \
    --rsync-user "$RSYNC_USER" \
    --storage-root "$STORAGE_ROOT" \
    --ssh_keyfile "ent.pem" \
    --stream-batch-size 200 \
    --process-batch-size 100 \
    --num-workers 10 \
    --max-concurrent-uploads 8 \
    --upload-timeout 120

# Check exit status
exit_code=$?
end_time=$(date +%s)
duration=$((end_time - start_time))

echo "========================================="
echo "Processing completed at $(date)"
echo "Duration: ${duration} seconds"
echo "Exit code: $exit_code"

if [ $exit_code -eq 0 ]; then
    echo "Successfully processed tar file: $TAR_FILE"
    echo "All files processed and uploads queued"
else
    echo "Failed to process tar file: $TAR_FILE (exit code: $exit_code)"
fi

# Show final resource usage
echo "Final Resource Status:"
echo "GPU Status:"
nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu --format=csv,noheader

echo "Memory usage:"
free -h

echo "Disk usage in temp:"
du -sh "$TEMP_DIR" 2>/dev/null || echo "Temp directory already cleaned"

# Clean up temp directory
echo "Cleaning up temporary directory: $TEMP_DIR"
rm -rf "$TEMP_DIR"

echo "========================================="
echo "Streaming tar processing job completed"
echo "========================================="

exit $exit_code