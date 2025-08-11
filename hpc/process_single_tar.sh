#!/bin/bash
#SBATCH --job-name=process_tar_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gpus=H100:1
#SBATCH --mem=64G
#SBATCH --time=01:00:00
#SBATCH --output=/scratch/cohenr/logs/process_tar_%j.out
#SBATCH --error=/scratch/cohenr/error/process_tar_%j.err

module load singularityce

# process_single_tar.sh - Worker job for processing individual tar.xz files
DATE_STR=$1
TAR_FILE=$2
TIMESTAMP=$3

# Container paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/audio_processing.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/src"

# Database connection
DB_HOST="172.23.76.3"
DB_PASSWORD="audio_password"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Set up paths
STAGING_DIR="/shares/bdm.ipz.uzh/audio_storage/staging/${DATE_STR}"
TEMP_DIR="/scratch/cohenr/temp_${SLURM_JOB_ID}_${TIMESTAMP}"

# Storage configuration for rsync
RSYNC_USER="ubuntu"
STORAGE_ROOT="/mnt/storage/audio_storage"
SECRETS_DIR="/data/cohenr/audio_pipeline_secrets"

echo "Processing tar file: $TAR_FILE"
echo "Date: $DATE_STR, Timestamp: $TIMESTAMP"
echo "Job ID: $SLURM_JOB_ID"
echo "Temporary directory: $TEMP_DIR"

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

# Create unique temp directory for this job
mkdir -p "$TEMP_DIR"

# Check available GPU memory
echo "GPU Status:"
nvidia-smi

# Run the single tar processing with GPU support
echo "Starting processing..."
singularity run --nv \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${STAGING_DIR}:/staging \
    --bind ${TEMP_DIR}:/temp \
    --bind ${SECRETS_DIR}:/secrets \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/hpc_process_single_tar.py \
    --date "$DATE_STR" \
    --tar-file "$TAR_FILE" \
    --timestamp "$TIMESTAMP" \
    --temp-dir "/temp" \
    --db-host "$DB_HOST" \
    --db-password "$DB_PASSWORD" \
    --rsync-user "$RSYNC_USER" \
    --storage-root "$STORAGE_ROOT" \
    --ssh_keyfile "ent.pem" \
    --batch-size 500 \
    --num-workers 8

# Check exit status
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Successfully processed tar file: $TAR_FILE"
else
    echo "Failed to process tar file: $TAR_FILE (exit code: $exit_code)"
fi

# Clean up temp directory
echo "Cleaning up temporary directory: $TEMP_DIR"
rm -rf "$TEMP_DIR"

# Final GPU status
echo "Final GPU Status:"
nvidia-smi

exit $exit_code