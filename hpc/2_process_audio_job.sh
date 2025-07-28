#!/bin/bash
#SBATCH --job-name=process_audio_%j
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --gpus=h100:1
#SBATCH --mem=256G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/user/logs/process_%j.out

# process_audio_job.sh - Updated for Singularity containers
DATE_STR=$1

# Container paths
CONTAINER_DIR="/shares/bdm.ipz.uzh/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/audio_processing.sif"
SCRIPT_DIR="/shares/bdm.ipz.uzh/audio_pipeline/src"

# Database connection
DB_HOST="172.23.76.3"
DB_PASSWORD="audio_password"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Set up paths
STAGING_DIR="/scratch/cohenr/audio_storage/staging/${DATE_STR}"
TEMP_DIR="/scratch/cohenr/audio_storage/${SLURM_JOB_ID}"  # Node-local fast storage

# Storage configuration for rsync
RSYNC_USER="audio_user"
STORAGE_ROOT="/opt/audio_storage"

# Create temp directory on local node
mkdir -p "$TEMP_DIR"

# Update database status
singularity run --nv \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing --job-id ${SLURM_JOB_ID}

# Run the main processing with GPU support
singularity run --nv \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${STAGING_DIR}:/staging \
    --bind ${TEMP_DIR}:/temp \
    --bind /scratch:/scratch \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/hpc_process_day.py \
    --date "$DATE_STR" \
    --staging-dir "/staging" \
    --temp-dir "/temp" \
    --db-host "$DB_HOST" \
    --db-password "$DB_PASSWORD" \
    --rsync-user "$RSYNC_USER" \
    --storage-root "$STORAGE_ROOT" \
    --batch-size 100 \
    --num-workers 32

# Check exit status
if [ $? -eq 0 ]; then
    # Clean up staging directory
    rm -rf "$STAGING_DIR"
    
    # Update database
    singularity run --nv \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${AUDIO_PROCESSING_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed
else
    singularity run --nv \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${AUDIO_PROCESSING_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "Processing job failed with exit code $?"
fi

# Clean up temp directory
rm -rf "$TEMP_DIR"