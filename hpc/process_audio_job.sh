#!/bin/bash
#SBATCH --job-name=process_audio_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus=H100:1
#SBATCH --mem=128G
#SBATCH --time=18:00:00
#SBATCH --output=/scratch/cohenr/logs/process_%j.out
#SBATCH --error=/scratch/cohenr/error/process_%j.err

module load singularityce

# process_audio_job.sh - Updated for Singularity containers
DATE_STR=$1

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
TEMP_DIR="/shares/bdm.ipz.uzh/audio_storage/staging/${SLURM_JOB_ID}"  # Node-local fast storage

# Storage configuration for rsync
RSYNC_USER="ubuntu"
STORAGE_ROOT="/mnt/storage/audio_storage"
SECRETS_DIR="/data/cohenr/audio_pipeline_secrets"

# Create temp directory on local node
mkdir -p "$TEMP_DIR"
mkdir -p "$STAGING_DIR"

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
    --bind ${SECRETS_DIR}:/secrets \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/hpc_process_day.py \
    --date "$DATE_STR" \
    --staging-dir "/staging" \
    --temp-dir "/temp" \
    --db-host "$DB_HOST" \
    --db-password "$DB_PASSWORD" \
    --rsync-user "$RSYNC_USER" \
    --storage-root "$STORAGE_ROOT" \
    --ssh_keyfile "ent.pem" \
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