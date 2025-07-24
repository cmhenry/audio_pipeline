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

# process_audio_job.sh
DATE_STR=$1

# Load required modules
# module load cuda/12.0
# module load python/3.10
# module load ffmpeg
module load gpu cudnn/8.9.7
module load mamba

# Activate virtual environment
# source /home/user/audio_env/bin/activate
mamba activate torch

# Database connection
DB_HOST="172.23.76.3"
DB_PASSWORD="audio_password"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Set up paths
STAGING_DIR="/scratch/cohenr/audio_storage/staging/${DATE_STR}"
TEMP_DIR="/scratch/cohenr/audio_storage/${SLURM_JOB_ID}"  # Node-local fast storage
# CLOUD_STORAGE_CONFIG="/home/user/.cloud_storage_config"

# Create temp directory on local node
mkdir -p "$TEMP_DIR"

# Update database
# psql "$DB_CREDS" -c "
#     UPDATE processing_queue 
#     SET status = 'processing',
#         processing_start = NOW(),
#         slurm_job_id = ${SLURM_JOB_ID}
#     WHERE year = ${YEAR} AND month = ${MONTH}::int AND date = ${DAY}::int;
# "
python ${SCRIPT_DIR}/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing --job-id ${SLURM_JOB_ID}

# Run the main processing
python /home/user/audio_pipeline/hpc_process_day.py \
    --date "$DATE_STR" \
    --staging-dir "$STAGING_DIR" \
    --temp-dir "$TEMP_DIR" \
    --db-host "$DB_HOST" \
    --db-password "$DB_PASSWORD" \
    --cloud-config "$CLOUD_STORAGE_CONFIG" \
    --batch-size 100 \
    --num-workers 32

# Check exit status
if [ $? -eq 0 ]; then
    # Clean up staging directory
    rm -rf "$STAGING_DIR"
    
    # Update database
    python ${SCRIPT_DIR}/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed
else
    python ${SCRIPT_DIR}/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "Processing job failed with exit code $?"
fi

# Clean up temp directory
rm -rf "$TEMP_DIR"