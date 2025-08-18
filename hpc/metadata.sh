#!/bin/bash
#SBATCH --job-name=metadata_process_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/cohenr/logs/metadata/metadata_process_%j.out
#SBATCH --error=/scratch/cohenr/error/metadata/metadata_process_%j.err

# Import .env global configuration
set -a; source config.env; set +a

# Import singularity module
module load singularityce

# User input staging directory
STAGING_DIR=$1

# Validate input
if [ -z "$STAGING_DIR" ]; then
    echo "Error: Please provide staging directory as argument"
    echo "Usage: sbatch metadata.sh /path/to/zurich_2025_january"
    exit 1
fi

# Extract folder name from staging directory path
FOLDER_NAME=$(basename "$STAGING_DIR")

# Parse folder name (format: zurich_2025_january) to create date string
if [[ "$FOLDER_NAME" =~ ^([a-z]+)_([0-9]{4})_([a-z]+)$ ]]; then
    LOCATION="${BASH_REMATCH[1]}"
    YEAR="${BASH_REMATCH[2]}"
    MONTH_NAME="${BASH_REMATCH[3]}"
    
    # Convert month name to number
    case "$MONTH_NAME" in
        january) MONTH="01" ;;
        february) MONTH="02" ;;
        march) MONTH="03" ;;
        april) MONTH="04" ;;
        may) MONTH="05" ;;
        june) MONTH="06" ;;
        july) MONTH="07" ;;
        august) MONTH="08" ;;
        september) MONTH="09" ;;
        october) MONTH="10" ;;
        november) MONTH="11" ;;
        december) MONTH="12" ;;
        *) echo "Error: Invalid month name '$MONTH_NAME'"; exit 1 ;;
    esac
    
    # Create date string for database (use first day of month)
    DATE_STR="${YEAR}-${MONTH}-01"
    
    echo "Processing folder: $FOLDER_NAME"
    echo "Location: $LOCATION, Year: $YEAR, Month: $MONTH"
    echo "Database date string: $DATE_STR"
else
    echo "Error: Folder name '$FOLDER_NAME' does not match expected format 'location_YYYY_month'"
    echo "Example: zurich_2025_january"
    exit 1
fi

# Update database status
singularity run --nv \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing --job-id ${SLURM_JOB_ID}

# Run the main metadata processing
singularity run --nv \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${STAGING_DIR}:/staging \
    --bind ${TEMP_DIR}:/temp \
    --bind ${SECRETS_DIR}:/secrets \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/metadata_process.py \
    --staging-dir "/staging" \
    --db-host "$DB_HOST" \
    --db-password "$DB_PASSWORD" \
    --rsync-user "$RSYNC_USER" \
    --storage-root "$STORAGE_ROOT" \
    --ssh_keyfile "ent.pem"

# Check exit status
if [ $? -eq 0 ]; then
    echo "Metadata processing completed successfully"
    
    # Submit rsync transfer job to run after completion
    TRANSFER_JOB_ID=$(sbatch --dependency=afterok:${SLURM_JOB_ID} \
        --job-name=transfer_parquet_${SLURM_JOB_ID} \
        --output=/scratch/cohenr/logs/metadata/transfer_%j.out \
        --error=/scratch/cohenr/error/metadata/transfer_%j.err \
        --nodes=1 \
        --ntasks=1 \
        --cpus-per-task=4 \
        --mem=8G \
        --time=2:00:00 \
        --export=STAGING_DIR=${STAGING_DIR},DB_HOST=${DB_HOST},RSYNC_USER=${RSYNC_USER},STORAGE_ROOT=${STORAGE_ROOT},SECRETS_DIR=${SECRETS_DIR},AUDIO_PROCESSING_SIF=${AUDIO_PROCESSING_SIF} \
        --wrap="
            # Load singularity module
            module load singularityce
            
            # Transfer all parquet files to cloud storage
            singularity run \
                --bind ${SECRETS_DIR}:/secrets \
                --bind ${STAGING_DIR}:/staging \
                ${AUDIO_PROCESSING_SIF} \
                rsync -avz --progress -e 'ssh -i /secrets/ent.pem -o StrictHostKeyChecking=no' \
                /staging/*.parquet \
                ${RSYNC_USER}@${DB_HOST}:${STORAGE_ROOT}/parquet_files/
            
            # Clean up staging directory after successful transfer
            if [ \$? -eq 0 ]; then
                rm -rf ${STAGING_DIR}
                echo 'Transfer completed successfully, staging directory cleaned'
            else
                echo 'Transfer failed, keeping staging directory for manual intervention'
                exit 1
            fi
        ")
    
    echo "Submitted transfer job: $TRANSFER_JOB_ID"
    
    # Update database
    singularity run --nv \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${AUDIO_PROCESSING_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" completed
else
    echo "Metadata processing failed"
    singularity run --nv \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${AUDIO_PROCESSING_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-processing "$DATE_STR" processing_failed --error "Processing job failed with exit code $?"
fi