#!/bin/bash
#SBATCH --job-name=transfer_%j
#SBATCH --partition=standard
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=06:00:00
#SBATCH --output=/scratch/user/logs/transfer_%j.out

# globus_transfer_job.sh - Updated for Singularity containers
DATE_STR=$1  # Format: YYYY-MM-DD
YEAR=$(echo $DATE_STR | cut -d'-' -f1)
MONTH=$(echo $DATE_STR | cut -d'-' -f2)
DAY=$(echo $DATE_STR | cut -d'-' -f3)

# Database connection
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Container paths
CONTAINER_DIR="/shares/bdm.ipz.uzh/audio_pipeline/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
SCRIPT_DIR="/shares/bdm.ipz.uzh/audio_pipeline/src"

# Convert month number to name (e.g., 01 -> january)
MONTH_NAME=$(date -d "${YEAR}-${MONTH}-01" +%B | tr '[:upper:]' '[:lower:]')

# Construct folder name (e.g., "zurich_2025_january")
LOCATION="zurich"  # Make this configurable if needed
FOLDER_NAME="${LOCATION}_${YEAR}_${MONTH_NAME}"
# Get folder name
# FOLDER_NAME=$(python ${SCRIPT_DIR}/db_utils.py --db-string "$DB_CREDS" get-folder $YEAR ${MONTH#0} --location "$LOCATION")

echo "Processing folder: ${FOLDER_NAME}"
# echo "Source folder: ${FOLDER_NAME}"

# Set up paths
SOURCE_ENDPOINT="7f1a1170-3e31-4241-864e-e504e736c7b8"
DEST_ENDPOINT="5f01d3f8-0697-11e8-a6c0-0a448319c2f8"
SOURCE_PATH="/scratch/olympus/tiktok_external_transfer/${FOLDER_NAME}/"
DEST_PATH="/shares/bdm.ipz.uzh/audio_pipeline/staging/${DATE_STR}/"

# Create destination directory
mkdir -p "$(dirname $DEST_PATH)"

# Update database status
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transferring

# Create a file list for all files from this specific date
FILELIST="/tmp/globus_files_${DATE_STR}.txt"
> "$FILELIST"

# Generate list of expected files for this date
# Files are in format: 0_YYYY-MM-DD_HH_MM.tar.xz and associated parquet files
for hour in $(seq -f "%02g" 0 23); do
    for minute in 00 10 20 30 40 50; do
        # Add all file types for this timestamp
        echo "${SOURCE_PATH}0_${DATE_STR}_${hour}_${minute}.tar.xz ${DEST_PATH}" >> "$FILELIST"
        echo "${SOURCE_PATH}0_${DATE_STR}_${hour}_${minute}_comments.parquet ${DEST_PATH}" >> "$FILELIST"
        echo "${SOURCE_PATH}0_${DATE_STR}_${hour}_${minute}_metadata.parquet ${DEST_PATH}" >> "$FILELIST"
        echo "${SOURCE_PATH}0_${DATE_STR}_${hour}_${minute}_subtitles.parquet ${DEST_PATH}" >> "$FILELIST"
    done
done

# Count expected files (144 timestamps * 4 files = 576 files per day)
EXPECTED_FILES=576

# Initiate batch transfer using file list
echo "Transferring ${EXPECTED_FILES} files for ${DATE_STR} from ${FOLDER_NAME}"

TASK_ID=$(singularity exec ${PIPELINE_UTILS_SIF} \
    globus transfer \
    "${SOURCE_ENDPOINT}" \
    "${DEST_ENDPOINT}" \
    --batch < "$FILELIST" \
    --label "Audio_${DATE_STR}" \
    --skip-source-errors \
    --format json | jq -r '.task_id')

# Store task ID in database
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transferring --task-id "$TASK_ID"

# Monitor transfer
CHECK_INTERVAL=300  # 5 minutes
MAX_CHECKS=72       # 6 hours max

for i in $(seq 1 $MAX_CHECKS); do
    sleep $CHECK_INTERVAL
    
    # Get transfer status
    TASK_INFO=$(singularity exec ${PIPELINE_UTILS_SIF} globus task show $TASK_ID --format json)
    STATUS=$(echo "$TASK_INFO" | jq -r '.status')
    FILES_TRANSFERRED=$(echo "$TASK_INFO" | jq -r '.files_transferred // 0')
    
    echo "Check $i/$MAX_CHECKS: Status=$STATUS, Files transferred=$FILES_TRANSFERRED/$EXPECTED_FILES"
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        # Verify we got most files (allow some missing)
        if [ "$FILES_TRANSFERRED" -ge $((EXPECTED_FILES * 90 / 100)) ]; then
            echo "Transfer successful: $FILES_TRANSFERRED files transferred"
            
            # Update database
            # psql "$DB_CREDS" -c "
            #     UPDATE processing_queue 
            #     SET status = 'ready_to_process',
            #         transfer_end = NOW(),
            #         error_message = CASE 
            #             WHEN ${FILES_TRANSFERRED} < ${EXPECTED_FILES} 
            #             THEN 'Partial transfer: ' || ${FILES_TRANSFERRED} || '/' || ${EXPECTED_FILES} || ' files'
            #             ELSE NULL 
            #         END
            #     WHERE year = ${YEAR} AND month = ${MONTH}::int AND date = ${DAY}::int;
            # "
            singularity run \
                --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
                ${PIPELINE_UTILS_SIF} \
                /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" ready_to_process
            
            # Submit processing job
            sbatch "${SCRIPT_DIR}/process_audio_job.sh" "$DATE_STR"
            exit 0
        else
            echo "Transfer incomplete: only $FILES_TRANSFERRED/$EXPECTED_FILES files"
            # psql "$DB_CREDS" -c "
            #     UPDATE processing_queue 
            #     SET status = 'transfer_failed',
            #         error_message = 'Insufficient files transferred: ' || ${FILES_TRANSFERRED} || '/' || ${EXPECTED_FILES}
            #     WHERE year = ${YEAR} AND month = ${MONTH}::int AND date = ${DAY}::int;
            # "
            singularity run \
                --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
                ${PIPELINE_UTILS_SIF} \
                /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Transfer failed: $ERROR_MSG"
            exit 1
        fi
        
    elif [ "$STATUS" = "FAILED" ]; then
        ERROR_MSG=$(echo "$TASK_INFO" | jq -r '.nice_status_details // .status')
        echo "Transfer failed: $ERROR_MSG"
        # psql "$DB_CREDS" -c "
        #     UPDATE processing_queue 
        #     SET status = 'transfer_failed',
        #         error_message = '${ERROR_MSG}'
        #     WHERE year = ${YEAR} AND month = ${MONTH}::int AND date = ${DAY}::int;
        # "
        singularity run \
            --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
            ${PIPELINE_UTILS_SIF} \
            /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Transfer failed: $ERROR_MSG"
        exit 1
    fi
done

# Timeout
echo "Transfer timed out after ${MAX_CHECKS} checks"
# psql "$DB_CREDS" -c "
#     UPDATE processing_queue 
#     SET status = 'transfer_failed',
#         error_message = 'Transfer timeout'
#     WHERE year = ${YEAR} AND month = ${MONTH}::int AND date = ${DAY}::int;
# 
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Transfer timeout"
exit 1