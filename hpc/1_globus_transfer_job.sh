#!/bin/bash
#SBATCH --job-name=transfer_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=colin.henry@ipz.uzh.ch
#SBATCH --partition=standard
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=06:00:00
#SBATCH --output=/scratch/cohenr/logs/transfer_%j.out
#SBATCH --error=/scratch/cohenr/error/transfer_%j.err

module load singularityce

# globus_transfer_job.sh - Updated for Singularity containers
DATE_STR=$1  # Format: YYYY-MM-DD
YEAR=$(echo $DATE_STR | cut -d'-' -f1)
MONTH=$(echo $DATE_STR | cut -d'-' -f2)
DAY=$(echo $DATE_STR | cut -d'-' -f3)

# Database connection
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Container paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/src"

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
SOURCE_PATH="/scratch/olympus/tiktok_external_transfer/${FOLDER_NAME}/prepped_files/"
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
echo "Finding file list at ${SOURCE_ENDPOINT}"
singularity exec ${PIPELINE_UTILS_SIF} \
    globus ls \
    "${SOURCE_ENDPOINT}":"${SOURCE_PATH}" \
    | grep "${DATE_STR}" >> "$FILELIST"
echo "File list found at ${SOURCE_ENDPOINT}"

# Count expected files (144 timestamps * 4 files = 576 files per day)
EXPECTED_FILES=$(wc -l < "$FILELIST")
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

# TODO: Replace above chunk with Globus Flow
# echo "Initiating transfer Flow for ${DATE_STR}"
# # Call the Flow via API
# FLOW_RESPONSE=$(curl -s -X POST \
#     "https://flows.globus.org/v1/flows/YOUR_FLOW_ID/run" \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H "Content-Type: application/json" \
#     -d '{
#         "body": {
#             "input": {
#                 "date_str": "'$DATE_STR'",
#                 "source_endpoint": "'$SOURCE_ENDPOINT'",
#                 "dest_endpoint": "'$DEST_ENDPOINT'",
#                 "source_path": "'$SOURCE_PATH'",
#                 "dest_path": "'$DEST_PATH'", 
#                 "transfer_label": "Audio_'$DATE_STR'"
#             }
#         }
#     }')

# # Extract Flow run ID
# FLOW_RUN_ID=$(echo "$FLOW_RESPONSE" | jq -r '.run_id')
# echo "Flow run started: $FLOW_RUN_ID"
# # Optionally monitor the Flow
# echo "Monitoring Flow execution..."
# curl -s "https://flows.globus.org/v1/runs/$FLOW_RUN_ID" \
#     -H "Authorization: Bearer $ACCESS_TOKEN" | jq '.status'

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
            singularity run \
                --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
                ${PIPELINE_UTILS_SIF} \
                /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" ready_to_process
            
            # Submit processing job
            sbatch "${SCRIPT_DIR}/process_audio_job.sh" "$DATE_STR"
            exit 0
        else
            echo "Transfer incomplete: only $FILES_TRANSFERRED/$EXPECTED_FILES files"
            singularity run \
                --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
                ${PIPELINE_UTILS_SIF} \
                /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Transfer failed: $ERROR_MSG"
            exit 1
        fi
        
    elif [ "$STATUS" = "FAILED" ]; then
        ERROR_MSG=$(echo "$TASK_INFO" | jq -r '.nice_status_details // .status')
        echo "Transfer failed: $ERROR_MSG"
        singularity run \
            --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
            ${PIPELINE_UTILS_SIF} \
            /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Transfer failed: $ERROR_MSG"
        exit 1
    fi
done

# Timeout
echo "Transfer timed out after ${MAX_CHECKS} checks"
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Transfer timeout"
exit 1