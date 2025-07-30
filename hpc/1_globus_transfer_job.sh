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
SOURCE_PATH="/scratch/olympus/tiktok_external_transfer/${FOLDER_NAME}/prepped_data/"
DEST_PATH="/shares/bdm.ipz.uzh/audio_pipeline/staging/${DATE_STR}/"

# Create destination directory
mkdir -p "$(dirname $DEST_PATH)"

# Update database status
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transferring

# Initiate transfer using Globus Flow
echo "Initiating transfer Flow for ${DATE_STR} from ${FOLDER_NAME}"

# Run the Globus Flow for file discovery and transfer
# Authentication will use GLOBUS_ACCESS_TOKEN or GLOBUS_CLIENT_ID/GLOBUS_CLIENT_SECRET env vars
FLOW_RESULT=$(singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/globus_flow_manager.py run \
    --date "$DATE_STR" \
    --source-endpoint "$SOURCE_ENDPOINT" \
    --dest-endpoint "$DEST_ENDPOINT" \
    --source-path "$SOURCE_PATH" \
    --dest-path "$DEST_PATH" \
    --label "Audio_${DATE_STR}" \
    --monitor)

# Check if Flow run was successful
if [ $? -eq 0 ]; then
    # Extract task ID from Flow result (Flow manager prints task_id in result)
    TASK_ID=$(echo "$FLOW_RESULT" | grep -o '"task_id": "[^"]*"' | cut -d'"' -f4)
    EXPECTED_FILES=$(echo "$FLOW_RESULT" | grep -o '"files_found": [0-9]*' | cut -d':' -f2 | tr -d ' ')
    
    if [ -z "$TASK_ID" ]; then
        echo "Error: Could not extract task ID from Flow result"
        echo "Flow result: $FLOW_RESULT"
        singularity run \
            --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
            ${PIPELINE_UTILS_SIF} \
            /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Flow completed but no task ID found"
        exit 1
    fi
    
    echo "Flow completed successfully. Transfer task ID: $TASK_ID, Files found: $EXPECTED_FILES"
else
    echo "Flow execution failed"
    echo "Flow output: $FLOW_RESULT"
    singularity run \
        --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
        ${PIPELINE_UTILS_SIF} \
        /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" update-transfer "$DATE_STR" transfer_failed --error "Globus Flow execution failed"
    exit 1
fi

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