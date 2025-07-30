#!/bin/bash
# scheduler.sh - Runs on HPC login node via cron
# Schedules daily Globus transfers and monitors completion
# Updated for Singularity containers

# Container paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"

SCRIPT_DIR="/data/cohenr/audio_pipeline/src"
PIPELINE_DIR="/data/cohenr/audio_pipeline/hpc"
WORK_DIR="/shares/bdm.ipz.uzh/audio_pipeline"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Check what days need processing using psql
# DAYS_TO_PROCESS=$(psql "$DB_CREDS" -t -c "
#     SELECT DISTINCT year || '-' || 
#            LPAD(month::text, 2, '0') || '-' || 
#            LPAD(date::text, 2, '0')
#     FROM processing_queue
#     WHERE status = 'pending'
#     ORDER BY year, month, date
#     LIMIT 5;
# ")

# Get pending dates using containerized Python
DAYS_TO_PROCESS=$(singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" get-pending --limit 5)

# for DAY in $DAYS_TO_PROCESS; do
#     # Check if transfer job already exists
#     JOB_EXISTS=$(squeue -u $USER -n "transfer_${DAY}" -h | wc -l)
    
#     if [ "$JOB_EXISTS" -eq 0 ]; then
#         # Submit Globus transfer job
#         sbatch "${SCRIPT_DIR}/globus_transfer_job.sh" "$DAY"
#     fi
# done

for DAY in $DAYS_TO_PROCESS; do
    # Check if transfer job already exists
    if ! python ${SCRIPT_DIR}/db_utils.py --db-string "$DB_CREDS" check-job "$DAY"; then
        # Submit Globus transfer job
        sbatch "${PIPELINE_DIR}/globus_transfer_job.sh" "$DAY"
    fi
done