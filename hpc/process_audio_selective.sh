#!/bin/bash
#SBATCH --job-name=process_selective_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/cohenr/logs/process_selective_%j.out
#SBATCH --error=/scratch/cohenr/error/process_selective_%j.err

# Import .env global configuration
set -a; source config.env; set +a

# process_audio_selective.sh - Selective audio pipeline processing
# Allows user to select which stages of the pipeline to run

set -euo pipefail
module load singularityce

# Default values
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/audio_processing.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/src"
DEFAULT_STAGING="/shares/bdm.ipz.uzh/audio_storage/staging/${DATE}"
DEFAULT_TEMP="/shares/bdm.ipz.uzh/audio_storage/staging/${SLURM_JOB_ID}"
DEFAULT_BATCH_SIZE=100
DEFAULT_WORKERS=32

# Storage configuration for rsync
RSYNC_USER="ubuntu"
STORAGE_ROOT="/mnt/storage/audio_storage"
SECRETS_DIR="/data/cohenr/audio_pipeline_secrets"

# Database connection
DB_HOST="172.23.76.3"
DB_PASSWORD="audio_password"
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"
export DB_CREDS

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Selective audio processing pipeline for TikTok audio files.

REQUIRED OPTIONS:
    --date DATE           Date to process (YYYY-MM-DD)
    --db-host HOST        Database host (also rsync target)
    --db-password PASS    Database password

OPTIONAL:
    --staging-dir DIR     Staging directory (default: $DEFAULT_STAGING)
    --temp-dir DIR        Temporary directory (default: $DEFAULT_TEMP)
    --batch-size N        Audio files per batch (default: $DEFAULT_BATCH_SIZE)
    --num-workers N       Parallel workers (default: $DEFAULT_WORKERS)
    --rsync-user USER     Username for rsync (default: audio_user)
    --storage-root DIR    Storage root on target (default: /opt/audio_storage)
    --ssh-keyfile FILE    SSH key file (default: ent.pem)
    --container-sif FILE  Container path (default: $AUDIO_PROCESSING_SIF)

PROCESSING STAGES:
    --extract             Extract audio files from tar archives
    --convert             Convert MP3 files to Opus format
    --transcribe          Transcribe audio using WhisperX (requires GPU)
    --upload              Store results in DB and upload to storage
    --metadata            Process metadata and comments files
    --all                 Run all stages (default if no stages specified)

INTERACTIVE MODE:
    --interactive         Interactive stage selection menu

EXAMPLES:
    # Run only extraction and conversion
    $0 --date 2025-01-31 --db-host 1.2.3.4 --db-password mypass --extract --convert

    # Run full pipeline
    $0 --date 2025-01-31 --db-host 1.2.3.4 --db-password mypass --all

    # Interactive mode
    $0 --date 2025-01-31 --db-host 1.2.3.4 --db-password mypass --interactive

    # Resume from transcription stage
    $0 --date 2025-01-31 --db-host 1.2.3.4 --db-password mypass --transcribe --upload
EOF
}

# Interactive stage selection
interactive_mode() {
    echo -e "${BLUE}=== Interactive Pipeline Stage Selection ===${NC}"
    echo "Select stages to run:"
    echo
    echo "1) Extract audio files from tar archives"
    echo "2) Convert MP3 files to Opus format" 
    echo "3) Transcribe audio using WhisperX (GPU required)"
    echo "4) Store results in database and upload to storage"
    echo "5) Process metadata and comments"
    echo "6) All stages (1-5)"
    echo "7) Typical resume scenarios:"
    echo "   a) Extract + Convert only"
    echo "   b) Convert + Transcribe + Upload (skip extraction)"
    echo "   c) Transcribe + Upload only (resume from transcription)"
    echo
    read -p "Enter your selection (1-7, or multiple numbers separated by spaces, or letter): " selection
    
    STAGES=()
    case "$selection" in
        *1*) STAGES+=("extract") ;;
    esac
    case "$selection" in
        *2*) STAGES+=("convert") ;;
    esac  
    case "$selection" in
        *3*) STAGES+=("transcribe") ;;
    esac
    case "$selection" in
        *4*) STAGES+=("upload") ;;
    esac
    case "$selection" in
        *5*) STAGES+=("metadata") ;;
    esac
    case "$selection" in
        *6*) STAGES=("extract" "convert" "transcribe" "upload" "metadata") ;;
        *a*) STAGES=("extract" "convert") ;;
        *b*) STAGES=("convert" "transcribe" "upload") ;;
        *c*) STAGES=("transcribe" "upload") ;;
    esac
    
    if [ ${#STAGES[@]} -eq 0 ]; then
        echo -e "${RED}No valid stages selected. Exiting.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}Selected stages: ${STAGES[*]}${NC}"
    return 0
}

# Validate required parameters
validate_params() {
    local errors=0
    
    if [ -z "${DATE:-}" ]; then
        echo -e "${RED}Error: --date is required${NC}"
        errors=1
    fi
    
    if [ -z "${DB_HOST:-}" ]; then
        echo -e "${RED}Error: --db-host is required${NC}"
        errors=1
    fi
    
    if [ -z "${DB_PASSWORD:-}" ]; then
        echo -e "${RED}Error: --db-password is required${NC}"
        errors=1
    fi
    
    if [ ! -f "${CONTAINER_SIF}" ]; then
        echo -e "${RED}Error: Container not found at ${CONTAINER_SIF}${NC}"
        errors=1
    fi
    
    if [ $errors -gt 0 ]; then
        echo -e "${RED}Please fix the above errors and try again.${NC}"
        exit 1
    fi
}

# Run the processing pipeline
run_pipeline() {
    echo -e "${BLUE}=== Audio Processing Pipeline ===${NC}"
    echo "Date: $DATE"
    echo "Database Host: $DB_HOST"
    echo "Staging Directory: $STAGING_DIR"
    echo "Temporary Directory: $TEMP_DIR"
    echo "Stages: ${STAGES[*]}"
    echo "Container: $CONTAINER_SIF"
    echo
    
    # Build stage arguments
    local stage_args=()
    if [ ${#STAGES[@]} -gt 0 ]; then
        stage_args+=("--stages")
        stage_args+=("${STAGES[@]}")
    fi
    
    # Build container command
    local cmd=(
        "singularity" "run" "--nv"
        "--bind" "${STAGING_DIR}:/staging"
        "--bind" "${TEMP_DIR}:/temp"
        "--bind" "${SCRIPT_DIR}:/opt/audio_pipeline/src"
        "--bind" "${SECRETS_DIR}:/secrets"
        "${CONTAINER_SIF}"
        "/opt/audio_pipeline/src/hpc_process_day.py"
        "--date" "${DATE}"
        "--staging-dir" "/staging"
        "--temp-dir" "/temp"
        "--db-host" "${DB_HOST}"
        "--db-password" "${DB_PASSWORD}"
        "--batch-size" "${BATCH_SIZE}"
        "--num-workers" "${NUM_WORKERS}"
        "--rsync-user" "${RSYNC_USER}"
        "--storage-root" "${STORAGE_ROOT}"
        "--ssh_keyfile" "${SSH_KEYFILE}"
    )
    
    # Add stages if specified
    if [ ${#stage_args[@]} -gt 0 ]; then
        cmd+=("${stage_args[@]}")
    fi
    
    echo -e "${YELLOW}Running command:${NC}"
    printf '%q ' "${cmd[@]}"
    echo
    echo
    
    # Execute the command
    exec "${cmd[@]}"
}

# Parse command line arguments
STAGES=()
INTERACTIVE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            DATE="$2"
            shift 2
            ;;
        --db-host)
            DB_HOST="$2" 
            shift 2
            ;;
        --db-password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        --staging-dir)
            STAGING_DIR="$2"
            shift 2
            ;;
        --temp-dir)
            TEMP_DIR="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --num-workers)
            NUM_WORKERS="$2"
            shift 2
            ;;
        --rsync-user)
            RSYNC_USER="$2"
            shift 2
            ;;
        --storage-root)
            STORAGE_ROOT="$2"
            shift 2
            ;;
        --ssh-keyfile)
            SSH_KEYFILE="$2"
            shift 2
            ;;
        --container-sif)
            CONTAINER_SIF="$2"
            shift 2
            ;;
        --extract)
            STAGES+=("extract")
            shift
            ;;
        --convert)
            STAGES+=("convert")
            shift
            ;;
        --transcribe)
            STAGES+=("transcribe")
            shift
            ;;
        --upload)
            STAGES+=("upload")
            shift
            ;;
        --metadata)
            STAGES+=("metadata")
            shift
            ;;
        --all)
            STAGES=("extract" "convert" "transcribe" "upload" "metadata")
            shift
            ;;
        --interactive)
            INTERACTIVE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            exit 1
            ;;
    esac
done

# Set defaults
STAGING_DIR="${STAGING_DIR:-$DEFAULT_STAGING}"
TEMP_DIR="${TEMP_DIR:-$DEFAULT_TEMP}"
BATCH_SIZE="${BATCH_SIZE:-$DEFAULT_BATCH_SIZE}"
NUM_WORKERS="${NUM_WORKERS:-$DEFAULT_WORKERS}"
RSYNC_USER="${RSYNC_USER:-audio_user}"
STORAGE_ROOT="${STORAGE_ROOT:-/opt/audio_storage}"
SSH_KEYFILE="${SSH_KEYFILE:-ent.pem}"
CONTAINER_SIF="${CONTAINER_SIF:-$AUDIO_PROCESSING_SIF}"

# Interactive mode
if [ "$INTERACTIVE" = true ]; then
    interactive_mode
fi

# Default to all stages if none specified and not interactive
if [ ${#STAGES[@]} -eq 0 ] && [ "$INTERACTIVE" = false ]; then
    STAGES=("extract" "convert" "transcribe" "upload" "metadata")
    echo -e "${YELLOW}No stages specified, running all stages${NC}"
fi

# Validate parameters
validate_params

# Run the pipeline
run_pipeline