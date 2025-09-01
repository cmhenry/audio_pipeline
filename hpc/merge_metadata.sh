#!/bin/bash
#SBATCH --job-name=merge_metadata_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=18:00:00
#SBATCH --output=/scratch/cohenr/logs/metadata/merge_%j.out
#SBATCH --error=/scratch/cohenr/error/metadata/merge_%j.err

module load singularityce

INPUT_FOLDER=$1
CLASS_FILE=$2
OUTPUT=$3

CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/audio_processing.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/src"

# Create necessary directories
# mkdir -p /scratch/cohenr/logs/metadata
# mkdir -p /scratch/cohenr/error/metadata

# Validate input arguments
if [ $# -lt 3 ]; then
    echo "Usage: sbatch $0 <input_folder> <classified_file> <output_file>"
    echo "  input_folder: Path to folder containing *_metadata.parquet files"
    echo "  classified_file: Path to classified parquet file"
    echo "  output_file: Path for merged output parquet file"
    exit 1
fi

# Set up container paths - use pipeline_utils.sif for data processing
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"

# Get directories for bind mounting
INPUT_DIR=$(dirname "$INPUT_FOLDER")
CLASS_DIR=$(dirname "$CLASS_FILE") 
OUTPUT_DIR=$(dirname "$OUTPUT")

echo "Starting metadata merge job at $(date)"
echo "Input folder: $INPUT_FOLDER"
echo "Classified file: $CLASS_FILE"
echo "Output file: $OUTPUT"

# Run the merge script with appropriate bind mounts
singularity run \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${INPUT_DIR}:/input_data \
    --bind ${CLASS_DIR}:/classified_data \
    --bind ${OUTPUT_DIR}:/output_data \
    ${PIPELINE_UTILS_SIF} \
    /opt/audio_pipeline/src/merge_metadata.py \
    --folder "/input_data/$(basename "$INPUT_FOLDER")" \
    --classified-file "/classified_data/$(basename "$CLASS_FILE")" \
    --output "/output_data/$(basename "$OUTPUT")" \
    --verbose

echo "Metadata merge job completed at $(date)"
