#!/bin/bash
#SBATCH --job-name=classify_subtitles_%j
#SBATCH --mail-type=ALL
#SBATCH --mail-user=cmhenry@protonmail.com
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gpus=H100:1
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/cohenr/logs/classification/classify_subtitles_%j.out
#SBATCH --error=/scratch/cohenr/error/classification/classify_subtitles_%j.err

module load apptainer

# classify_subtitles_job.sh - Subtitle classification using experimental classifier
# Usage: sbatch --export=INPUT_DIR=/path/to/subtitles,OUTPUT_FILE=/path/to/results.parquet classify_subtitles_job.sh

# Container paths
CONTAINER_DIR="/home/cohenr/data/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/audio_processing.sif"
SCRIPT_DIR="/home/cohenr/data/audio_pipeline/src"
MODELS_DIR="/home/cohenr/data/models"

# Check required environment variables
if [[ -z "$INPUT_DIR" ]]; then
    echo "Error: INPUT_DIR environment variable not set"
    echo "Usage: sbatch --export=INPUT_DIR=/path/to/subtitles,OUTPUT_FILE=/path/to/results.parquet classify_subtitles_job.sh"
    exit 1
fi

if [[ -z "$OUTPUT_FILE" ]]; then
    echo "Warning: OUTPUT_FILE not specified, using default filename"
    OUTPUT_FILE="classification_results_${SLURM_JOB_ID}.parquet"
fi

# Display job information
echo "========================================="
echo "Subtitle Classification Job"
echo "========================================="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node: $(hostname)"
echo "GPU: $CUDA_VISIBLE_DEVICES"
echo "Input Directory: $INPUT_DIR"
echo "Output File: $OUTPUT_FILE"
echo "Container: $AUDIO_PROCESSING_SIF"
echo "========================================="

# Verify input directory exists
if [[ ! -d "$INPUT_DIR" ]]; then
    echo "Error: Input directory does not exist: $INPUT_DIR"
    exit 1
fi

# Create output directory if it doesn't exist
OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
mkdir -p "$OUTPUT_DIR"

# Set up bind paths for container
INPUT_DIR_BIND=$(realpath "$INPUT_DIR")
OUTPUT_DIR_BIND=$(realpath "$OUTPUT_DIR")

echo "Starting subtitle classification..."
echo "Looking for subtitle files in: $INPUT_DIR_BIND"

# Run the classification with GPU support
apptainer run --nv \
    --bind ${MODELS_DIR}:/models \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${INPUT_DIR_BIND}:/input_data \
    --bind ${OUTPUT_DIR_BIND}:/output_data \
    ${AUDIO_PROCESSING_SIF} \
    /opt/audio_pipeline/src/experimental_classifier.py \
    --input_dir "/input_data" \
    --output "/output_data/$(basename "$OUTPUT_FILE")"

# Check exit status
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "========================================="
    echo "Classification completed successfully!"
    echo "Results saved to: $OUTPUT_FILE"
    
    # Display basic stats about the results
    if [[ -f "$OUTPUT_FILE" ]]; then
        echo "Output file size: $(du -h "$OUTPUT_FILE" | cut -f1)"
        echo "Output file location: $OUTPUT_FILE"
    fi
    echo "========================================="
else
    echo "========================================="
    echo "Classification failed with exit code: $EXIT_CODE"
    echo "Check the error log for details: /scratch/cohenr/logs/classify_${SLURM_JOB_ID}.err"
    echo "========================================="
    exit $EXIT_CODE
fi