#!/bin/bash
# build_streaming_containers.sh - Build streaming audio processing containers

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Streaming Audio Pipeline Container Builder ===${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root (for singularity build)${NC}"
    echo "Please run: sudo $0"
    exit 1
fi

# Check if singularity is available
if ! command -v singularity &> /dev/null; then
    echo -e "${RED}Error: Singularity not found${NC}"
    echo "Please install singularity first"
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINERS_DIR="$SCRIPT_DIR"

echo "Building containers in: $CONTAINERS_DIR"

# Build streaming audio processing container
echo -e "${BLUE}Building streaming audio processing container...${NC}"
echo "This may take 15-20 minutes due to GPU libraries and streaming optimizations..."

cd "$CONTAINERS_DIR"

if singularity build streaming_audio_processing.sif streaming_audio_processing.def; then
    echo -e "${GREEN}✓ Streaming audio processing container built successfully${NC}"
else
    echo -e "${RED}✗ Failed to build streaming audio processing container${NC}"
    exit 1
fi

# Show container info
echo
echo -e "${BLUE}Container Information:${NC}"
ls -lh streaming_audio_processing.sif

echo
echo -e "${BLUE}Testing streaming container...${NC}"
if singularity run streaming_audio_processing.sif python3 -c "
import torch
import whisperx
import psycopg2
import pandas
import concurrent.futures
import asyncio

print('✓ All streaming dependencies imported successfully')
print(f'PyTorch version: {torch.__version__}')
print(f'CUDA available: {torch.cuda.is_available()}')
print('Container is ready for streaming processing')
"; then
    echo -e "${GREEN}✓ Streaming container test passed${NC}"
else
    echo -e "${RED}✗ Streaming container test failed${NC}"
    exit 1
fi

echo
echo -e "${GREEN}=== Streaming Container Build Complete ===${NC}"
echo 
echo "Built containers:"
echo "  streaming_audio_processing.sif - GPU-enabled streaming processing with async uploads"
echo
echo "To use the streaming pipeline:"
echo "  1. Deploy containers to HPC: rsync streaming_audio_processing.sif hpc:/data/cohenr/audio_pipeline/containers/"
echo "  2. Test single tar: sbatch streaming_pipeline/hpc/process_single_tar_streaming.sh 2025-01-25 /path/to/file.tar.xz 12_30"
echo "  3. Run full day: sbatch streaming_pipeline/hpc/process_day_streaming.sh 2025-01-25"
echo
echo "Key streaming features:"
echo "  - Stream extraction (no full tar extraction)"
echo "  - Async background uploads"
echo "  - Reduced memory usage"
echo "  - Timeout prevention"
echo "  - Faster processing per file"