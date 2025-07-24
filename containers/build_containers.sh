#!/bin/bash
# build_containers.sh - Build Singularity containers for audio pipeline

set -e

CONTAINER_DIR="$(dirname "$0")"
cd "$CONTAINER_DIR"

echo "=== Building Audio Pipeline Singularity Containers ==="
echo "Container directory: $(pwd)"
echo

# Check if Singularity is available
if ! command -v singularity &> /dev/null; then
    echo "ERROR: Singularity is not available. Please load the singularity module:"
    echo "  module load singularity"
    exit 1
fi

# Build audio processing container (GPU-enabled)
echo "1. Building audio processing container (with GPU support)..."
echo "   This container includes WhisperX, PyTorch with CUDA, and FFmpeg"
echo "   Build time: ~20-30 minutes"
echo

if [ -f "audio_processing.sif" ]; then
    echo "   Existing audio_processing.sif found. Remove it to rebuild."
    read -p "   Remove and rebuild? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f audio_processing.sif
    else
        echo "   Skipping audio_processing.sif build."
    fi
fi

if [ ! -f "audio_processing.sif" ]; then
    echo "   Building audio_processing.sif..."
    sudo singularity build audio_processing.sif audio_processing.def
    echo "   ✓ audio_processing.sif built successfully"
else
    echo "   ✓ audio_processing.sif already exists"
fi

echo

# Build pipeline utilities container
echo "2. Building pipeline utilities container..."
echo "   This container includes database tools, Globus CLI, and monitoring"
echo "   Build time: ~10-15 minutes"
echo

if [ -f "pipeline_utils.sif" ]; then
    echo "   Existing pipeline_utils.sif found. Remove it to rebuild."
    read -p "   Remove and rebuild? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f pipeline_utils.sif
    else
        echo "   Skipping pipeline_utils.sif build."
    fi
fi

if [ ! -f "pipeline_utils.sif" ]; then
    echo "   Building pipeline_utils.sif..."
    sudo singularity build pipeline_utils.sif pipeline_utils.def
    echo "   ✓ pipeline_utils.sif built successfully"
else
    echo "   ✓ pipeline_utils.sif already exists"
fi

echo
echo "=== Container Build Summary ==="
echo "Built containers:"
ls -lh *.sif 2>/dev/null || echo "No .sif files found"

echo
echo "=== Next Steps ==="
echo "1. Test the containers:"
echo "   singularity run --nv audio_processing.sif --help"
echo "   singularity run pipeline_utils.sif --help"
echo
echo "2. Test GPU access (on GPU node):"
echo "   singularity exec --nv audio_processing.sif python -c \"import torch; print(torch.cuda.is_available())\""
echo
echo "3. Update your job scripts to use these containers"
echo "4. Update CLAUDE.md with containerized workflow"

echo
echo "=== Container Usage Notes ==="
echo "• Always use --nv flag with audio_processing.sif for GPU access"
echo "• Bind mount directories you need access to with --bind"
echo "• Source code should be bind mounted to /opt/audio_pipeline/src"
echo "• Data directories should be bind mounted as needed"

echo
echo "Build complete!"