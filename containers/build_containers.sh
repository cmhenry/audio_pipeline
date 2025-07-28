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

# Offer choice between regular and venv-based approach
echo "   Choose build approach:"
echo "   1) Standard build (audio_processing.def)"
echo "   2) Skip audio processing container"
read -p "   Select option (1/2): " -n 1 -r BUILD_CHOICE
echo

AUDIO_DEF="audio_processing.def"
AUDIO_SIF="audio_processing.sif"

case $BUILD_CHOICE in
    1)
        AUDIO_DEF="audio_processing.def"
        AUDIO_SIF="audio_processing.sif"
        ;;
    2)
        echo "   Skipping audio processing container build"
        AUDIO_SIF=""
        ;;
    *)
        echo "   Invalid choice, defaulting to virtual environment build"
        AUDIO_DEF="audio_processing.def"
        AUDIO_SIF="audio_processing.sif"
        ;;
esac

if [ -n "$AUDIO_SIF" ]; then
    if [ -f "$AUDIO_SIF" ]; then
        echo "   Existing $AUDIO_SIF found. Remove it to rebuild."
        read -p "   Remove and rebuild? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -f "$AUDIO_SIF"
        else
            echo "   Skipping $AUDIO_SIF build."
        fi
    fi

    if [ ! -f "$AUDIO_SIF" ]; then
        echo "   Building $AUDIO_SIF from $AUDIO_DEF..."
        if sudo singularity build "$AUDIO_SIF" "$AUDIO_DEF"; then
            echo "   ✓ $AUDIO_SIF built successfully"
        else
            echo "   ✗ Failed to build $AUDIO_SIF"
            echo "   Check the error messages above for troubleshooting"
            exit 1
        fi
    else
        echo "   ✓ $AUDIO_SIF already exists"
    fi
fi

echo

# Build pipeline utilities container
echo "2. Building pipeline utilities container..."
echo "   This container includes database tools, Globus CLI, and monitoring"
echo "   Build time: ~10-15 minutes"
echo

# Offer choice between regular and venv-based approach
echo "   Choose build approach:"
echo "   1) Standard build (pipeline_utils.def)"
echo "   2) Skip pipeline utilities container"
read -p "   Select option (1/2): " -n 1 -r UTILS_BUILD_CHOICE
echo

UTILS_DEF="pipeline_utils.def"
UTILS_SIF="pipeline_utils.sif"

case $UTILS_BUILD_CHOICE in
    1)
        UTILS_DEF="pipeline_utils.def"
        UTILS_SIF="pipeline_utils.sif"
        ;;
    2)
        echo "   Skipping pipeline utilities container build"
        UTILS_SIF=""
        ;;
    *)
        echo "   Invalid choice, defaulting to virtual environment build"
        UTILS_DEF="pipeline_utils.def"
        UTILS_SIF="pipeline_utils.sif"
        ;;
esac

if [ -n "$UTILS_SIF" ]; then
    if [ -f "$UTILS_SIF" ]; then
        echo "   Existing $UTILS_SIF found. Remove it to rebuild."
        read -p "   Remove and rebuild? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -f "$UTILS_SIF"
        else
            echo "   Skipping $UTILS_SIF build."
        fi
    fi

    if [ ! -f "$UTILS_SIF" ]; then
        echo "   Building $UTILS_SIF from $UTILS_DEF..."
        if sudo singularity build "$UTILS_SIF" "$UTILS_DEF"; then
            echo "   ✓ $UTILS_SIF built successfully"
        else
            echo "   ✗ Failed to build $UTILS_SIF"
            echo "   Check the error messages above for troubleshooting"
            exit 1
        fi
    else
        echo "   ✓ $UTILS_SIF already exists"
    fi
fi

echo
echo "=== Container Build Summary ==="
echo "Built containers:"
ls -lh *.sif 2>/dev/null || echo "No .sif files found"

echo
echo "=== Next Steps ==="
echo "1. Test the containers:"
if [ -f "audio_processing.sif" ]; then
    echo "   singularity run --nv audio_processing.sif --help"
elif [ -f "audio_processing_venv.sif" ]; then
    echo "   singularity run --nv audio_processing.sif --help"
fi
if [ -f "pipeline_utils.sif" ]; then
    echo "   singularity run pipeline_utils.sif --help"
elif [ -f "pipeline_utils_venv.sif" ]; then
    echo "   singularity run pipeline_utils.sif --help"
fi
echo
echo "2. Test GPU access (on GPU node):"
if [ -f "audio_processing.sif" ]; then
    echo "   singularity exec --nv audio_processing.sif python -c \"import torch; print(torch.cuda.is_available())\""
elif [ -f "audio_processing_venv.sif" ]; then
    echo "   singularity exec --nv audio_processing.sif python -c \"import torch; print(torch.cuda.is_available())\""
fi
echo
echo "3. Update your job scripts to use these containers"
echo "   Note: If using venv containers, update container paths in job scripts accordingly"

echo
echo "=== Container Usage Notes ==="
echo "• Always use --nv flag with audio_processing.sif for GPU access"
echo "• Bind mount directories you need access to with --bind"
echo "• Source code should be bind mounted to /opt/audio_pipeline/src"
echo "• Data directories should be bind mounted as needed"

echo
echo "Build complete!"