# Audio Pipeline Singularity Containers

This directory contains Singularity container definitions and build scripts for the TikTok audio processing pipeline.

## Container Overview

### `audio_processing.sif`
**GPU-enabled container for audio processing**
- **Base**: nvidia/cuda:12.1-cudnn8-devel-ubuntu22.04
- **Purpose**: WhisperX transcription, PyTorch operations, audio conversion
- **Key Software**:
  - WhisperX with large-v3 model
  - PyTorch with CUDA 12.1 support
  - FFmpeg for audio conversion
  - Python 3.10 with scientific libraries

### `pipeline_utils.sif`
**Utilities container for database and orchestration**
- **Base**: ubuntu:22.04
- **Purpose**: Database operations, Globus transfers, monitoring
- **Key Software**:
  - PostgreSQL client tools
  - Globus CLI and SDK
  - Flask for monitoring API
  - Python 3.10 with database libraries

## Building Containers

### Prerequisites
- Singularity 3.7+ with sudo access
- Internet connection for downloading base images
- ~30GB free disk space for builds

### Build Process
```bash
# Build both containers interactively
./build_containers.sh

# Build individual containers
sudo singularity build audio_processing.sif audio_processing.def
sudo singularity build pipeline_utils.sif pipeline_utils.def
```

## Usage Examples

### Audio Processing Container
```bash
# Test GPU access (on GPU node)
singularity exec --nv audio_processing.sif python -c "import torch; print(torch.cuda.is_available())"

# Run transcription job
singularity run --nv \
    --bind ../hpc/src:/opt/audio_pipeline/src \
    --bind /staging:/staging \
    --bind /temp:/temp \
    audio_processing.sif \
    /opt/audio_pipeline/src/hpc_process_day.py --date 2024-01-15
```

### Pipeline Utils Container
```bash
# Database connectivity test
singularity run \
    --bind ../hpc/src:/opt/audio_pipeline/src \
    pipeline_utils.sif \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" test-connection

# Globus transfer
singularity exec pipeline_utils.sif globus transfer --help
```

## Container Specifications

### Resource Requirements
- **audio_processing.sif**: ~5GB image size, requires GPU node with NVIDIA drivers
- **pipeline_utils.sif**: ~2GB image size, runs on any node

### Security Model
- Containers run as regular user (not root)
- No privileged operations required during runtime
- Isolated filesystem prevents contamination
- Network access only when explicitly needed

### Performance
- Container startup overhead: ~1-2 seconds
- GPU passthrough performance: Near-native
- I/O performance: Dependent on bind mounts

## Troubleshooting

### Common Issues

**GPU not detected in container**
```bash
# Ensure NVIDIA runtime is available
nvidia-smi

# Use --nv flag for GPU access
singularity run --nv audio_processing.sif
```

**Permission denied errors**
```bash
# Ensure bind mount directories are accessible
ls -la /path/to/bind/mount

# Check container is not running as root
singularity exec container.sif whoami
```

**Module conflicts**
```bash
# Clear existing modules before running containers
module purge
module load singularity
```

### Performance Optimization

**For audio processing:**
- Use local scratch storage for temp directories
- Bind mount only necessary directories
- Ensure sufficient GPU memory for batch processing

**For database operations:**
- Use connection pooling for multiple operations
- Batch database updates when possible
- Monitor network latency to database server

## Maintenance

### Updating Containers
1. Modify the appropriate `.def` file
2. Rebuild the container: `sudo singularity build new_container.sif container.def`
3. Test the new container thoroughly
4. Replace the old container when validated

### Version Control
- Container definitions (`.def` files) are version controlled
- Built containers (`.sif` files) are not version controlled due to size
- Document major changes in this README

## Integration with SLURM

All SLURM job scripts have been updated to use these containers:
- `1_globus_transfer_job.sh` uses `pipeline_utils.sif`
- `2_process_audio_job.sh` uses `audio_processing.sif` with `--nv`
- `0_master_scheduler.sh` uses `pipeline_utils.sif`

See the main CLAUDE.md for complete containerized workflow documentation.