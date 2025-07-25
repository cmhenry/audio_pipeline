# Container Build Troubleshooting Guide

## Common Build Issues and Solutions

### 1. Blinker Package Conflict

**Error:**
```
× Cannot uninstall blinker 1.4
╰─> It is a distutils installed project and thus we cannot accurately determine which files belong to it
```

**Solutions:**

**Option A: Use Virtual Environment Build (Recommended)**
```bash
# Use the venv-based container definitions
./build_containers.sh
# Select option 2 for both audio_processing and pipeline_utils when prompted
```

**Option B: Manual Fix for Standard Build**
If you prefer the standard build, modify both `audio_processing.def` and `pipeline_utils.def`:
```dockerfile
# Add to %post section before pip installs:
python -m pip uninstall -y blinker || true
apt-get remove -y python3-blinker || true
apt-get autoremove -y

# Then install Flask dependencies carefully:
pip install blinker>=1.6.0 flask
```

### 2. CUDA Compatibility Issues

**Error:**
```
CUDA driver version is insufficient for CUDA runtime version
```

**Solution:**
Check your CUDA driver version and update the container base image:
```bash
# Check driver version
nvidia-smi

# Update base image in .def file if needed
From: nvidia/cuda:11.8-cudnn8-devel-ubuntu22.04  # For older drivers
From: nvidia/cuda:12.1-cudnn8-devel-ubuntu22.04  # For newer drivers
```

### 3. Network Issues During Build

**Error:**
```
Failed to download packages from PyPI
```

**Solutions:**
```bash
# Check internet connectivity
ping pypi.org

# Use alternative PyPI index if needed
pip install --index-url https://pypi.python.org/simple/ package_name

# For corporate networks, set proxy in %post:
export http_proxy=http://proxy.company.com:8080
export https_proxy=http://proxy.company.com:8080
```

### 4. Insufficient Disk Space

**Error:**
```
No space left on device
```

**Solutions:**
```bash
# Check available space
df -h /tmp
df -h $SINGULARITY_TMPDIR

# Set custom temp directory
export SINGULARITY_TMPDIR=/path/to/large/tmp
sudo singularity build container.sif container.def

# Clean up build cache
sudo singularity cache clean --force
```

### 5. Permission Issues

**Error:**
```
Permission denied when building container
```

**Solutions:**
```bash
# Ensure you have sudo access
sudo -v

# Check if Singularity is installed correctly
which singularity
singularity version

# For rootless builds (if supported):
singularity build --fakeroot container.sif container.def
```

### 6. WhisperX Installation Issues

**Error:**
```
Failed building wheel for whisperx
```

**Solutions:**
```bash
# Ensure build dependencies are installed in container %post:
apt-get install -y build-essential python3-dev
pip install --upgrade setuptools wheel

# Install WhisperX dependencies separately:
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121
pip install transformers
pip install whisperx
```

### 7. Memory Issues During Build

**Error:**
```
Killed (OOM - Out of Memory)
```

**Solutions:**
```bash
# Build on a node with more memory
srun --mem=32G --pty bash
cd containers && ./build_containers.sh

# Reduce parallel builds:
pip install --no-cache-dir package_name

# Clear pip cache in %post:
pip cache purge
```

## Build Environment Requirements

### System Requirements
- **RAM**: Minimum 16GB, recommended 32GB for builds
- **Disk**: 30GB free space for build process
- **Network**: Stable internet connection for downloads
- **Privileges**: sudo access for building containers

### Module Requirements
```bash
module load singularity
# Do NOT load other modules (cuda, python, etc.) - containers are self-contained
```

## Debugging Build Process

### Enable Verbose Output
```bash
sudo singularity build --debug container.sif container.def
```

### Test Container Components
```bash
# Test base image
singularity run docker://nvidia/cuda:12.1-cudnn8-devel-ubuntu22.04 python3 --version

# Test specific sections
# Create a minimal .def file with just the problematic section
```

### Interactive Debugging
```bash
# Start interactive session with base image
singularity shell docker://nvidia/cuda:12.1-cudnn8-devel-ubuntu22.04

# Manually run commands from %post section
apt-get update
apt-get install -y python3-pip
# ... etc
```

## Performance Optimization

### Reduce Build Time
1. **Layer caching**: Order package installs from most stable to least stable
2. **Minimize downloads**: Use local package mirrors if available
3. **Parallel builds**: Avoid if memory is limited

### Reduce Container Size
1. **Clean package caches**:
   ```dockerfile
   apt-get clean && rm -rf /var/lib/apt/lists/*
   pip cache purge
   ```
2. **Remove build dependencies** after compilation
3. **Use multi-stage builds** if needed

## Recovery Procedures

### Failed Build Recovery
```bash
# Remove partial container
rm -f failed_container.sif

# Clear Singularity cache
sudo singularity cache clean --force

# Check system resources
df -h
free -h

# Retry with clean environment
sudo env -i singularity build container.sif container.def
```

### Container Validation
```bash
# Test basic functionality
singularity exec container.sif python3 --version

# Test GPU access (on GPU node)
singularity exec --nv container.sif nvidia-smi

# Test Python imports
singularity exec container.sif python3 -c "import torch; print(torch.cuda.is_available())"
```

## Getting Help

### Log Collection
When reporting issues, collect:
```bash
# Build logs
sudo singularity build --debug container.sif container.def 2>&1 | tee build.log

# System information
uname -a
singularity version
nvidia-smi
df -h
free -h
```

### Useful Commands
```bash
# Check container contents
singularity inspect container.sif

# List container environment
singularity exec container.sif env

# Check installed packages
singularity exec container.sif pip list
singularity exec container.sif apt list --installed
```