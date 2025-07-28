# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance audio processing pipeline for TikTok audio files, designed for distributed computing across HPC clusters and cloud VMs. The system processes audio files at scale using GPU-accelerated transcription and maintains comprehensive metadata in PostgreSQL.

## Architecture

The system consists of three main layers:
- **HPC Layer** (`/hpc/`): SLURM-scheduled jobs for audio processing using WhisperX on H100 GPUs
- **Database Layer** (`/db/`): PostgreSQL with full-text search, hosted on cloud VM
- **Monitoring Layer** (`/vm/`): Flask API for real-time pipeline status

### Core Processing Flow
1. Master scheduler discovers pending dates from processing queue
2. Globus transfers fetch timestamped tar archives from source
3. Audio processing converts MP3→Opus and runs batch GPU transcription
4. Results stored in PostgreSQL with metadata from Parquet files
5. Processed audio uploaded to database server via rsync

## Key Commands

### Container Setup
```bash
# Build Singularity containers (requires sudo)
cd containers && ./build_containers.sh

# Test containers
singularity run --nv audio_processing.sif --help
singularity run pipeline_utils.sif --help

# Test GPU access (on GPU node)
singularity exec --nv audio_processing.sif python -c "import torch; print(torch.cuda.is_available())"
```

### Database Management
```bash
# Setup PostgreSQL (run on cloud VM)
./db/setup_postgres.sh

# Create/update schema
./db/manage_schema.sh

# Test database connectivity (containerized)
singularity run --bind ./hpc/src:/opt/audio_pipeline/src containers/pipeline_utils.sif \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" test-connection
```

### HPC Operations (Containerized)
```bash
# Start master scheduler (typically via cron)
./hpc/0_master_scheduler.sh

# Manual Globus transfer for specific date
sbatch --export=DATE=2024-01-15 ./hpc/1_globus_transfer_job.sh

# Manual audio processing for specific date
sbatch --export=DATE=2024-01-15,DB_PASSWORD=your_password ./hpc/2_process_audio_job.sh

# Process single day (direct containerized execution)
singularity run --nv --bind ./src:/opt/audio_pipeline/src --bind /staging:/staging --bind /tmp:/temp \
    containers/audio_processing.sif /opt/audio_pipeline/src/hpc_process_day.py \
    --date 2024-01-15 --db-host VM_IP --db-password PASSWORD --staging-dir /staging --temp-dir /temp \
    --rsync-user audio_user --storage-root /opt/audio_storage
```

### Monitoring
```bash
# Start monitoring API (containerized)
singularity run --bind ./vm:/opt/audio_pipeline/vm --bind ./src:/opt/audio_pipeline/src \
    containers/pipeline_utils.sif /opt/audio_pipeline/vm/monitor_pipeline.py

# Check processing queue status
singularity run --bind ./src:/opt/audio_pipeline/src containers/pipeline_utils.sif \
    /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" get-pending --limit 10
```

### Storage Setup
```bash
# Set up SSH key authentication for rsync
./src/setup_ssh_keys.sh

# Test storage manager
python -c "from storage_manager import create_storage_manager; sm = create_storage_manager('VM_IP'); print('Storage ready')"
```

## Database Schema

The system uses PostgreSQL with 4 main tables:
- `audio_files`: Core audio metadata with 90+ TikTok fields
- `transcripts`: WhisperX transcription results with full-text search
- `audio_metadata`: Extended TikTok metadata (author, stats, location)
- `processing_queue`: Job coordination and status tracking

Database credentials are managed via environment variables or connection strings. Default connection pattern:
```
host=VM_IP port=5432 dbname=audio_pipeline user=audio_user password=audio_password
```

## Technology Stack

### Containerized Environment
The pipeline runs entirely in Singularity containers for consistent environments:

- **audio_processing.sif**: GPU-enabled container with WhisperX, PyTorch (CUDA 12.1), FFmpeg
- **pipeline_utils.sif**: Database tools, Globus CLI, monitoring utilities

### Core Dependencies (Containerized)
- **Python 3.10** with psycopg2, pandas, pyarrow
- **WhisperX** for GPU-accelerated transcription
- **PyTorch with CUDA 12.1** support for H100 GPUs
- **FFmpeg** for audio format conversion
- **Globus CLI** for high-performance data transfer
- **SLURM** for HPC job scheduling

### Processing Specifications
- Audio conversion: MP3 → Opus (32kbps, mono, 16kHz)
- Transcription: WhisperX large-v3 model with batch processing
- Batch sizes: 100+ audio files per processing job
- GPU optimization: Aggressive memory management and cache clearing
- Container isolation: Consistent runtime environment across all nodes

## File Organization

- `containers/audio_processing.def`: GPU container definition (WhisperX, PyTorch, rsync)
- `containers/pipeline_utils.def`: Utilities container definition (DB, Globus, rsync)
- `containers/build_containers.sh`: Container build script
- `hpc/0_master_scheduler.sh`: Main orchestration (cron-scheduled, containerized)
- `hpc/1_globus_transfer_job.sh`: Automated data transfer (containerized)
- `hpc/2_process_audio_job.sh`: Audio processing job submission (containerized)
- `src/hpc_process_day.py`: Core audio processing logic with rsync storage
- `src/storage_manager.py`: Rsync-based storage management module
- `src/setup_ssh_keys.sh`: SSH key setup helper for rsync authentication
- `src/db_utils.py`: Database utilities replacing raw SQL
- `db/setup_postgres.sh`: Automated PostgreSQL deployment
- `db/manage_schema.sh`: Schema creation and migration
- `vm/monitor_pipeline.py`: Flask monitoring API

## Environment Configuration

### Required Environment Variables
```bash
DB_HOST=your-cloud-vm-ip
DB_PASSWORD=your_secure_password
GLOBUS_ENDPOINT_ID=your_endpoint_id
DB_CREDS="host=VM_IP port=5432 dbname=audio_pipeline user=audio_user password=PASSWORD"
```

### Container Paths
```bash
CONTAINER_DIR="/shares/bdm.ipz.uzh/audio_pipeline/containers"
AUDIO_PROCESSING_SIF="${CONTAINER_DIR}/audio_processing.sif" 
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
```

### HPC Module Requirements (Minimal)
```bash
module load singularity  # For container runtime
# No longer need: mamba, cuda, python modules - all containerized
```

## Processing Patterns

### Timestamped Data Structure
Files are organized in 10-minute intervals:
- Archive format: `YYYY-MM-DD_HH-MM.tar.xz`
- Contains: audio files + metadata.parquet
- Processing: Extract → Convert → Transcribe → Store → Upload

### Error Handling
- Database operations use atomic transactions with rollback
- GPU processing includes memory cleanup and error recovery
- Failed jobs remain in processing queue for retry
- Comprehensive logging at INFO level throughout pipeline
- Container isolation prevents dependency conflicts

## Performance Considerations

- Use batch processing for GPU efficiency (batch_size=16 for WhisperX)
- Parallel audio conversion using ProcessPoolExecutor
- Strategic database indexing including GIN indexes for full-text search
- Compressed data transfer (tar.xz format)
- Aggressive cleanup of temporary files and GPU cache
- Container overhead minimal compared to processing time

## Container Usage Patterns

### GPU Container Usage
Always use `--nv` flag for GPU access:
```bash
singularity run --nv audio_processing.sif script.py
```

### Bind Mounts
Essential directories to bind mount:
- Source code: `--bind ./hpc/src:/opt/audio_pipeline/src`
- Data directories: `--bind /staging:/staging --bind /temp:/temp`
- Scratch space: `--bind /scratch:/scratch`

### Container Security
- Containers run as regular user (not root)
- No network access during processing
- Isolated filesystem prevents contamination