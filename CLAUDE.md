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
5. Processed audio uploaded to cloud storage

## Key Commands

### Database Management
```bash
# Setup PostgreSQL (run on cloud VM)
./db/setup_postgres.sh

# Create/update schema
./db/manage_schema.sh

# Test database connectivity
python hpc/src/db_utils.py --db-string "host=VM_IP port=5432 dbname=audio_pipeline user=audio_user password=audio_password" test-connection
```

### HPC Operations
```bash
# Start master scheduler (typically via cron)
./hpc/0_master_scheduler.sh

# Manual Globus transfer for specific date
sbatch --export=DATE=2024-01-15 ./hpc/1_globus_transfer_job.sh

# Manual audio processing for specific date
sbatch --export=DATE=2024-01-15,DB_PASSWORD=your_password ./hpc/2_process_audio_job.sh

# Process single day (direct Python execution)
cd hpc/src && python hpc_process_day.py --date 2024-01-15 --db-host VM_IP --db-password PASSWORD --staging-dir /path/to/staging --temp-dir /tmp/audio
```

### Monitoring
```bash
# Start monitoring API
python vm/monitor_pipeline.py

# Check processing queue status
python hpc/src/db_utils.py --db-string "$DB_CREDS" get-pending --limit 10
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

### Core Dependencies
- **Python 3.x** with psycopg2, pandas, pyarrow
- **WhisperX** for GPU-accelerated transcription
- **PyTorch** with CUDA support for H100 GPUs
- **FFmpeg** for audio format conversion
- **Globus** for high-performance data transfer
- **SLURM** for HPC job scheduling

### Processing Specifications
- Audio conversion: MP3 → Opus (32kbps, mono, 16kHz)
- Transcription: WhisperX large-v3 model with batch processing
- Batch sizes: 100+ audio files per processing job
- GPU optimization: Aggressive memory management and cache clearing

## File Organization

- `hpc/0_master_scheduler.sh`: Main orchestration (cron-scheduled)
- `hpc/1_globus_transfer_job.sh`: Automated data transfer
- `hpc/2_process_audio_job.sh`: Audio processing job submission
- `hpc/src/hpc_process_day.py`: Core processing logic
- `hpc/src/db_utils.py`: Database utilities replacing raw SQL
- `db/setup_postgres.sh`: Automated PostgreSQL deployment
- `db/manage_schema.sh`: Schema creation and migration
- `vm/monitor_pipeline.py`: Flask monitoring API

## Environment Configuration

### Required Environment Variables
```bash
DB_HOST=your-cloud-vm-ip
DB_PASSWORD=your_secure_password
GLOBUS_ENDPOINT_ID=your_endpoint_id
```

### HPC Module Requirements
```bash
module load mamba
mamba activate globus  # For Globus operations
mamba activate whisperx  # For audio processing (with PyTorch+CUDA)
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

## Performance Considerations

- Use batch processing for GPU efficiency (batch_size=16 for WhisperX)
- Parallel audio conversion using ProcessPoolExecutor
- Strategic database indexing including GIN indexes for full-text search
- Compressed data transfer (tar.xz format)
- Aggressive cleanup of temporary files and GPU cache