# Streaming Audio Pipeline

A separate, optimized pipeline for processing tar.gz files with streaming extraction and asynchronous storage uploads. This pipeline addresses timeout issues and memory constraints by processing files in small batches with background uploads.

## Key Improvements

### 1. Stream Processing
- **No Full Extraction**: Extract files from tar archive in small batches (100-200 files)
- **Process-and-Delete**: Convert and transcribe each batch, then immediately delete extracted files
- **Memory Efficient**: Never hold entire tar contents in memory simultaneously

### 2. Asynchronous Storage
- **Background Uploads**: rsync uploads happen in ThreadPoolExecutor background threads
- **Non-blocking**: Continue processing while uploads happen asynchronously  
- **Queue Management**: Track upload status and wait for completion at the end
- **Timeout Prevention**: Shorter individual operations prevent timeout issues

### 3. Resource Optimization
- **Smaller Batches**: Process 50-100 files per batch (vs 500 in original)
- **Higher Concurrency**: 6-8 concurrent uploads per job
- **Faster Jobs**: 1-2 hour jobs instead of 18-hour jobs
- **Better GPU Utilization**: More granular processing prevents memory issues

## Architecture

```
streaming_pipeline/
├── src/
│   ├── streaming_tar_processor.py     # Stream processing with async uploads
│   └── streaming_storage_manager.py   # Async rsync storage manager
├── hpc/
│   ├── process_single_tar_streaming.sh # Worker job for single tar
│   └── process_day_streaming.sh       # Master controller for full day
├── containers/
│   ├── streaming_audio_processing.def # Container definition with optimizations
│   └── build_streaming_containers.sh  # Container build script
└── test_streaming_setup.sh            # Setup validation

```

## Processing Flow

### Original Pipeline
```
Extract All → Convert All → Transcribe All → Upload All (Synchronous)
   ~30min      ~45min         ~8hours        ~4hours
   (Timeout risk)             (Memory risk)   (Timeout risk)
```

### Streaming Pipeline  
```
Stream Extract → Convert Batch → Transcribe Batch → Queue Upload → Repeat
    ~2min           ~3min          ~20min           ~instant    
    (No timeout)    (Low memory)   (Controlled)     (Async)
```

## Configuration

### Stream Processing Settings
```bash
--stream-batch-size 200        # Files extracted from tar at once
--process-batch-size 100       # Files processed in parallel 
--num-workers 10               # Parallel conversion workers
```

### Async Upload Settings
```bash
--max-concurrent-uploads 8     # Background upload threads
--upload-timeout 120           # Individual upload timeout (seconds)
```

### Resource Allocation
```bash
#SBATCH --cpus-per-task=12     # More CPU cores for streaming
#SBATCH --mem=96G              # More memory for batch processing  
#SBATCH --time=02:00:00        # Shorter job time limit
```

## Usage

### 1. Build Container
```bash
cd streaming_pipeline/containers
sudo ./build_streaming_containers.sh
```

### 2. Deploy to HPC
```bash
rsync -av streaming_pipeline/ hpc:/data/cohenr/audio_pipeline/
```

### 3. Process Single File
```bash
sbatch streaming_pipeline/hpc/process_single_tar_streaming.sh \
    2025-01-25 /path/to/file.tar.xz 12_30
```

### 4. Process Full Day
```bash
sbatch streaming_pipeline/hpc/process_day_streaming.sh 2025-01-25
```

### 5. Monitor Progress
```bash
# Check job status
squeue -u $USER

# View streaming job output
tail -f /scratch/cohenr/logs/stream_tar_*.out

# Check upload progress in job logs
grep -i "upload\|async" /scratch/cohenr/logs/stream_tar_*.out
```

## Performance Comparison

| Metric | Original Pipeline | Streaming Pipeline |
|--------|-------------------|-------------------|
| Memory Usage | ~50GB (full extraction) | ~10GB (stream batches) |
| Job Duration | 18 hours | 1-2 hours |
| Timeout Risk | High | Low |
| GPU Utilization | Variable | Consistent |
| Upload Method | Synchronous | Asynchronous |
| Failure Recovery | Lose entire day | Lose single tar file |

## Key Components

### StreamingTarProcessor
- Streams files from tar archive in configurable batches
- Processes audio conversion and transcription in sub-batches
- Queues uploads asynchronously and continues processing
- Cleans up extracted files immediately to save space

### AsyncRsyncStorageManager
- ThreadPoolExecutor for concurrent uploads
- Upload queuing with status tracking
- Automatic retry with exponential backoff
- Final completion waiting with timeout

### Container Optimizations
- Streaming-specific Python packages
- Optimized CUDA memory settings
- Concurrent processing libraries
- Reduced container size for faster deployment

## Troubleshooting

### Common Issues

1. **Container Build Fails**
   ```bash
   # Ensure you're running as root
   sudo ./build_streaming_containers.sh
   ```

2. **Upload Timeouts**
   ```bash
   # Reduce concurrent uploads or increase timeout
   --max-concurrent-uploads 4 --upload-timeout 180
   ```

3. **Memory Issues**
   ```bash
   # Reduce batch sizes
   --stream-batch-size 100 --process-batch-size 50
   ```

4. **GPU Memory Errors**
   ```bash
   # Enable more aggressive memory clearing
   export PYTORCH_CUDA_ALLOC_CONF="max_split_size_mb:256"
   ```

### Monitoring Commands

```bash
# Check GPU usage during streaming
watch -n 5 nvidia-smi

# Monitor upload progress
tail -f /scratch/cohenr/logs/stream_tar_*.out | grep -i upload

# Check temp directory usage
du -sh /scratch/cohenr/stream_temp_*

# Monitor database activity
psql "$DB_CREDS" -c "SELECT COUNT(*) FROM audio_files WHERE created_at > NOW() - INTERVAL '1 hour'"
```

## Integration

The streaming pipeline is completely separate from the original pipeline and can be used as a drop-in replacement:

1. Replace `process_audio_job.sh` with `process_single_tar_streaming.sh`
2. Replace `process_day_parallel.sh` with `process_day_streaming.sh`  
3. Use the streaming container instead of the original container
4. Adjust resource allocation for streaming workloads

The same database schema and monitoring tools work with both pipelines.