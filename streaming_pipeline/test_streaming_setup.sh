#!/bin/bash
# test_streaming_setup.sh - Test streaming pipeline setup

echo "=========================================="
echo "Streaming Audio Pipeline Setup Test"
echo "=========================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test configuration
TEST_DATE="2025-01-25"
STREAMING_DIR="/data/cohenr/audio_pipeline/streaming_pipeline"
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"

echo -e "${BLUE}Testing streaming pipeline setup...${NC}"

# Check if streaming directory structure exists
echo
echo "1. Checking streaming directory structure..."
streaming_dirs=(
    "${STREAMING_DIR}/src"
    "${STREAMING_DIR}/hpc" 
    "${STREAMING_DIR}/containers"
)

all_dirs_found=true
for dir in "${streaming_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo -e "   ${GREEN}✓${NC} $(basename $dir) directory exists"
    else
        echo -e "   ${RED}✗${NC} $(basename $dir) directory missing: $dir"
        all_dirs_found=false
    fi
done

# Check streaming source files
echo
echo "2. Checking streaming source files..."
streaming_files=(
    "${STREAMING_DIR}/src/streaming_tar_processor.py"
    "${STREAMING_DIR}/src/streaming_storage_manager.py"
)

all_files_found=true
for file in "${streaming_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "   ${GREEN}✓${NC} $(basename $file)"
    else
        echo -e "   ${RED}✗${NC} $(basename $file) - NOT FOUND"
        all_files_found=false
    fi
done

# Check HPC scripts
echo
echo "3. Checking HPC scripts..."
hpc_scripts=(
    "${STREAMING_DIR}/hpc/process_single_tar_streaming.sh"
    "${STREAMING_DIR}/hpc/process_day_streaming.sh"
)

for script in "${hpc_scripts[@]}"; do
    if [ -f "$script" ]; then
        if [ -x "$script" ]; then
            echo -e "   ${GREEN}✓${NC} $(basename $script) (executable)"
        else
            echo -e "   ${YELLOW}!${NC} $(basename $script) (not executable, fixing...)"
            chmod +x "$script" 2>/dev/null || echo -e "   ${RED}✗${NC} Failed to make executable"
        fi
    else
        echo -e "   ${RED}✗${NC} $(basename $script) - NOT FOUND"
        all_files_found=false
    fi
done

# Check container definition
echo
echo "4. Checking container definition..."
container_files=(
    "${STREAMING_DIR}/containers/streaming_audio_processing.def"
    "${STREAMING_DIR}/containers/build_streaming_containers.sh"
)

for file in "${container_files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "   ${GREEN}✓${NC} $(basename $file)"
    else
        echo -e "   ${RED}✗${NC} $(basename $file) - NOT FOUND"
        all_files_found=false
    fi
done

# Check Python syntax
echo
echo "5. Validating Python streaming scripts..."
python_scripts=(
    "${STREAMING_DIR}/src/streaming_tar_processor.py"
    "${STREAMING_DIR}/src/streaming_storage_manager.py"
)

for script in "${python_scripts[@]}"; do
    if [ -f "$script" ]; then
        python3 -m py_compile "$script" 2>/dev/null
        if [ $? -eq 0 ]; then
            echo -e "   ${GREEN}✓${NC} $(basename $script) syntax OK"
        else
            echo -e "   ${RED}✗${NC} $(basename $script) syntax error"
        fi
    fi
done

# Check bash syntax
echo
echo "6. Validating bash scripts..."
for script in "${hpc_scripts[@]}"; do
    if [ -f "$script" ]; then
        bash -n "$script" 2>/dev/null
        if [ $? -eq 0 ]; then
            echo -e "   ${GREEN}✓${NC} $(basename $script) syntax OK"
        else
            echo -e "   ${RED}✗${NC} $(basename $script) syntax error"
        fi
    fi
done

# Check differences from original pipeline
echo
echo "7. Streaming pipeline key differences:"
echo -e "   ${BLUE}Stream Processing:${NC} Extract files in small batches (100-200 files)"
echo -e "   ${BLUE}Async Uploads:${NC} Background rsync with ThreadPoolExecutor"
echo -e "   ${BLUE}Memory Efficient:${NC} Process-then-delete, no full tar extraction"
echo -e "   ${BLUE}Timeout Prevention:${NC} Shorter operations, faster cleanup"
echo -e "   ${BLUE}Resource Usage:${NC} Higher CPU/memory per job, fewer concurrent jobs"

# Show configuration recommendations
echo
echo "8. Recommended streaming configuration:"
echo "   Stream batch size: 100-200 files (vs full tar extraction)"
echo "   Process batch size: 50-100 files (vs 500 in original)"
echo "   Concurrent uploads: 6-8 threads"
echo "   Upload timeout: 120-180 seconds"
echo "   Max concurrent jobs: 6-8 (vs 12 in original)"
echo "   Job time limit: 2 hours (vs 18 hours in original)"

# Summary
echo
echo -e "${BLUE}=== Test Summary ===${NC}"

if [ "$all_dirs_found" = true ] && [ "$all_files_found" = true ]; then
    echo -e "${GREEN}✅ Streaming pipeline setup is complete and ready${NC}"
    echo
    echo "To deploy:"
    echo "1. Build container: cd streaming_pipeline/containers && sudo ./build_streaming_containers.sh"
    echo "2. Deploy to HPC: rsync -av streaming_pipeline/ hpc:/data/cohenr/audio_pipeline/"
    echo "3. Test single file: sbatch streaming_pipeline/hpc/process_single_tar_streaming.sh 2025-01-25 /path/to/file.tar.xz 12_30"
    echo "4. Run full day: sbatch streaming_pipeline/hpc/process_day_streaming.sh 2025-01-25"
    echo
    echo -e "${YELLOW}Key Benefits:${NC}"
    echo "• No tar extraction timeouts (stream processing)"
    echo "• No upload timeouts (async background uploads)"
    echo "• Lower memory usage (process in small batches)"
    echo "• Faster individual file processing"
    echo "• Better resource utilization"
else
    echo -e "${RED}❌ Streaming pipeline setup has missing components${NC}"
    echo "Please check the missing files and directories above"
fi

echo
echo -e "${BLUE}Streaming vs Original Pipeline:${NC}"
echo "Original: Extract all → Convert all → Transcribe all → Upload all"
echo "Streaming: Stream extract → Convert batch → Transcribe batch → Queue upload → Repeat"
echo "Result: Prevents timeouts, reduces memory, enables async processing"
echo "=========================================="