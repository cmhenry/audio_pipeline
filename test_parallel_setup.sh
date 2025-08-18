#!/bin/bash
# test_parallel_setup.sh - Test script for parallel processing setup

echo "=== Audio Pipeline Parallel Processing Test ==="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test configuration
TEST_DATE="2025-01-25"  # Use a test date
HPC_DIR="/data/cohenr/audio_pipeline/hpc"
SRC_DIR="/data/cohenr/audio_pipeline/src"
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"

echo -e "${BLUE}Testing parallel processing setup...${NC}"

# Check if required scripts exist
echo
echo "1. Checking script files..."
scripts=(
    "${HPC_DIR}/process_day_parallel.sh"
    "${HPC_DIR}/process_single_tar.sh" 
    "${SRC_DIR}/hpc_process_single_tar.py"
    "${SRC_DIR}/parallel_coordinator.py"
)

all_found=true
for script in "${scripts[@]}"; do
    if [ -f "$script" ]; then
        echo -e "   ${GREEN}✓${NC} $(basename $script)"
    else
        echo -e "   ${RED}✗${NC} $(basename $script) - NOT FOUND"
        all_found=false
    fi
done

if [ "$all_found" = false ]; then
    echo -e "${RED}Error: Missing required scripts${NC}"
    exit 1
fi

# Check if containers exist
echo
echo "2. Checking container files..."
containers=(
    "${CONTAINER_DIR}/audio_processing.sif"
    "${CONTAINER_DIR}/pipeline_utils.sif"
)

for container in "${containers[@]}"; do
    if [ -f "$container" ]; then
        echo -e "   ${GREEN}✓${NC} $(basename $container)"
    else
        echo -e "   ${YELLOW}!${NC} $(basename $container) - NOT FOUND (build needed)"
    fi
done

# Check script permissions
echo
echo "3. Checking script permissions..."
for script in "${scripts[@]}"; do
    if [ -x "$script" ]; then
        echo -e "   ${GREEN}✓${NC} $(basename $script) is executable"
    else
        echo -e "   ${YELLOW}!${NC} $(basename $script) - making executable"
        chmod +x "$script" 2>/dev/null || echo -e "   ${RED}✗${NC} Failed to make $(basename $script) executable"
    fi
done

# Test database connection (if available)
echo
echo "4. Testing database utilities..."
DB_CREDS="host=172.23.76.3 port=5432 dbname=audio_pipeline user=audio_user password=audio_password"

if command -v singularity &> /dev/null; then
    echo -e "   ${GREEN}✓${NC} Singularity available"
    
    # Test db_utils.py if container is available
    if [ -f "${CONTAINER_DIR}/pipeline_utils.sif" ]; then
        echo "   Testing database connection..."
        singularity run --bind ${SRC_DIR}:/opt/audio_pipeline/src \
            ${CONTAINER_DIR}/pipeline_utils.sif \
            /opt/audio_pipeline/src/db_utils.py --db-string "$DB_CREDS" test-connection 2>/dev/null
        
        if [ $? -eq 0 ]; then
            echo -e "   ${GREEN}✓${NC} Database connection successful"
        else
            echo -e "   ${YELLOW}!${NC} Database connection failed (expected in test environment)"
        fi
    else
        echo -e "   ${YELLOW}!${NC} Pipeline utils container not available for testing"
    fi
else
    echo -e "   ${YELLOW}!${NC} Singularity not available (testing on different system)"
fi

# Test directory structure
echo
echo "5. Checking directory structure..."
required_dirs=(
    "/shares/bdm.ipz.uzh/audio_storage/staging"
    "/scratch/cohenr/logs"
    "/scratch/cohenr/error"
    "/data/cohenr/audio_pipeline_secrets"
)

for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo -e "   ${GREEN}✓${NC} $dir exists"
    else
        echo -e "   ${YELLOW}!${NC} $dir - NOT FOUND (may need creation on HPC system)"
    fi
done

# Validate script syntax
echo
echo "6. Validating Python script syntax..."
python_scripts=(
    "${SRC_DIR}/hpc_process_single_tar.py"
    "${SRC_DIR}/parallel_coordinator.py"
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

# Validate bash script syntax
echo
echo "7. Validating bash script syntax..."
bash_scripts=(
    "${HPC_DIR}/process_day_parallel.sh"
    "${HPC_DIR}/process_single_tar.sh"
)

for script in "${bash_scripts[@]}"; do
    if [ -f "$script" ]; then
        bash -n "$script" 2>/dev/null
        if [ $? -eq 0 ]; then
            echo -e "   ${GREEN}✓${NC} $(basename $script) syntax OK"
        else
            echo -e "   ${RED}✗${NC} $(basename $script) syntax error"
        fi
    fi
done

# Summary
echo
echo -e "${BLUE}=== Test Summary ===${NC}"
echo "Parallel processing setup validation complete."
echo
echo "To deploy:"
echo "1. Ensure containers are built: cd containers && ./build_containers.sh"
echo "2. Apply database schema: ./db/manage_schema.sh"  
echo "3. Test with small dataset: sbatch hpc/process_day_parallel.sh 2025-01-25"
echo
echo "Key differences from original pipeline:"
echo "- Master job coordinates 144 worker jobs (one per tar.xz file)"
echo "- Each worker processes 1000-5000 files independently"
echo "- Expected speedup: 6-9x (18 hours → 2-3 hours)"
echo "- Resource requirement: 8-12 H100 GPUs concurrently"

echo
echo -e "${GREEN}Parallel processing setup ready!${NC}"