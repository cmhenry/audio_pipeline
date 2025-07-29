#!/bin/bash
# test_flow_deployment.sh - Test Globus Flow deployment and basic functionality

set -e

echo "=== Globus Flow Deployment Test ==="
echo "This script tests the Flow definition and deployment process"
echo

# Configuration
SCRIPT_DIR="$(dirname "$0")/src"
CONTAINER_DIR="$(dirname "$0")/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
FLOW_DEF_FILE="${SCRIPT_DIR}/audio_transfer_flow.json"

# Check prerequisites
echo "1. Checking prerequisites..."

if [ ! -f "$PIPELINE_UTILS_SIF" ]; then
    echo "✗ Pipeline utils container not found at $PIPELINE_UTILS_SIF"
    echo "Please build the containers first: cd containers && ./build_containers.sh"
    exit 1
fi
echo "✓ Container found: $PIPELINE_UTILS_SIF"

if [ ! -f "$FLOW_DEF_FILE" ]; then
    echo "✗ Flow definition not found at $FLOW_DEF_FILE"
    exit 1
fi
echo "✓ Flow definition found: $FLOW_DEF_FILE"

if [ -z "$GLOBUS_ACCESS_TOKEN" ]; then
    echo "✗ GLOBUS_ACCESS_TOKEN environment variable not set"
    echo "Please set your Globus access token first"
    exit 1
fi
echo "✓ Access token configured"

echo

# Validate JSON syntax
echo "2. Validating Flow definition JSON syntax..."
if python3 -m json.tool "$FLOW_DEF_FILE" > /dev/null 2>&1; then
    echo "✓ JSON syntax is valid"
else
    echo "✗ JSON syntax error in Flow definition"
    python3 -m json.tool "$FLOW_DEF_FILE"
    exit 1
fi

echo

# Test deployment (dry run mode if available)
echo "3. Testing Flow deployment..."
DEPLOY_OUTPUT=$(singularity run \
    --bind "${SCRIPT_DIR}:/opt/audio_pipeline/src" \
    "${PIPELINE_UTILS_SIF}" \
    /opt/audio_pipeline/src/globus_flow_manager.py deploy \
    /opt/audio_pipeline/src/audio_transfer_flow.json \
    --title "Audio Pipeline Transfer Flow (Test)" 2>&1)

if [ $? -eq 0 ]; then
    FLOW_ID=$(echo "$DEPLOY_OUTPUT" | grep "Flow deployed:" | cut -d' ' -f3)
    
    if [ -n "$FLOW_ID" ]; then
        echo "✓ Flow deployed successfully!"
        echo "Flow ID: $FLOW_ID"
        
        # Test listing flows to verify deployment
        echo
        echo "4. Verifying Flow in list..."
        LIST_OUTPUT=$(singularity run \
            --bind "${SCRIPT_DIR}:/opt/audio_pipeline/src" \
            "${PIPELINE_UTILS_SIF}" \
            /opt/audio_pipeline/src/globus_flow_manager.py list 2>&1)
        
        if echo "$LIST_OUTPUT" | grep -q "$FLOW_ID"; then
            echo "✓ Flow found in listing"
        else
            echo "⚠ Flow not found in listing (may take time to propagate)"
        fi
        
        echo
        echo "=== Test Results ==="
        echo "✓ JSON syntax validation: PASSED"
        echo "✓ Flow deployment: PASSED"
        echo "✓ Flow ID: $FLOW_ID"
        echo
        echo "=== Next Steps ==="
        echo "1. Set the Flow ID environment variable:"
        echo "   export AUDIO_TRANSFER_FLOW_ID=$FLOW_ID"
        echo
        echo "2. Test the Flow with actual endpoints:"
        echo "   singularity run --bind ./src:/opt/audio_pipeline/src $PIPELINE_UTILS_SIF \\"
        echo "     /opt/audio_pipeline/src/globus_flow_manager.py run \\"
        echo "     --date 2025-01-25 \\"
        echo "     --source-endpoint YOUR_SOURCE_ENDPOINT \\"
        echo "     --dest-endpoint YOUR_DEST_ENDPOINT \\"
        echo "     --source-path /path/to/source/ \\"
        echo "     --dest-path /path/to/dest/"
        echo
        echo "3. The transfer job script will use this Flow automatically"
        
    else
        echo "✗ Could not extract Flow ID from deployment output"
        echo "Deployment output:"
        echo "$DEPLOY_OUTPUT"
        exit 1
    fi
else
    echo "✗ Flow deployment failed"
    echo "Error output:"
    echo "$DEPLOY_OUTPUT"
    exit 1
fi

echo
echo "=== Test Complete ==="
echo "Flow deployment test completed successfully!"