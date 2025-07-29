#!/bin/bash
# setup_globus_flow.sh - Deploy Globus Flow for audio pipeline transfers

set -e

echo "=== Globus Flow Setup for Audio Pipeline ==="
echo "This script deploys the Globus Flow definition for automated transfers"
echo

# Configuration
SCRIPT_DIR="$(dirname "$0")/src"
CONTAINER_DIR="$(dirname "$0")/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
FLOW_DEF_FILE="${SCRIPT_DIR}/audio_transfer_flow.json"

# Check if container exists
if [ ! -f "$PIPELINE_UTILS_SIF" ]; then
    echo "Error: Pipeline utils container not found at $PIPELINE_UTILS_SIF"
    echo "Please build the containers first:"
    echo "  cd containers && ./build_containers.sh"
    exit 1
fi

# Check if flow definition exists
if [ ! -f "$FLOW_DEF_FILE" ]; then
    echo "Error: Flow definition not found at $FLOW_DEF_FILE"
    exit 1
fi

# Check for access token
if [ -z "$GLOBUS_ACCESS_TOKEN" ]; then
    echo "Error: GLOBUS_ACCESS_TOKEN environment variable not set"
    echo
    echo "To get an access token:"
    echo "1. Go to https://app.globus.org/settings/developers"
    echo "2. Create a new app or use existing one"
    echo "3. Generate an access token with Transfer and Flow scopes"
    echo "4. Export the token: export GLOBUS_ACCESS_TOKEN=your_token_here"
    echo
    exit 1
fi

echo "✓ Container found: $PIPELINE_UTILS_SIF"
echo "✓ Flow definition found: $FLOW_DEF_FILE"
echo "✓ Access token configured"
echo

# Deploy the flow
echo "Deploying Globus Flow..."
DEPLOY_OUTPUT=$(singularity run \
    --bind "${SCRIPT_DIR}:/opt/audio_pipeline/src" \
    "${PIPELINE_UTILS_SIF}" \
    /opt/audio_pipeline/src/globus_flow_manager.py deploy \
    /opt/audio_pipeline/src/audio_transfer_flow.json \
    --title "Audio Pipeline Transfer Flow")

if [ $? -eq 0 ]; then
    # Extract Flow ID from output
    FLOW_ID=$(echo "$DEPLOY_OUTPUT" | grep "Flow deployed:" | cut -d' ' -f3)
    
    if [ -n "$FLOW_ID" ]; then
        echo "✓ Flow deployed successfully!"
        echo "Flow ID: $FLOW_ID"
        echo
        echo "=== Next Steps ==="
        echo "1. Set the Flow ID environment variable:"
        echo "   export AUDIO_TRANSFER_FLOW_ID=$FLOW_ID"
        echo
        echo "2. Test the flow:"
        echo "   singularity run --bind ./src:/opt/audio_pipeline/src $PIPELINE_UTILS_SIF \\"
        echo "     /opt/audio_pipeline/src/globus_flow_manager.py run \\"
        echo "     --date 2025-01-25 \\"
        echo "     --source-endpoint YOUR_SOURCE_ENDPOINT \\"
        echo "     --dest-endpoint YOUR_DEST_ENDPOINT \\"
        echo "     --source-path /path/to/source/ \\"
        echo "     --dest-path /path/to/dest/ \\"
        echo "     --monitor"
        echo
        echo "3. Update your environment configuration:"
        echo "   Add 'export AUDIO_TRANSFER_FLOW_ID=$FLOW_ID' to your profile or job scripts"
        echo
        echo "4. The transfer job script will now use the Flow automatically"
        
        # Optionally save to a config file
        echo "AUDIO_TRANSFER_FLOW_ID=$FLOW_ID" > "${SCRIPT_DIR}/../flow_config.env"
        echo "✓ Flow ID saved to flow_config.env"
        
    else
        echo "✗ Could not extract Flow ID from deployment output"
        echo "Output: $DEPLOY_OUTPUT"
        exit 1
    fi
else
    echo "✗ Flow deployment failed"
    echo "Output: $DEPLOY_OUTPUT"
    exit 1
fi

echo
echo "=== Flow Setup Complete ==="
echo "Your audio pipeline is now configured to use Globus Flows for automated transfers!"