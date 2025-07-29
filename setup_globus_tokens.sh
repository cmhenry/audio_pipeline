#!/bin/bash
# generate_globus_tokens.sh - Run token generation in Singularity container

# Container and script paths
CONTAINER_DIR="/data/cohenr/audio_pipeline/containers"
PIPELINE_UTILS_SIF="${CONTAINER_DIR}/pipeline_utils.sif"
SCRIPT_DIR="/data/cohenr/audio_pipeline/src"

# Ensure home directory is bound so tokens are saved persistently
echo "Starting Globus token generation..."
echo "This is a ONE-TIME setup process."
echo

# Run the token generation script inside the container
singularity exec \
    --bind ${SCRIPT_DIR}:/opt/audio_pipeline/src \
    --bind ${HOME}:${HOME} \
    ${PIPELINE_UTILS_SIF} \
    python /opt/audio_pipeline/src/generate_globus_tokens.py

# Check if token file was created
if [ -f "${HOME}/.globus_refresh_tokens.json" ]; then
    echo
    echo "✓ Token generation complete!"
    echo "Tokens saved to: ${HOME}/.globus_refresh_tokens.json"
    echo "You can now use these tokens in all your Globus scripts."
else
    echo
    echo "✗ Token generation may have failed."
    echo "Please check the output above for errors."
fi