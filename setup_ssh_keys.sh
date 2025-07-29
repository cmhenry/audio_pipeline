#!/bin/bash
# setup_ssh_keys.sh - Helper script for setting up SSH key authentication for rsync

set -e

echo "=== Audio Pipeline SSH Key Setup ==="
echo "This script helps set up SSH key authentication for rsync transfers"
echo

# Get database host
read -p "Enter database host IP/hostname: " DB_HOST
read -p "Enter rsync username (default: audio_user): " RSYNC_USER
RSYNC_USER=${RSYNC_USER:-audio_user}

echo
echo "Setting up SSH key authentication for ${RSYNC_USER}@${DB_HOST}"

# Check if SSH key exists
SSH_KEY_PATH="$HOME/.ssh/id_rsa"
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo
    echo "No SSH key found. Generating new SSH key..."
    ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PATH" -N ""
    echo "✓ SSH key generated at $SSH_KEY_PATH"
else
    echo "✓ SSH key already exists at $SSH_KEY_PATH"
fi

# Copy public key to target server
echo
echo "Copying public key to target server..."
echo "You'll be prompted for the password for ${RSYNC_USER}@${DB_HOST}"

if ssh-copy-id -i "${SSH_KEY_PATH}.pub" "${RSYNC_USER}@${DB_HOST}"; then
    echo "✓ Public key copied successfully"
else
    echo "✗ Failed to copy public key"
    echo "You may need to manually copy the key:"
    echo "cat ${SSH_KEY_PATH}.pub"
    echo
    echo "Then on the target server, add it to ~/.ssh/authorized_keys"
    exit 1
fi

# Test SSH connection
echo
echo "Testing SSH connection..."
if ssh -o ConnectTimeout=10 "${RSYNC_USER}@${DB_HOST}" "echo 'SSH connection successful'"; then
    echo "✓ SSH connection works"
else
    echo "✗ SSH connection failed"
    exit 1
fi

# Test rsync
echo
echo "Testing rsync..."
TEST_FILE="/tmp/rsync_test_$$"
echo "test content" > "$TEST_FILE"

if rsync "$TEST_FILE" "${RSYNC_USER}@${DB_HOST}:/tmp/"; then
    echo "✓ Rsync test successful"
    rm -f "$TEST_FILE"
    ssh "${RSYNC_USER}@${DB_HOST}" "rm -f /tmp/$(basename $TEST_FILE)"
else
    echo "✗ Rsync test failed"
    rm -f "$TEST_FILE"
    exit 1
fi

# Create storage directory on target
echo
echo "Creating audio storage directory on target..."
STORAGE_ROOT="/opt/audio_storage"
if ssh "${RSYNC_USER}@${DB_HOST}" "sudo mkdir -p $STORAGE_ROOT && sudo chown ${RSYNC_USER}: $STORAGE_ROOT"; then
    echo "✓ Storage directory created: $STORAGE_ROOT"
else
    echo "⚠ Could not create storage directory. You may need to create it manually:"
    echo "  sudo mkdir -p $STORAGE_ROOT"
    echo "  sudo chown ${RSYNC_USER}: $STORAGE_ROOT"
fi

echo
echo "=== Setup Complete ==="
echo "SSH key authentication is now configured for:"
echo "  User: ${RSYNC_USER}"  
echo "  Host: ${DB_HOST}"
echo "  Storage: ${STORAGE_ROOT}"
echo
echo "You can now run the audio processing pipeline with rsync storage."
echo
echo "To test the storage manager, you can run:"
echo "python -c \"from storage_manager import create_storage_manager; sm = create_storage_manager('$DB_HOST'); print('Storage manager ready')\""