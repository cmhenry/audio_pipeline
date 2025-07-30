#!/usr/bin/env python3
# save_globus_tokens.py - Run this ONCE to save tokens permanently

import json
import os
from globus_sdk import NativeAppAuthClient

# Replace with your Client ID from the Native App registration
CLIENT_ID = "caac995b-9dd6-4e1d-b150-d36581a70de9"

def get_and_save_tokens():
    """One-time setup to get refresh tokens"""
    
    # Create native app client
    client = NativeAppAuthClient(CLIENT_ID)
    
    # Start OAuth2 flow with refresh tokens
    client.oauth2_start_flow(
        requested_scopes=[
            "urn:globus:auth:scope:transfer.api.globus.org:all",
            "urn:globus:auth:scope:flows.globus.org:all",
            "openid", 
            "profile"
        ],
        refresh_tokens=True  # CRITICAL - enables refresh tokens!
    )
    
    # Get auth URL
    auth_url = client.oauth2_get_authorize_url()
    
    print("\n" + "="*70)
    print("ONE-TIME SETUP - After this, you'll never need to authenticate again")
    print("="*70)
    print("\n1. Go to this URL in your browser:\n")
    print(auth_url)
    print("\n2. Log in with your institutional credentials")
    print("3. You'll get an authorization code")
    print("="*70 + "\n")
    
    # Get auth code
    auth_code = input("Enter the authorization code here: ").strip()
    
    # Exchange for tokens
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    
    # Extract refresh tokens (these last "forever")
    tokens = {
        "CLIENT_ID": CLIENT_ID,
        "TRANSFER_REFRESH_TOKEN": token_response.by_resource_server['transfer.api.globus.org']['refresh_token'],
        "AUTH_REFRESH_TOKEN": token_response.by_resource_server['auth.globus.org']['refresh_token']
    }
    
    # Save to file
    token_file = os.path.expanduser("~/.globus_refresh_tokens.json")
    
    with open(token_file, 'w') as f:
        json.dump(tokens, f, indent=2)
    
    # Secure the file (readable only by you)
    os.chmod(token_file, 0o600)
    
    print("\n" + "="*70)
    print("SUCCESS! Tokens saved to:", token_file)
    print("="*70)
    print("\nYou can now use these tokens FOREVER in your scripts.")
    print("Copy the use_tokens.py script below to see how.")
    print("\nKEEP THAT FILE SECURE - it's your permanent access key!")

if __name__ == "__main__":
    get_and_save_tokens()