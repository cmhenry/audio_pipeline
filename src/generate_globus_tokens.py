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
            "https://auth.globus.org/scopes/bbe2c78f-b7e4-490c-99de-f2b49b6cbb42/flow_bbe2c78f_b7e4_490c_99de_f2b49b6cbb42_user",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/all",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run_status",
            "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run_manage",
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
    
    # Extract refresh tokens
    tokens = {
        "CLIENT_ID": CLIENT_ID,
        "TRANSFER_REFRESH_TOKEN": token_response.by_resource_server['transfer.api.globus.org']['refresh_token'],
        "AUTH_REFRESH_TOKEN": token_response.by_resource_server['auth.globus.org']['refresh_token']
    }

    # The Flows tokens might be under a different resource server key
    if 'flows.globus.org' in token_response.by_resource_server:
        tokens["FLOWS_REFRESH_TOKEN"] = token_response.by_resource_server['flows.globus.org']['refresh_token']
    elif 'eec9b274-0c81-4334-bdc2-54e90e689b9a' in token_response.by_resource_server:
        # Sometimes it's under the service UUID
        tokens["FLOWS_REFRESH_TOKEN"] = token_response.by_resource_server['eec9b274-0c81-4334-bdc2-54e90e689b9a']['refresh_token']
    # Save the flow-specific token using the flow ID as key
    flow_id = 'bbe2c78f-b7e4-490c-99de-f2b49b6cbb42'
    if flow_id in token_response.by_resource_server:
        tokens[flow_id] = token_response.by_resource_server[flow_id]['refresh_token']   

    # Debug: print all resource servers to see what we got
    print("\nResource servers in response:")
    for server in token_response.by_resource_server.keys():
        print(f"  - {server}")
    
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