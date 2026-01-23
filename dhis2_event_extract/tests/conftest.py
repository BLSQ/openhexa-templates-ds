import os
import pathlib

import pytest
from openhexa.toolbox.dhis2 import DHIS2


@pytest.fixture(scope="session")
def dhis2_env():
    """DHIS2 environment configuration with multiple fallback options."""
    # Working demo servers from https://play.im.dhis2.org/
    demo_urls = [
        "https://play.im.dhis2.org/stable-2-39-10-1",  # Current working server
        "https://play.im.dhis2.org/stable-2-41-3-1",   # Alternative server
        "https://play.im.dhis2.org/stable-2-39-9-1",   # Fallback server
    ]
    local_url = "http://localhost:8080"
    
    # Use environment variable if set, otherwise prefer first demo server
    dhis2_url = os.getenv("DHIS2_URL")
    if not dhis2_url:
        dhis2_url = demo_urls[0]
    
    return {
        "url": dhis2_url,
        "user": os.getenv("DHIS2_USER", "admin"),
        "password": os.getenv("DHIS2_PASS", "district"),
        "demo_urls": demo_urls,
        "local_url": local_url,
    }


@pytest.fixture(scope="session")
def dhis2_client(dhis2_env):
    """DHIS2 client with automatic fallback to multiple servers."""
    # List of DHIS2 servers to try in order
    servers_to_try = [(dhis2_env["url"], "configured")]
    
    # Add demo servers as fallback
    for i, demo_url in enumerate(dhis2_env["demo_urls"]):
        servers_to_try.append((demo_url, f"demo server {i + 1}"))
    
    # Add local server as last resort
    servers_to_try.append((dhis2_env["local_url"], "local server"))
    
    # Remove duplicates while preserving order
    unique_servers = []
    seen = set()
    for url, name in servers_to_try:
        if url not in seen:
            unique_servers.append((url, name))
            seen.add(url)
    
    last_error = None
    
    for url, server_name in unique_servers:
        try:
            print(f"\n🔗 Trying DHIS2 {server_name}: {url}")
            client = DHIS2(
                url=url,
                username=dhis2_env["user"],
                password=dhis2_env["password"],
            )
            
            # Test connection
            info = client.api.get("system/info")
            print(f"✅ Connected to DHIS2 {server_name} (version: {info.get('version', 'unknown')})")
            return client
            
        except Exception as e:
            print(f"❌ Failed to connect to DHIS2 {server_name}: {e}")
            last_error = e
            continue
    
    # If all servers failed, create client anyway for graceful test skipping
    print("\n⚠️  No DHIS2 servers accessible. Integration tests will skip gracefully.")
    print(f"   Last error: {last_error}")
    
    return DHIS2(
        url=dhis2_env["url"],
        username=dhis2_env["user"],
        password=dhis2_env["password"],
    )


@pytest.fixture
def workspace_test_programs():
    """Load the workspace test programs file with real DHIS2 UIDs."""
    import os
    workspace_dir = os.path.join(pathlib.Path(__file__).parent, "..", "workspace")
    programs_file = os.path.join(workspace_dir, "test_programs.json")
    
    if not pathlib.Path(programs_file).exists():
        pytest.skip(f"Workspace test programs file not found: {programs_file}")
    
    return programs_file


@pytest.fixture
def dynamic_dhis2_data(dhis2_client, tmp_path, dhis2_env):
    """Dynamically discover real DHIS2 programs and events for testing."""
    try:
        print(f"\n🔍 Discovering programs from DHIS2: {dhis2_env['url']}")
        
        # Get real programs
        programs_response = dhis2_client.api.get("programs", params={
            "fields": "id,name,programStages[id,name]", "pageSize": "5"
        })
        programs = programs_response.get("programs", [])
        print(f"   📊 Found {len(programs)} programs")
        
        # Get real org units
        org_units_response = dhis2_client.api.get("organisationUnits", params={
            "fields": "id,name", "pageSize": "5"
        })
        org_units = org_units_response.get("organisationUnits", [])
        print(f"   🏢 Found {len(org_units)} organization units")
        
        if programs and org_units:
            # Use first program with stages
            program_with_stages = None
            for program in programs:
                if program.get("programStages") and len(program["programStages"]) > 0:
                    program_with_stages = program
                    break
            
            if program_with_stages:
                test_data = {
                    "program": program_with_stages,
                    "org_units": org_units[:3],  # Use first 3 org units
                    "program_stages": program_with_stages["programStages"]
                }
                
                print(f"   ✅ Using program: {program_with_stages['name']} ({program_with_stages['id']})")
                return test_data
            pytest.skip("No programs with stages found")
        else:
            pytest.skip("No programs or org units found")
            
    except Exception as e:
        pytest.skip(f"DHIS2 not accessible for dynamic data discovery: {e}")