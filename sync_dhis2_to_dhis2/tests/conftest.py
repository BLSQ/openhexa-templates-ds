import json
import os

import pytest
from openhexa.toolbox.dhis2 import DHIS2


@pytest.fixture(scope="session")
def dhis2_env():
    """DHIS2 environment configuration with multiple fallback options."""
    # Working demo servers from https://play.im.dhis2.org/
    demo_urls = [
        "https://play.im.dhis2.org/stable-2-39-10-1",  # DHIS2 2.39 demo (currently working)
        "https://play.im.dhis2.org/stable-2-41-3-1",   # DHIS2 2.41 demo
        "https://play.im.dhis2.org/stable-2-39-9-1",   # DHIS2 2.39 demo (older)
    ]
    local_url = "http://localhost:8080" 
    
    # Use environment variable if set, otherwise prefer first demo server
    dhis2_url = os.getenv("DHIS2_URL")
    if not dhis2_url:
        dhis2_url = demo_urls[0]  # Default to newest demo server
    
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
    servers_to_try = [
        (dhis2_env["url"], "configured"),
    ]
    
    # Add demo servers as fallback
    for i, demo_url in enumerate(dhis2_env["demo_urls"]):
        servers_to_try.append((demo_url, f"demo server {i+1}"))
    
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
            
            # Test the connection
            info = client.api.get("system/info")
            print(f"✅ Connected to DHIS2 {server_name} (version: {info.get('version', 'unknown')})")
            return client
            
        except Exception as e:
            print(f"❌ Failed to connect to DHIS2 {server_name}: {e}")
            last_error = e
            continue
    
    # If all servers failed, create a client anyway for graceful test skipping
    print(f"\n⚠️  No DHIS2 servers accessible. Integration tests will skip gracefully.")
    print(f"   Last error: {last_error}")
    
    # Return client with the primary configured URL for error reporting
    return DHIS2(
        url=dhis2_env["url"],
        username=dhis2_env["user"],
        password=dhis2_env["password"],
    )


@pytest.fixture
def sample_mapping(tmp_path):
    data = {
        "dataElements": {"SRC_DE": "TGT_DE"},
        "categoryOptionCombos": {"SRC_COC": "TGT_COC"},
        "orgUnits": {"SRC_OU": "TGT_OU"}
    }
    p = tmp_path / "mapping.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


@pytest.fixture
def workspace_test_mapping():
    """Load the workspace test mapping file with real DHIS2 UIDs."""
    import os
    workspace_dir = os.path.join(os.path.dirname(__file__), "..", "workspace")
    mapping_file = os.path.join(workspace_dir, "test_mapping.json")
    
    if not os.path.exists(mapping_file):
        pytest.skip(f"Workspace test mapping file not found: {mapping_file}")
    
    return mapping_file


@pytest.fixture
def dynamic_dhis2_data(dhis2_client, tmp_path, dhis2_env):
    """Dynamically discover real DHIS2 data elements and org units for testing.
    
    CRITICAL: Never hardcode DHIS2 UIDs! Always fetch real data from the running instance.
    """
    try:
        print(f"\n🔍 Discovering data from DHIS2: {dhis2_env['url']}")
        
        # Step 1: Get actual data elements from DHIS2
        data_elements_response = dhis2_client.api.get("dataElements", params={
            "fields": "id,name",
            "pageSize": "10"
        })
        data_elements = data_elements_response.get("dataElements", [])
        print(f"   📊 Found {len(data_elements)} data elements")
        
        # Step 2: Get actual category option combos from DHIS2  
        coc_response = dhis2_client.api.get("categoryOptionCombos", params={
            "fields": "id,name",
            "pageSize": "10"
        })
        category_option_combos = coc_response.get("categoryOptionCombos", [])
        print(f"   📋 Found {len(category_option_combos)} category option combos")
        
        # Step 3: Get actual org units from DHIS2
        org_units_response = dhis2_client.api.get("organisationUnits", params={
            "fields": "id,name",
            "pageSize": "10"
        })
        org_units = org_units_response.get("organisationUnits", [])
        print(f"   🏢 Found {len(org_units)} organization units")
        
        # Step 4: Create mapping with real UIDs (not hardcoded!)
        if data_elements and category_option_combos and org_units:
            real_mapping = {
                "dataElements": {de["id"]: de["id"] for de in data_elements[:5]},
                "categoryOptionCombos": {coc["id"]: coc["id"] for coc in category_option_combos[:5]},
                "orgUnits": {ou["id"]: ou["id"] for ou in org_units[:3]}
            }
            
            # Step 5: Write dynamic mapping file
            mapping_file = tmp_path / "dynamic_dhis2_mapping.json"
            mapping_file.write_text(json.dumps(real_mapping), encoding="utf-8")
            
            print(f"   ✅ Created dynamic mapping with {len(real_mapping['dataElements'])} data elements, {len(real_mapping['categoryOptionCombos'])} COCs, {len(real_mapping['orgUnits'])} org units")
            
            return {
                "mapping_file": mapping_file,
                "data_elements": data_elements,
                "category_option_combos": category_option_combos,
                "org_units": org_units,
                "mapping": real_mapping
            }
        else:
            # Fallback if no data available
            missing_data = []
            if not data_elements:
                missing_data.append("data elements")
            if not category_option_combos:
                missing_data.append("category option combos")
            if not org_units:
                missing_data.append("organization units")
            pytest.skip(f"No DHIS2 data available - DHIS2 instance at {dhis2_env['url']} is missing: {', '.join(missing_data)}")
            
    except Exception as e:
        # Skip integration tests if DHIS2 not accessible
        server_info = f"DHIS2 server: {dhis2_env['url']}"
        pytest.skip(f"DHIS2 not accessible for dynamic data discovery ({server_info}): {e}")

@pytest.fixture
def sierra_leone_mapping(dynamic_dhis2_data):
    """DEPRECATED: Use dynamic_dhis2_data instead. 
    
    This fixture is kept for backward compatibility but will use dynamic data.
    """
    return dynamic_dhis2_data["mapping_file"]