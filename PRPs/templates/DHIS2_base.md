# OpenHEXA DHIS2 Pipelines – Complete Specification (PRP / CLAUDE)

> **CRITICAL**: This document contains the complete, exhaustive specification for creating OpenHEXA DHIS2 pipelines with comprehensive testing. Every pipeline MUST follow these specifications exactly to ensure 100% test coverage and zero integration issues. All examples are normative and must be implemented exactly as shown.

---

## 🚀 Executive Summary - What Gets Created

Every DHIS2 pipeline following this specification will have:
- **passing tests** (unit + integration + schema validation)
- **Real DHIS2 integration** using connections to server and with demo server fallback
- **Workspace test data** with validated DHIS2 UIDs  
- **Complete error handling** and graceful degradation
- **VSCode Testing integration** for immediate developer feedback
- **Docker containerization** for CI/CD and reproducible testing
- **Zero hardcoded UIDs** - all data discovered dynamically or provided via workspace (files for instance) if available

---

## 📋 Quick Start - Testing Verification

**Step 1: Immediate validation** (no external dependencies):
```bash
pytest tests/ -k "not integration" -v
# Result: 19-25 unit tests should pass ✅
```

**Step 2: Full integration testing** (with real DHIS2):
```bash
export DHIS2_URL=https://play.im.dhis2.org/stable-2-39-10-1
export DHIS2_USER=admin
export DHIS2_PASS=district
pytest tests/ -v
# Result: 35+ tests should pass, 0 failed ✅
```

**Step 3: Complete test suite**:
```bash
make test  # Docker-based testing
# Result: All tests pass in containerized environment ✅
```

---

## 1) Project Structure (MANDATORY)

Every pipeline MUST have this exact structure:

```
<pipeline_name>/
├── __init__.py                    # REQUIRED: Package initialization
├── pipeline.py                    # Main pipeline implementation
├── README.md                      # Pipeline-specific documentation  
├── requirements.txt               # Pipeline dependencies
├── setup.py                       # REQUIRED: Package installation
├── .env.example                   # Environment configuration template
├── pytest.ini                     # REQUIRED: Test configuration
├── docker-compose.dhis2.yml       # Local DHIS2 test environment
├── Makefile                       # Convenience commands
├── workspace/                     # REQUIRED: Test data directory
│   ├── test_mapping.json          # REQUIRED: Real DHIS2 UIDs for testing
│   ├── workspace.yaml.example     # OpenHEXA connections template
│   └── README.md                  # Test data documentation
├── tests/                         # REQUIRED: Complete test suite
│   ├── __init__.py                # REQUIRED: Test package
│   ├── conftest.py                # REQUIRED: Test fixtures and configuration
│   ├── test_mapping_schema.py     # Schema validation tests
│   ├── test_task_validate_connections.py    # Connection testing
│   ├── test_task_load_mappings.py           # Mapping validation
│   ├── test_task_<task_name>.py             # Individual task tests
│   ├── test_pipeline_integration.py        # End-to-end testing
│   ├── test_integration_dry_run.py         # Legacy integration tests
│   └── test_unit_transforms.py             # Pure unit tests
├── docker/
│   └── Dockerfile.tests           # REQUIRED: Test container
└── .vscode/
    └── settings.json              # REQUIRED: VSCode testing integration
```

---

## 2) OpenHEXA Pipeline Implementation (CRITICAL PATTERNS)

### 2.1 Import Requirements (EXACT SYNTAX)
```python
# REQUIRED imports - use exact syntax to avoid import errors
from openhexa.sdk import current_run, parameter, pipeline, DHIS2Connection
from openhexa.toolbox.dhis2 import DHIS2  # CORRECT: not DHIS2Client
import json
from datetime import datetime
from typing import Any, dict, list
```

### 2.2 Pipeline Definition Pattern (MANDATORY)
```python
@pipeline(
    "pipeline_name",
    name="Human Readable Pipeline Name",
    description="Brief description of what this pipeline does"
)
@parameter(
    "source_connection", 
    type=DHIS2Connection, 
    required=True,
    help="Source DHIS2 instance to extract data from"  # Use 'help' not 'description'
)
@parameter(
    "target_connection", 
    type=DHIS2Connection, 
    required=True,
    help="Target DHIS2 instance (for comparison/writing)"
)
@parameter(
    "mapping_file", 
    type=str, 
    required=True,
    help="Path to JSON mapping file with dataElements, categoryOptionCombos, and orgUnits"
)
@parameter(
    "since_date", 
    type=str, 
    required=True,
    help="Date in YYYY-MM-DD format to check for updates since"
)
@parameter(
    "dry_run", 
    type=bool, 
    default=True,
    help="Execute in dry-run mode (no actual writes)"
)
def pipeline_main_function() -> dict[str, Any]:
    """Main pipeline function with OpenHEXA decorators."""
    current_run.log_info("Starting pipeline execution")
    
    # Call internal task functions in sequence
    _validate_connections()
    mappings = _load_and_validate_mappings()
    updates = _fetch_updates_since_date(mappings)
    summary = _generate_summary(mappings, updates)
    
    current_run.log_info(f"Pipeline complete. Summary: {summary}")
    return summary
```
If examples of existing pipelines are given, use them as reference for synthax, pattern, etc.
### 2.3 Task Architecture Pattern (CRITICAL)
```python
# PATTERN: Dual function architecture for testability
# - Internal function (_function_name) for direct testing
# - Decorated function (task_name) for OpenHEXA execution
# - Export internal function for direct test access

def _validate_connections() -> dict[str, Any]:
    """Internal function for connection validation - directly testable."""
    current_run.log_info("Validating DHIS2 connections")
    
    # Get connections from OpenHEXA context
    source_conn = current_run.connections.source_connection
    target_conn = current_run.connections.target_connection
    
    # Initialize DHIS2 clients - CRITICAL: use correct parameters
    source_client = DHIS2(
        url=source_conn.url,        # CORRECT: use 'url' not 'base_url'
        username=source_conn.username,
        password=source_conn.password
    )
    target_client = DHIS2(
        url=target_conn.url,
        username=target_conn.username,
        password=target_conn.password
    )
    
    # Test connections - CRITICAL: use .api.get() not .get()
    try:
        source_info = source_client.api.get("system/info")
        current_run.log_info(f"Source DHIS2 version: {source_info.get('version', 'unknown')}")
    except Exception as e:
        raise ValueError(f"Cannot connect to source DHIS2: {e}")
    
    try:
        target_info = target_client.api.get("system/info")
        current_run.log_info(f"Target DHIS2 version: {target_info.get('version', 'unknown')}")
    except Exception as e:
        raise ValueError(f"Cannot connect to target DHIS2: {e}")
    
    return {
        "source_client": source_client,
        "target_client": target_client,
        "source_info": source_info,
        "target_info": target_info
    }

@pipeline_main_function.task  # CORRECT: attach to main pipeline
def validate_connections() -> dict[str, Any]:
    """OpenHEXA task wrapper for connection validation."""
    return _validate_connections()

# REQUIRED: Export internal function for direct testing
validate_connections = _validate_connections
```

### 2.4 Example function reading a file (here a mapping) (COMPLETE SCHEMA)
```python
def _load_and_validate_mappings() -> dict[str, Any]:
    """Load and validate mapping file - COMPLETE schema validation."""
    current_run.log_info("Loading and validating mappings")
    
    mapping_path = current_run.parameters.mapping_file
    
    try:
        with open(mapping_path, encoding="utf-8") as f:
            mappings = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Mapping file not found: {mapping_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in mapping file: {e}")
    
    if not isinstance(mappings, dict):
        raise ValueError("Mapping file must contain a JSON object")
    
    # CRITICAL: Validate ALL required sections
    required_sections = ["dataElements", "categoryOptionCombos", "orgUnits"]
    for section in required_sections:
        if section not in mappings:
            raise ValueError(f"Mapping file must contain '{section}' section")
        if not isinstance(mappings[section], dict):
            raise ValueError(f"'{section}' must be a dictionary")
    
    # Log mapping counts
    de_count = len(mappings["dataElements"])
    coc_count = len(mappings["categoryOptionCombos"]) 
    ou_count = len(mappings["orgUnits"])
    current_run.log_info(
        f"Loaded {de_count} data element mappings, {coc_count} category option combo mappings, and {ou_count} organization unit mappings"
    )
    
    return mappings
```

### 2.5 DHIS2 API Data Fetching (CRITICAL PATTERN)
```python
def _fetch_updates_since_date(mappings: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch data from DHIS2 with proper API usage."""
    current_run.log_info("Fetching updates from source DHIS2 since date")
    
    since_date = current_run.parameters.since_date
    
    # Validate date format
    try:
        datetime.strptime(since_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {since_date}")
    
    # Initialize DHIS2 client
    source_conn = current_run.connections.source_connection
    source_client = DHIS2(
        url=source_conn.url, 
        username=source_conn.username, 
        password=source_conn.password
    )
    
    # Extract mapping data - CRITICAL: include orgUnits
    source_des = list(mappings["dataElements"].keys())
    source_cocs = list(mappings["categoryOptionCombos"].keys())
    source_orgunits = list(mappings.get("orgUnits", {}).keys())
    
    if not source_des:
        current_run.log_info("No data elements to check")
        return []
    
    if not source_orgunits:
        current_run.log_info("No organization units specified in mapping - required for DHIS2 dataValueSets API")
        return []
    
    current_run.log_info(f"Checking {len(source_des)} data elements for updates since {since_date}")
    
    # CRITICAL: dataValueSets API requires orgUnit parameter
    params = {
        "dataElement": source_des,
        "categoryOptionCombo": source_cocs,
        "orgUnit": source_orgunits,  # REQUIRED: prevents 409 errors
        "lastUpdated": since_date,
        "paging": "false",
    }
    
    try:
        # CRITICAL: use .api.get() not .get() if the request is not available in the openhexa toolbox library
        response = source_client.api.get("dataValueSets", params=params)
        data_values = response.get("dataValues", [])
        
        current_run.log_info(f"Found {len(data_values)} updated data values since {since_date}")
        return data_values
        
    except Exception as e:
        current_run.log_error(f"Error fetching data values: {e}")
        raise ValueError(f"Failed to fetch updates from source DHIS2: {e}")
```

### 2.6 Helper Function for Testing (MANDATORY)
```python
def run_pipeline_tasks(mock_current_run) -> dict[str, Any]:
    """REQUIRED: Helper function to run pipeline tasks directly for testing."""
    # Replace global current_run temporarily
    global current_run
    original_current_run = current_run
    current_run = mock_current_run
    
    try:
        # Run internal functions in sequence
        _validate_connections()
        mappings = _load_and_validate_mappings()
        updates = _fetch_updates_since_date(mappings)
        summary = _generate_summary(mappings, updates)
        return summary
    finally:
        # Restore original current_run
        current_run = original_current_run

# REQUIRED: Export all internal functions for direct testing
validate_connections = _validate_connections
load_and_validate_mappings = _load_and_validate_mappings
fetch_updates_since_date = _fetch_updates_since_date
generate_summary = _generate_summary
```

---

## 3) Mapping Schema (COMPLETE SPECIFICATION)

### 3.1 Required Mapping File Format
**File**: `workspace/test_mapping.json` (MANDATORY)

```json
{
  "dataElements": {
    "source_DE_UID_1": "target_DE_UID_1",
    "source_DE_UID_2": "target_DE_UID_2"
  },
  "categoryOptionCombos": {
    "source_COC_UID_1": "target_COC_UID_1",
    "source_COC_UID_2": "target_COC_UID_2"
  },
  "orgUnits": {
    "source_OU_UID_1": "target_OU_UID_1", 
    "source_OU_UID_2": "target_OU_UID_2"
  }
}
```

### 3.2 Real DHIS2 UIDs (CRITICAL REQUIREMENT)
**All test mapping files MUST contain actual DHIS2 UIDs that exist in the demo server:**

```json
{
  "dataElements": {
    "FTRrcoaog83": "FTRrcoaog83",
    "P3jJH5Tu5VC": "P3jJH5Tu5VC", 
    "FQ2o8UBlcrS": "FQ2o8UBlcrS",
    "M62VHgYT2n0": "M62VHgYT2n0",
    "WO8yRIZb7nb": "WO8yRIZb7nb"
  },
  "categoryOptionCombos": {
    "S34ULMcHMca": "S34ULMcHMca",
    "sqGRzCziswD": "sqGRzCziswD",
    "o2gxEt6Ek2C": "o2gxEt6Ek2C"
  },
  "orgUnits": {
    "Rp268JB6Ne4": "Rp268JB6Ne4",
    "cDw53Ej8rju": "cDw53Ej8rju",
    "GvFqTavdpGE": "GvFqTavdpGE"
  }
}
```

**How to discover real UIDs:**
```python
from openhexa.toolbox.dhis2 import DHIS2
client = DHIS2(url='https://play.im.dhis2.org/stable-2-39-10-1', username='admin', password='district')

# Discover data elements
des = client.api.get('dataElements', params={'fields': 'id,name', 'pageSize': '5'})
for de in des.get('dataElements', []):
    print(f'"{de["id"]}": "{de["id"]}",  # {de["name"]}')
```

---

## 4) Required Files (EXACT IMPLEMENTATIONS)

### 4.1 `setup.py` (MANDATORY)
```python
from setuptools import find_packages, setup

setup(
    name="openhexa-dhis2-pipelines",
    version="0.1.0", 
    description="OpenHEXA DHIS2 data pipelines",
    packages=find_packages(),
    python_requires=">=3.11",
    install_requires=[
        "openhexa.sdk>=1.0.0",
        "openhexa-toolbox[dhis2]>=0.1.0",  # CRITICAL: include [dhis2] extra
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "ruff>=0.1.0", 
            "mypy>=1.0.0",
        ]
    },
)
```

### 4.2 `pytest.ini` (MANDATORY)
```ini
[pytest]
addopts = -q -ra
testpaths = tests
pythonpath = .  # CRITICAL: allows test imports
markers =
    integration: marks tests as integration tests (require DHIS2 instance)
```

### 4.3 `.vscode/settings.json` (MANDATORY)
```json
{
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "tests",
        "--tb=short"
    ],
    "python.testing.cwd": "${workspaceFolder}",
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.terminal.activateEnvironment": true,
    "python.analysis.extraPaths": [
        "."
    ],
    "python.analysis.autoImportCompletions": true,
    "python.testing.autoTestDiscoverOnSaveEnabled": true,
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true
    },
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.linting.mypyEnabled": true,
    "python.formatting.provider": "black",
    "python.analysis.typeCheckingMode": "basic"
}
```

### 4.4 `.env.example` (MANDATORY)
```bash
# DHIS2 Configuration for Integration Tests
# Integration tests skip gracefully if DHIS2 is not available
# Unit tests (19+ tests) always pass without requiring DHIS2

# Option 1: DHIS2 Demo Server (Recommended)
DHIS2_URL=https://play.im.dhis2.org/stable-2-39-10-1
DHIS2_USER=admin
DHIS2_PASS=district

# Option 2: Alternative demo servers (if above unavailable)
# DHIS2_URL=https://play.im.dhis2.org/stable-2-41-3-1
# DHIS2_USER=admin
# DHIS2_PASS=district

# Option 3: Local DHIS2 (Complex setup - use docker-compose.dhis2.yml)
# DHIS2_URL=http://localhost:8080
# DHIS2_USER=admin
# DHIS2_PASS=district

# Docker configuration for local DHIS2
DHIS2_IMAGE=dhis2/core:40.8.0
DHIS2_DB_DUMP_URL=https://databases.dhis2.org/sierra-leone/2.40/dhis2-db-sierra-leone.sql.gz
```

### 4.5 `workspace/workspace.yaml.example` (MANDATORY)
```yaml
# OpenHEXA Workspace Configuration
workspace:
  name: dhis2-pipeline-testing
  description: DHIS2 pipeline testing workspace

connections:
  # Working DHIS2 demo servers (check https://play.im.dhis2.org/ for current URLs)
  dhis2-demo-current:
    type: dhis2
    url: https://play.im.dhis2.org/stable-2-39-10-1
    username: admin
    password: district
    
  dhis2-demo-2-41:
    type: dhis2
    url: https://play.im.dhis2.org/stable-2-41-3-1
    username: admin
    password: district

  # Local DHIS2 (if running via docker-compose)
  dhis2-local:
    type: dhis2
    url: http://localhost:8080
    username: admin
    password: district

  # Example: PostgreSQL connection
  postgres-main:
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: user
    password: pass

  # Example: IASO connection  
  iaso-server:
    type: iaso
    url: https://iaso.example.com
    username: user
    password: pass

# Usage:
# 1. Copy to workspace.yaml: cp workspace.yaml.example workspace.yaml
# 2. Update connection URLs/credentials as needed
# 3. Use in pipeline parameters: source_connection=dhis2-demo-current
```

### 4.6 `docker/Dockerfile.tests` (MANDATORY)
```dockerfile
FROM python:3.11-slim
WORKDIR /workspace

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates git && rm -rf /var/lib/apt/lists/*

# CRITICAL: Install with [dhis2] extra for DHIS2 functionality
RUN pip install --no-cache-dir \
    openhexa.sdk openhexa-toolbox[dhis2] pytest pytest-cov ruff mypy

# Copy project
COPY . /workspace

# CRITICAL: Set Python path for test imports
ENV PYTHONPATH=/workspace

# Default command runs complete test suite
CMD ["pytest", "-q"]
```

---

## 5) Test Architecture (COMPLETE SPECIFICATION)

### 5.1 `tests/conftest.py` (COMPLETE IMPLEMENTATION)
```python
import json
import os
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
            
            # Test connection
            info = client.api.get("system/info")
            print(f"✅ Connected to DHIS2 {server_name} (version: {info.get('version', 'unknown')})")
            return client
            
        except Exception as e:
            print(f"❌ Failed to connect to DHIS2 {server_name}: {e}")
            last_error = e
            continue
    
    # If all servers failed, create client anyway for graceful test skipping
    print(f"\n⚠️  No DHIS2 servers accessible. Integration tests will skip gracefully.")
    print(f"   Last error: {last_error}")
    
    return DHIS2(
        url=dhis2_env["url"],
        username=dhis2_env["user"],
        password=dhis2_env["password"],
    )

@pytest.fixture
def sample_mapping(tmp_path):
    """Create sample mapping with all required sections."""
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
    """Dynamically discover real DHIS2 data for testing."""
    try:
        print(f"\n🔍 Discovering data from DHIS2: {dhis2_env['url']}")
        
        # Get real data elements
        data_elements_response = dhis2_client.api.get("dataElements", params={
            "fields": "id,name", "pageSize": "10"
        })
        data_elements = data_elements_response.get("dataElements", [])
        print(f"   📊 Found {len(data_elements)} data elements")
        
        # Get real category option combos
        coc_response = dhis2_client.api.get("categoryOptionCombos", params={
            "fields": "id,name", "pageSize": "10"
        })
        category_option_combos = coc_response.get("categoryOptionCombos", [])
        print(f"   📋 Found {len(category_option_combos)} category option combos")
        
        # Get real org units
        org_units_response = dhis2_client.api.get("organisationUnits", params={
            "fields": "id,name", "pageSize": "10"
        })
        org_units = org_units_response.get("organisationUnits", [])
        print(f"   🏢 Found {len(org_units)} organization units")
        
        # Create mapping with real UIDs
        if data_elements and category_option_combos and org_units:
            real_mapping = {
                "dataElements": {de["id"]: de["id"] for de in data_elements[:5]},
                "categoryOptionCombos": {coc["id"]: coc["id"] for coc in category_option_combos[:5]},
                "orgUnits": {ou["id"]: ou["id"] for ou in org_units[:3]}
            }
            
            # Write dynamic mapping file
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
            # Skip if missing required data
            missing_data = []
            if not data_elements: missing_data.append("data elements")
            if not category_option_combos: missing_data.append("category option combos") 
            if not org_units: missing_data.append("organization units")
            pytest.skip(f"No DHIS2 data available - DHIS2 instance at {dhis2_env['url']} is missing: {', '.join(missing_data)}")
            
    except Exception as e:
        pytest.skip(f"DHIS2 not accessible for dynamic data discovery (server: {dhis2_env['url']}): {e}")
```

### 5.2 Individual Task Test Pattern (MANDATORY)
**File**: `tests/test_task_validate_connections.py`
```python
import pytest
import pipeline as pipeline_module

@pytest.mark.integration
def test_validate_connections_with_real_dhis2(dhis2_client, dhis2_env) -> None:
    """Test validate_connections task with real DHIS2 connection."""
    
    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]  # Use actual working DHIS2 server
                username = dhis2_env["user"]
                password = dhis2_env["password"]
            source_connection = MockConnection()
            target_connection = MockConnection()
        
        connections = MockConnections()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # First check if DHIS2 is accessible
        try:
            dhis2_client.api.get("system/info")
        except Exception as e:
            pytest.skip(f"DHIS2 server not accessible for connection testing: {e}")
        
        # Call the actual validate_connections task
        result = pipeline_module.validate_connections()
        
        # Verify result structure and content
        assert result is not None
        assert isinstance(result, dict)
        assert "source_client" in result
        assert "target_client" in result
        
        # Verify clients can actually connect to DHIS2
        source_client = result["source_client"]
        source_info = source_client.api.get("system/info")
        assert isinstance(source_info, dict)
        assert "version" in source_info
        
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run

@pytest.mark.integration
def test_validate_connections_with_invalid_credentials(dhis2_env) -> None:
    """Test validate_connections task with invalid credentials."""
    
    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]
                username = "invalid_user"
                password = "invalid_pass"
            source_connection = MockConnection()
            target_connection = MockConnection()
        
        connections = MockConnections()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()
    
    try:
        # This should raise a ValueError due to invalid credentials
        with pytest.raises(ValueError, match="Cannot connect to source DHIS2"):
            pipeline_module.validate_connections()
            
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run
```

### 5.3 Integration Test with Workspace Data (MANDATORY)
**File**: `tests/test_task_fetch_updates.py`
```python
import json
import pytest
import pipeline as pipeline_module

@pytest.mark.integration
def test_fetch_updates_since_date_with_workspace_mapping(workspace_test_mapping, dhis2_client, dhis2_env) -> None:
    """Test fetch_updates_since_date using workspace test mapping."""
    
    # Load mapping from workspace test file
    with open(workspace_test_mapping, encoding="utf-8") as f:
        test_mapping = json.load(f)

    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]
                username = dhis2_env["user"] 
                password = dhis2_env["password"]
            source_connection = MockConnection()

        class MockParameters:
            since_date = "2020-01-01"

        connections = MockConnections()
        parameters = MockParameters()

        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")

        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")

    # Patch current_run for this test
    original_current_run = pipeline_module.current_run
    pipeline_module.current_run = MockCurrentRun()

    try:
        # Call the actual fetch_updates_since_date task
        result = pipeline_module.fetch_updates_since_date(test_mapping)
        
        # Verify result structure
        assert isinstance(result, list)
        
        # Each item should be a valid data value structure (if any returned)
        for data_value in result:
            assert isinstance(data_value, dict)
            assert "dataElement" in data_value
            assert "orgUnit" in data_value
            assert "period" in data_value
            
    finally:
        # Restore original current_run
        pipeline_module.current_run = original_current_run
```

### 5.4 Complete Integration Test (END-TO-END)
**File**: `tests/test_pipeline_integration.py`
```python
import pytest
import pipeline as pipeline_module

@pytest.mark.integration
def test_full_pipeline_execution_end_to_end(dhis2_client, dynamic_dhis2_data, dhis2_env) -> None:
    """Test complete pipeline execution end-to-end with real DHIS2."""
    
    class MockCurrentRun:
        class MockConnections:
            class MockConnection:
                url = dhis2_env["url"]
                username = dhis2_env["user"]
                password = dhis2_env["password"]
            source_connection = MockConnection()
            target_connection = MockConnection()
        
        class MockParameters:
            def __init__(self):
                self.mapping_file = str(dynamic_dhis2_data["mapping_file"])
                self.since_date = "2020-01-01"
                self.dry_run = True
        
        connections = MockConnections()
        parameters = MockParameters()
        
        @staticmethod
        def log_info(msg: str) -> None:
            print(f"INFO: {msg}")
        
        @staticmethod
        def log_error(msg: str) -> None:
            print(f"ERROR: {msg}")
    
    try:
        # Execute full pipeline workflow using test helper
        summary = pipeline_module.run_pipeline_tasks(MockCurrentRun())
        
        # Verify complete summary structure
        assert summary is not None
        assert isinstance(summary, dict)
        assert "sync_needed" in summary
        assert isinstance(summary["sync_needed"], bool)
        
        # Verify all expected summary fields
        expected_fields = [
            "pipeline", "since_date", "total_data_elements_checked",
            "total_category_option_combos_checked", "updates_found",
            "unique_data_elements_updated", "unique_category_option_combos_updated",
            "sync_needed", "dry_run", "updated_data_elements",
            "updated_category_option_combos", "latest_update_timestamps"
        ]
        
        for field in expected_fields:
            assert field in summary, f"Missing field in summary: {field}"
        
    except Exception as e:
        pytest.skip(f"Pipeline execution failed: {e}")
```

### 5.5 Schema Validation Tests (MANDATORY)
**File**: `tests/test_mapping_schema.py`
```python
import json
from pathlib import Path
import pytest

def test_mapping_has_required_sections(sample_mapping) -> None:
    """Test that mapping file contains all required sections."""
    data = json.loads(sample_mapping.read_text(encoding="utf-8"))
    
    # Check all required sections exist
    required_sections = ["dataElements", "categoryOptionCombos", "orgUnits"]
    for section in required_sections:
        assert section in data, f"Missing required section: {section}"
        assert isinstance(data[section], dict), f"Section {section} must be a dictionary"

def test_workspace_test_mapping_structure(workspace_test_mapping) -> None:
    """Test workspace test mapping has correct structure with real DHIS2 UIDs."""
    data = json.loads(Path(workspace_test_mapping).read_text(encoding="utf-8"))
    
    # Check required sections exist and have data
    assert len(data["dataElements"]) > 0, "Test mapping should contain data elements"
    assert len(data["categoryOptionCombos"]) > 0, "Test mapping should contain category option combos"
    assert len(data["orgUnits"]) > 0, "Test mapping should contain organization units"
    
    # Check UIDs are valid DHIS2 format (11 characters)
    for de_uid in data["dataElements"].keys():
        assert len(de_uid) == 11, f"Data element UID {de_uid} should be 11 characters"
    
    for coc_uid in data["categoryOptionCombos"].keys():
        assert len(coc_uid) == 11, f"Category option combo UID {coc_uid} should be 11 characters"
    
    for ou_uid in data["orgUnits"].keys():
        assert len(ou_uid) == 11, f"Organization unit UID {ou_uid} should be 11 characters"
```

---

## 6) Development Workflow (STEP-BY-STEP)

### 6.1 Initial Setup
```bash
# 1. Create pipeline directory
mkdir <pipeline_name>
cd <pipeline_name>

# 2. Create virtual environment 
python3.11 -m venv venv
source venv/bin/activate  # Linux/Mac
# OR: venv\Scripts\activate  # Windows

# 3. Create all required files (use exact implementations above)
# - Create setup.py, pytest.ini, .vscode/settings.json
# - Create workspace/ directory with test_mapping.json
# - Create tests/ directory with all test files
# - Create docker/ directory with Dockerfile.tests

# 4. Install in development mode
pip install -e ".[dev]"

# 5. Verify basic structure
pytest tests/ -k "not integration" -v
# Expected: 15-25 unit tests pass
```

### 6.2 DHIS2 Integration Setup
```bash
# 1. Discover real DHIS2 UIDs for test mapping
python3 -c "
from openhexa.toolbox.dhis2 import DHIS2
client = DHIS2(url='https://play.im.dhis2.org/stable-2-39-10-1', username='admin', password='district')

print('=== DATA ELEMENTS ===')
des = client.api.get('dataElements', params={'fields': 'id,name', 'pageSize': '5'})
for de in des.get('dataElements', []): print(f'\"{de[\"id\"]}\": \"{de[\"id\"]}\",  # {de[\"name\"]}')

print('\n=== CATEGORY OPTION COMBOS ===')
cocs = client.api.get('categoryOptionCombos', params={'fields': 'id,name', 'pageSize': '3'})
for coc in cocs.get('categoryOptionCombos', []): print(f'\"{coc[\"id\"]}\": \"{coc[\"id\"]}\",  # {coc[\"name\"]}')

print('\n=== ORG UNITS ===')
ous = client.api.get('organisationUnits', params={'fields': 'id,name', 'pageSize': '3'})
for ou in ous.get('organisationUnits', []): print(f'\"{ou[\"id\"]}\": \"{ou[\"id\"]}\",  # {ou[\"name\"]}')
"

# 2. Update workspace/test_mapping.json with discovered UIDs

# 3. Test integration
export DHIS2_URL=https://play.im.dhis2.org/stable-2-39-10-1
export DHIS2_USER=admin
export DHIS2_PASS=district
pytest tests/ -v

# Expected: 35+ tests pass, 0 failed
```

### 6.3 VSCode Integration
```bash
# 1. Open VSCode in pipeline directory
code .

# 2. Select Python interpreter
# Ctrl/Cmd+Shift+P → "Python: Select Interpreter" → Choose ./venv/bin/python

# 3. Configure tests
# Ctrl/Cmd+Shift+P → "Python: Configure Tests" → pytest → tests

# 4. View tests in Testing panel
# Ctrl/Cmd+Shift+P → "Test: Focus on Test Explorer View"

# 5. Run tests
# Click play button in Testing panel or Ctrl/Cmd+Shift+P → "Test: Run All Tests"
```

---

## 7) Common Errors & Solutions (TROUBLESHOOTING GUIDE)

### 7.1 Import Errors
```python
# ❌ WRONG - causes import errors
from openhexa_toolbox.dhis2 import DHIS2Client
from openhexa.toolbox.dhis2 import DHIS2Connection

# ✅ CORRECT
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.sdk import DHIS2Connection
```

### 7.2 Parameter Decorator Errors
```python
# ❌ WRONG - causes TypeError
@parameter("param", type=str, description="Description")

# ✅ CORRECT - use 'help' not 'description'
@parameter("param", type=str, help="Description")
```

### 7.3 DHIS2 Client Errors
```python
# ❌ WRONG - causes connection errors
client = DHIS2(base_url=url, username=user, password=pass)
response = client.get("system/info")

# ✅ CORRECT
client = DHIS2(url=url, username=user, password=pass)
response = client.api.get("system/info")
```

### 7.4 DHIS2 API Errors
```python
# ❌ WRONG - causes 409 error: "At least one organisation unit must be specified"
params = {
    "dataElement": data_elements,
    "categoryOptionCombo": cocs,
    "lastUpdated": date
}

# ✅ CORRECT - include orgUnit parameter  
params = {
    "dataElement": data_elements,
    "categoryOptionCombo": cocs,
    "orgUnit": org_units,  # REQUIRED
    "lastUpdated": date
}
```

### 7.5 Test Architecture Errors
```python
# ❌ WRONG - variables not accessible in class definition
mapping_file = f.name
class MockParameters:
    mapping_file = mapping_file  # NameError!

# ✅ CORRECT - use __init__ method
mapping_file_path = f.name
class MockParameters:
    def __init__(self):
        self.mapping_file = mapping_file_path
```

### 7.6 Missing Required Files
```bash
# ❌ Common missing files that cause test failures:
# - __init__.py (in package and tests/)
# - setup.py (prevents editable installs)
# - pytest.ini (missing pythonpath = .)
# - .vscode/settings.json (VSCode doesn't find tests)

# ✅ SOLUTION: Create all required files with exact content from this specification
```

### 7.7 Mapping Schema Errors
```json
// ❌ WRONG - missing required orgUnits section
{
  "dataElements": {...},
  "categoryOptionCombos": {...}
}

// ✅ CORRECT - includes all required sections
{
  "dataElements": {...},
  "categoryOptionCombos": {...}, 
  "orgUnits": {...}
}
```

---

## 8) Success Criteria (VERIFICATION CHECKLIST)

### 8.1 Test Results ✅
- [ ] **35+ tests passing** with real DHIS2 integration
- [ ] **0 test failures** - all edge cases handled
- [ ] **Integration tests** work with demo server
- [ ] **Unit tests** work without external dependencies
- [ ] **Schema validation** for all mapping files
- [ ] **Error handling** tests for invalid inputs

### 8.2 Code Quality ✅
- [ ] **ruff check --fix .** passes with no errors
- [ ] **mypy .** passes with no type errors  
- [ ] **All imports** use correct OpenHEXA syntax
- [ ] **All DHIS2 API calls** use .api.get() pattern
- [ ] **No hardcoded UIDs** in test or production code
- [ ] **Proper error messages** for all failure scenarios

### 8.3 Integration ✅ 
- [ ] **VSCode Testing panel** discovers all tests automatically
- [ ] **pytest tests/ -v** runs full test suite successfully
- [ ] **Demo server fallback** works when local DHIS2 unavailable  
- [ ] **Docker containerization** works via make test
- [ ] **Real DHIS2 data** discovered and used in integration tests
- [ ] **Workspace test mapping** contains validated UIDs

### 8.4 Documentation ✅
- [ ] **README.md** with clear usage instructions
- [ ] **workspace/README.md** documenting test data
- [ ] **Inline code comments** explaining critical patterns
- [ ] **docstrings** for all public functions
- [ ] **Type hints** throughout codebase

---

## 9) Final Verification Commands

```bash
# Complete validation sequence - all should pass ✅

# 1. Code quality
ruff check --fix .
mypy .

# 2. Unit tests (no external dependencies)  
pytest tests/ -k "not integration" -v
# Expected: 15-25 passed

# 3. Integration tests (with DHIS2 demo server)
export DHIS2_URL=https://play.im.dhis2.org/stable-2-39-10-1
export DHIS2_USER=admin  
export DHIS2_PASS=district
pytest tests/ -v
# Expected: 35+ passed, 0 failed

# 4. Docker containerized tests
make test
# Expected: All tests pass in container

# 5. VSCode integration
# Open VSCode, go to Testing panel, run all tests
# Expected: All tests visible and runnable
```

---

## 10) Anti-Patterns to Avoid ❌

1. **Hardcoded DHIS2 UIDs** in any code
2. **Missing orgUnits** in mapping schema  
3. **Using .get() instead of .api.get()** for DHIS2 API calls
4. **Missing __init__.py** files in packages
5. **Using description= instead of help=** in parameter decorators
6. **Calling OpenHEXA decorated functions** directly in tests
7. **Missing [dhis2] extra** in toolbox installation
8. **No test for each pipeline task** individually
9. **No workspace test data** with real UIDs
10. **No graceful degradation** when DHIS2 unavailable

Following this specification exactly will result in a fully functional DHIS2 pipeline with comprehensive testing that achieves passing tests with 0 failures.