# Feature PRP: `sync_dhis2_to_dhis2`

## 🎯 Goal
Build an OpenHEXA pipeline that **verifies if mapped Data Elements in a source DHIS2 instance have been updated since a given date**.  
If updates are detected, the pipeline outputs a summary so users know whether to trigger a synchronization into the target DHIS2.

This pipeline **does not perform the update itself**; it checks for freshness and produces a decision report.

---

## 🔎 Why / Use case
- Data managers need to avoid redundant synchronizations across DHIS2 instances.  
- By checking updates since a reference date, the team can run heavy sync jobs only when needed.  
- Example: run daily/weekly, and only sync if source DEs changed since the last sync date.

---

## 🧩 Parameters
```python
@parameter("source_connection", type=DHIS2Connection, required=True, help="Source DHIS2 instance")
@parameter("target_connection", type=DHIS2Connection, required=True, help="Target DHIS2 instance (to be compared, not modified)")
@parameter("mapping_file", type=str, required=True, help=" path to JSON with dataElement and categoryOptionCombo mappings")
@parameter("since_date", type=str, required=True, help="Date in YYYY-MM-DD format. Check if source DE values updated since this date.")
@parameter("dry_run", type=bool, default=True, help="Always true here; no writing occurs, but required for standardization.")
```

---

## 🏗️ Blueprint / Tasks

1. **validate_connections**  
   Ensure both DHIS2 connections are live.

2. **load_and_validate_mappings**  
   Parse mapping JSON. Must contain `dataElements` and `categoryOptionCombos`.

3. **fetch_updates_since_date**  
   Query source DHIS2 for data values of mapped DE/COCs where `lastUpdated >= since_date`.  
   Endpoint reference: `/dataValueSets` with `lastUpdated` filter.

4. **compare_with_target (optional)**  
   Fetch same DE/COCs from target for context. (No updates written, just info.)

5. **generate_summary**  
   Produce a JSON/CSV summary with:
   - Number of DEs checked
   - Number of DEs updated since `since_date`
   - List of DE/COCs updated with timestamps
   - Recommendation: `"sync_needed": true/false`

---

## 🧾 Mapping Schema (same as base)
```json
{
  "dataElements": {"SRC_DE": "TGT_DE"},
  "categoryOptionCombos": {"SRC_COC": "TGT_COC"}
}
```

---

## 🔎 Validation & Gotchas
- `since_date` must be valid ISO date (YYYY-MM-DD).  
- Reject mapping files without both sections.  
- Warn if mapped DE/COCs don’t exist in source.  
- Handle empty update set gracefully (summary should say `"sync_needed": false`).  
- Do not modify target DHIS2. This is a **read-only decision helper pipeline**.

---

## 🧪 Testing

### Unit Tests
- Invalid mapping file rejected.  
- Invalid `since_date` raises error.  
- DE query with no updates returns `sync_needed: false`.  
- DE query with updates returns `sync_needed: true` and includes DE IDs.

### Integration Tests (local DHIS2 Sierra Leone via Docker)
- Seed Sierra Leone DB.  
- Run with a `since_date` far in the past → expect updates found.  
- Run with a future `since_date` → expect `sync_needed: false`.  
- Include a test mapping.json with known DE/COCs present in Sierra Leone DB.

### Required Harness
- Use the default Docker setup (`docker-compose.dhis2.yml`, `Dockerfile.tests`, `Makefile`) as enforced by `prp_base.md`.  
- Tests executed via `make test`.

---

## 📄 Deliverables
- `pipelines/sync_dhis2_to_dhis2/pipeline.py`  
- `pipelines/sync_dhis2_to_dhis2/README.md`  
- `pipelines/sync_dhis2_to_dhis2/tests/` with unit + integration tests  
- Docker harness files already included (from base template).

---

## ✅ Success Checklist
- [ ] Only OpenHEXA decorators + toolbox helpers used.  
- [ ] Parameters exactly as defined.  
- [ ] Handles valid/invalid mappings and dates.  
- [ ] Summary output with `sync_needed`.  
- [ ] Unit + integration tests pass locally (`make test`).  
- [ ] No TODOs/placeholders.  
- [ ] README explains purpose, parameters, and sample usage.

