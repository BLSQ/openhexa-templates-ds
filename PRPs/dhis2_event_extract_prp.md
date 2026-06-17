# Feature PRP: `extract_dhis2_events`

## 🎯 Goal
Build an OpenHEXA pipeline that **extracts Events (program stage instances) from a DHIS2 instance** and outputs them in a structured dataset for downstream use (analytics, reporting, warehousing).  

This pipeline **does not push events anywhere**; it is a read-only extractor.

---
## Example pipeline for code reference
- see dhis2_metadata_extract/pipeline.py or dhis2_extract_data_elements/pipeline.py as reference for code style, pattern and call to openhexa toolbox. Please carefully follow that examples as implicit guidelines, best practices for openhexa pipelines interactinf with DHIS2

## 🔎 Why / Use case
- Monitoring & evaluation teams need event-level data for reporting.  
- Data engineers need clean extracts for data warehouses or dashboards.  
- Example: run nightly to pull all **completed events** for a program stage, save to CSV/Parquet in S3.

---

## 🧩 Parameters
```python
@parameter("source_connection", type=DHIS2Connection, required=True, help="Source DHIS2 instance")
@parameter("program", type=str, required=True, help="Program UID from which to extract events")
@parameter("program_stage", type=str, required=False, help="Optional Program Stage UID (restrict to one stage)")
@parameter("org_units", type=list, required=False, help="List of orgUnit UIDs to filter on")
@parameter("status", type=str, default="COMPLETED", help="Event status filter: ACTIVE, COMPLETED, ALL")
@parameter("since_date", type=str, required=False, help="If provided, only extract events updated since this date (YYYY-MM-DD)")
@parameter("output_format", type=str, default="parquet", help="Output format: csv, jsonl, parquet")
@parameter("destination", type=FileConnection, required=True, help="Where to write extracted files (local/S3)")
```

---

## 🏗️ Blueprint / Tasks

1. **validate_connection**  
   Confirm source DHIS2 connection is valid.

2. **build_query**  
   Construct `/api/events` request with filters: program, programStage, orgUnits, status, since_date.

3. **fetch_events**  
   Paginate through events, stream results, handle retries.

4. **transform_events**  
   Flatten DHIS2 event payloads into tabular rows with:
   - event, program, programStage, orgUnit, eventDate, status, completedDate
   - dataValues (one column per DE UID)

5. **write_output**  
   Save rows in chosen format (CSV/JSONL/Parquet) to the destination.

6. **generate_summary**  
   Produce a JSON/CSV report with:
   - Total events fetched
   - Filters applied
   - Output file path
   - Run timestamp

---

## 🧾 Example Output Schema
- `event` (string)  
- `program` (string)  
- `programStage` (string)  
- `orgUnit` (string)  
- `eventDate` (date)  
- `completedDate` (date)  
- `status` (string)  
- For each Data Element in stage: `<deUid>_value`

---

## 🔎 Validation & Gotchas
- `program` UID must exist in source instance.  
- If `program_stage` not provided, extract all stages of the program.  
- Empty result sets are valid; output file still produced (0 rows).  
- Large extracts must stream to avoid memory errors.  
- `since_date` must be valid ISO date.  

---

## 🧪 Testing

### Unit Tests
- Invalid program UID → raises error.  
- Invalid `since_date` → raises error.  
- Empty fetch → outputs empty file + summary with `events=0`.  
- Valid fetch → outputs correct row count and schema.

### Integration Tests (local DHIS2 Sierra Leone)
- Extract all completed events for program X → expect >0 rows.  
- Extract with `since_date` in the future → expect 0 rows.  
- Extract with valid orgUnits list → only rows from those OUs.

### Harness
- Use same Docker/DHIS2 harness as in base PRP.  
- Run with `make test`.

---

## 📄 Deliverables
- `pipelines/extract_dhis2_events/pipeline.py`  
- `pipelines/extract_dhis2_events/README.md`  
- `pipelines/extract_dhis2_events/tests/` with unit + integration tests  
- Docker harness files reused from template.

---

## ✅ Success Checklist
- [ ] Uses OpenHEXA decorators + helpers only.  
- [ ] Parameters exactly as defined.  
- [ ] Handles valid/invalid program UIDs and dates.  
- [ ] Extract produces schema-stable outputs.  
- [ ] Summary report generated each run.  
- [ ] Unit + integration tests pass (`make test`).  
- [ ] README explains purpose, parameters, sample usage.
