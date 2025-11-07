# 📄 OpenHEXA Pipeline Product Requirement Template

## 🎯 Goal / Description

**Mandatory – General guideline:**  
- Describe in plain language what the pipeline does and why it exists.  
- Identify the main user (analyst, engineer, data manager).  
- State the business value.  

**Example (DHIS2 Event Extract):**  
This pipeline extracts **Events (program stage instances)** from a DHIS2 instance and saves them into a structured dataset (CSV/Parquet/JSONL). It enables analysts and M&E teams to build reports and dashboards without directly querying DHIS2.  

---

## 🔎 Scope

**Mandatory – General guideline:**  
- Explicitly state **what the pipeline does**.  
- Explicitly state **what it does not do**.  
- Mention how it differs from related pipelines.  

**Example:**  
- **Does:** Extract completed or active DHIS2 events, flatten them, and save as files in S3.  
- **Does not:** Push data back into DHIS2, modify source data, or handle TEI/attribute exports.  
- **Differs from:** The `sync_dhis2_to_dhis2` pipeline, which only checks freshness of data elements.  

---
## Documentation
**Optional - good if you use gen AI tool**
List here all documentation that you want to be used following that format:
 - url
 - usecase/goal
Please place here OpenHexa documentation, DHIS2 api documentation, openhexa toolbox specific libraries you want to use (DHIS2, IASO, Kobotoolbox, etc.) or any other available and used library or API doc.
## 🌐 Endpoints & Actions

**Mandatory – General guideline:**  
- For **integration pipelines** (most of our use cases), list all endpoints the pipeline will call.  
- Specify which **action** (GET/PUT/POST/DELETE) is taken and on which attributes.  
- This ensures clarity if the requirement is later executed by an AI agent.  

**Example:**  
- Endpoint: `GET /api/events`  
- Attributes used: `program`, `programStage`, `orgUnit`, `status`, `lastUpdated`  
- Action: **GET** (read-only)  

---

## 🧩 Parameters

**Mandatory – General guideline:**  
- List all configurable parameters with type, required/default values, and help text.  
- Use OpenHEXA `@parameter` decorators.  

**Example:**  
```python
@parameter("source_connection", type=DHIS2Connection, required=True, help="Source DHIS2 instance")
@parameter("program", type=str, required=True, help="Program UID to extract events from")
@parameter("program_stage", type=str, required=False, help="Optional Program Stage UID")
@parameter("status", type=str, default="COMPLETED", help="Event status filter")
@parameter("since_date", type=str, required=False, help="Only extract events updated since this date (YYYY-MM-DD)")
@parameter("output_format", type=str, default="parquet", help="Output format: csv, jsonl, parquet")
@parameter("destination", type=FileConnection, required=True, help="Where to write extracted files")
```

---

## 📥 Inputs

**Mandatory – General guideline:**  
- Define clearly which inputs are needed: files, database tables, APIs.  
- Explicitly specify required **columns or attributes** for efficient queries and easier error handling.  

**Example:**  
- **Connection:** DHIS2 API connection (Basic Auth).  
- **API endpoint:** `/api/events`  
- **Attributes read:** `event`, `program`, `programStage`, `orgUnit`, `eventDate`, `dataValues`.  
- **Required columns if reading from DB:** `event_id`, `program_id`, `event_date`.  

---

## 📤 Outputs

**Mandatory – General guideline:**  
- Explicitly define which OpenHEXA output types are used:  
  - **Database table** (workspace DB or external DB).  
  - **File on workspace** (path + format: .parquet/.csv/.json).  
  - **OpenHEXA dataset** (dataset ID, file name, version strategy: explicit name or timestamp).  
- Define schema: columns/keys and their **data types**.  

**Example:**  
- Output type: **OpenHEXA dataset**  
- Dataset ID: `dhis2_events`  
- File: `events.parquet`  
- Version: timestamp-based (automatic).  
- Schema:  
  - `event` (string), `program` (string), `eventDate` (date), `status` (string), plus `<deUid>_value` (typed).  

---

## 🏗️ Pipeline Skeleton (Main Steps)

**Mandatory – General guideline:**  
- List the key functions/steps to implement.  

**Example:**  
1. `validate_connection` – confirm DHIS2 connection.  
2. `build_query` – prepare API call with filters.  
3. `fetch_events` – paginate API, retry on errors.  
4. `transform_events` – flatten event payloads.  
5. `write_output` – save to dataset/file/database.  
6. `generate_summary` – output run report.  

---

## ⚠️ Exceptions & Error Handling

**Mandatory – General guideline:**  
- List known exceptions (invalid params, API errors, missing columns).  
- State how each should be handled (fail/skip/retry).  

**Example:**  
- Invalid `since_date` → fail with clear error.  
- API 500 error → retry 5 times with backoff.  
- Empty input → produce empty file and summary.  

---

## 🔀 Edge Cases Handling

**Mandatory – General guideline:**  
- Explicitly list **foreseeable edge cases** (not just errors) and define handling rules.  

**Example:**  
- Extract returns **empty DataFrame** → still save empty output + summary.  
- Events with **mixed periodicity (monthly/weekly)** → normalize to weekly, log warning.  
- Some provided orgUnits **do not exist** → filter them out, log warning (no hard failure).  

---

## 🧪 Examples

**Mandatory – General guideline:**  
- Provide examples of input parameters and resulting outputs.  

**Example 1: Normal run**  
```json
{ "program": "a1b2c3", "status": "COMPLETED", "since_date": "2024-01-01" }
```
Output: Parquet file with 1,234 events.  

**Example 2: Empty run**  
`since_date = "2050-01-01"` → Empty file + summary `{ "rows_written": 0 }`.  

---

## 🛡️ Quality Checks

**Mandatory – General guideline:**  
- Define what checks to run on outputs: schema, value types, uniqueness, mandatory columns.  

**Example:**  
- Columns: `event` (string, unique), `program` (string), `eventDate` (date).  
- Reject if `event` column has nulls.  
- Ensure all numeric DEs are stored as integers/floats.  

---

## 🛠️ Code Guidelines

**Mandatory – General guideline:**  
- Use OpenHEXA toolbox where possible; if not, justify.  
- Follow PEP8 + ruff linting.  
- Use docstrings, type hints, modular functions.  

---

## 📄 README Requirement

**Mandatory – General guideline:**  
- Each pipeline must include a `README.md` with:  
  - Goal  
  - Parameters  
  - Inputs/Outputs  
  - Examples  
  - How to run locally and in production  

**Example:**  
The README for `extract_dhis2_events` includes:  
- Short description of the pipeline’s goal.  
- Parameters table.  
- Example config and expected output.  
- Instructions to run locally (`make test`) and in production.  

---

## ✅ Success Checklist

**Mandatory – General guideline:**  
- Final checklist before implementation is accepted.  

**Example:**  
- [ ] Goal & scope clear.  
- [ ] Endpoints & actions listed.  
- [ ] Parameters documented.  
- [ ] Inputs & outputs defined.  
- [ ] Schema + quality checks included.  
- [ ] Edge cases listed.  
- [ ] README provided.  
- [ ] Tests planned.  

---

## 🧪 Tests (Optional vs Mandatory)

- **Optional:** If PR is meant for manual implementation, tests may be sketched as examples (unit/integration).  
- **Mandatory:** If PR is designed for execution by an AI agent, tests (unit + integration) must be fully described.  
