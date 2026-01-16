# DHIS2 Metadata Extract

This pipeline extracts metadata from a DHIS2 instance, saving it as a CSV file for further analysis. It is able to extract metadata for: Organisation Units, Organisation Unit Groups, Datasets, Data Elements, Data Element Groups, and Category Option Combos.

## Example run

![Example run](docs/images/parameters.png)

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| Source DHIS2 instance | DHIS2 Connection | Yes | - | The DHIS2 instance to extract data from |
| Organisation Units | Bool | Yes | False | Whether to extract Organisation Units metadata |
| Organisation Unit Groups | Bool | Yes | False | Whether to extract Organisation Unit Groups metadata |
| Datasets | Bool | Yes | False | Whether to extract Dataset metadata |
| Data Elements | Bool | Yes | False | Whether to extractData Elements metadata |
| Data Element Groups | Bool | Yes | False | Whether to extract Data Element Groups metadata |
| Category Option Combos | Bool | Yes | False | Whether to extract Category Option Combos metadata |
| Output path | String | No | Auto-generated | Custom output file path in workspace |

## Output

### 1. File Output (csv)
The pipeline generates Parquets filex containing the extracted metadata. 

- If the parameter `Output file` is not provided, the files are saved to:
```
<workspace>/pipelines/dhis2_metadata_extract/<timestamp>
```

- If the parameter `Output file` is provided, the files are saved to the specified path. 

Each of the output files will be saved with a filename following this pattern:
**(metadata name)_yyyy_mm_dd_hhmm.csv**

### Output Data Structure

The pipeline extracts metadata from the DHIS2 instance and saves it in CSV format. The metadata that is extracted depends on the user's selection of parameters.

**Example of Data element groups:**

![Data format](docs/images/dataelement_groups.png)

## Formatting and validation
After the metadata is extracted from the DHIS2 instance, it is formatted and validated to ensure consistency and accuracy.

The formatting includes dropping unnecessary columns and transforming the data-types into more usable formats.

The validation includes: 
- The DataFrame is not empty
- All required columns are present
- The column has the correct kind of data
- Certain columns have no null values
If the validation fails, the pipeline raises an error and stops execution.


## Flow

```mermaid
flowchart TD
    A[Start: Connect to DHIS2] --> B1|if extract organisation units|[Extract Organisation Units]
    A --> B3|if extract organisation unit groups|[Extract Organisation Unit Groups]
    A --> B3|if extract dataset|[Extract Datasets]
    A --> B4|if extract data elements|[Extract Data Elements]
    A --> B5|if extract data element groups|[Extract Data Element Groups]
    A --> B6|if extract category option combos|[Extract Category Option Combos]    
    B1 --> C1[Format Organisation Units]
    B2 --> C2[Format Organisation Unit Groups]
    B3 --> C3[Format Datasets]
    B4 --> C4[Format Data Elements]
    B5 --> C5[Format Data Element Groups]
    B6 --> C6[Format Category Option Combos]
    C1 --> D1[Validate Organisation Units]
    C2 --> D2[Validate Organisation Unit Groups]
    C3 --> D3[Validate Datasets]
    C4 --> D4[Validate Data Elements]
    C5 --> D5[Validate Data Element Groups]
    C6 --> D6[Validate Category Option Combos]
    D1 --> E1[Save Organisation Units as CSV]
    D2 --> E2[Save Organisation Unit Groups as CSV]
    D3 --> E3[Save Datasets as CSV]
    D4 --> E4[Save Data Elements as CSV]
    D5 --> E5[Save Data Element Groups as CSV]
    D6 --> E6[Save Category Option Combos as CSV]    
    E1 --> F[End of Pipeline]
    E2 --> F
    E3 --> F
    E4 --> F
    E5 --> F
    E6 --> F
```