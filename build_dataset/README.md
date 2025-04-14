# Build Dataset from CSV Folders

This OpenHEXA pipeline allows you to automatically build a new version of a dataset by collecting and combining `.csv` files from one or more folders.

Each folder should follow a structure like this:

```
📁 your-dataset-folder/
├── element_1/
│   ├── 2023.csv
│   └── 2024.csv
├── element_2/
│   └── 2024.csv
...
```

## What the Pipeline Does

For each folder you provide:
- It reads all the `.csv` files inside the subfolders (e.g. `element_1`, `element_2`).
- It combines all these `.csv` files into one single table per folder.
- It automatically detects and formats the `period` column (e.g. `202401`, `2024-03-01`, `2024Q1`, etc.).
- It creates a **new version** of the selected OpenHEXA dataset.
- It uploads the combined data as a single `.csv` file to the dataset version.

## Input Parameters

| Parameter Name     | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| **Dataset**         | The OpenHEXA dataset you want to update.                                   |
| **Dataset Paths**   | One or more folders (in your workspace files) containing subfolders of `.csv` files. |

> 💡 Each subfolder (e.g. `element_1`) is expected to represent a category or data element and should contain one or more `.csv` files.

## Example

If you select a dataset folder named `Malaria/`, with this structure:

```
Malaria/
├── confirmed_cases/
│   ├── 2023.csv
│   └── 2024.csv
├── supsected_cases/
│   └── 2024.csv
```

The pipeline will:
- Read and combine the `confirmed_cases/*.csv` files.
- Read and combine the `suspected_cases/*.csv` files.
- Upload both to a new version of your selected OpenHEXA dataset.

## Notes

- The `period` column will be interpreted even if written in formats like `2024Q1`, `202401`, `2024-01-01`, or `2024`.
- If `.csv` files have inconsistent columns, the pipeline will log a warning.
- Any column named like `Unnamed: 0` (auto-created by Excel or pandas) is ignored.

## Output

- A new version of the selected dataset is created.
- A `.csv` file is uploaded for each dataset path, containing the cleaned and merged data.

---
