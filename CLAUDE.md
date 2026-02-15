# CLAUDE.md

## Project Overview

BOE-Processing is a Databricks notebook-based system for processing Spanish-language financial documents (invoices and BOE/Basis of Expense documents) on behalf of the U.S. Department of State. It classifies expenses against Foreign Affairs Manual (FAM) guidelines for residential lease operating expenses incurred by diplomatic personnel stationed abroad.

## Repository Structure

```
BOE-Processing/
├── invoice_processing.txt        # Full invoice processing notebook (863 lines)
├── boe_invoice_processing.txt    # Streamlined BOE document processing notebook (334 lines)
├── agentsmd.file                 # Funhouse SDK reference documentation (large)
├── .gitignore                    # Standard Python gitignore
└── CLAUDE.md                     # This file
```

Files use `.txt` extension for Databricks notebook version control compatibility. They are **not** plain Python scripts — they follow the Databricks notebook source format with `# COMMAND ----------` cell separators and `# MAGIC %md` markdown cells.

## Tech Stack

- **Runtime**: Databricks (Funhouse fork) with PySpark
- **Language**: Python 3
- **Key libraries**: pandas, pyspark, `funhouse.utils`
- **External services** (via Funhouse SDK, initialized by `%run /setup_python`):
  - `fh_doc` — Azure Document Intelligence (OCR, invoice field extraction)
  - `fh_translator` — Azure Translator (Spanish to English)
  - `fh_prompter` — LLM API (classification, structured extraction)
  - `fh_vision` — Azure Vision API (OCR fallback)
  - `fh_sp_client` / `fh_sp_file` / `fh_sp_list` — SharePoint client, file manager, list manager

## Notebook Descriptions

### `invoice_processing.txt`

General-purpose Spanish invoice processor. Sections:

| Section | Purpose |
|---------|---------|
| 0 | Initialize Funhouse environment (`%run /setup_python`) |
| 1 | Environment notes and SharePoint connection guidance |
| 2 | Import libraries |
| 3 | Service initialization and storage configuration |
| 4 | FAM guidelines constant (`FAM_GUIDELINES`) |
| 5 | Translation functions (`translate_text`, `translate_invoice_fields`, `batch_translate_dataframe`) |
| 6 | Text extraction (`download_file_from_sharepoint`, `get_invoice_files_from_sharepoint`, `update_invoice_tracking_list`, `extract_text_from_pdf`) |
| 7 | AI classification (`classify_and_extract_invoice_info`) |
| 8 | Batch processing (`process_invoice_batch`) |
| 9 | Main execution (DBFS or SharePoint source) |
| 10 | Summary statistics |
| 11 | Export results (Excel, CSV, Delta table) |
| 12 | Manual review report for borderline scores |

Key configuration variables (Section 3):
- `INPUT_STORAGE_PATH` — DBFS path for local invoice files
- `USE_SHAREPOINT` — toggle between DBFS and SharePoint sources
- `SHAREPOINT_LIBRARY_NAME`, `SHAREPOINT_FOLDER_PATH` — SharePoint location

### `boe_invoice_processing.txt`

Streamlined BOE document processor with a two-stage triage approach. Key differences from invoice_processing:

- **Text triage before Document Intelligence** — reduces API cost by skipping DI when text alone suffices
- **Multi-model DI fallbacks** — tries `prebuilt-invoice`, `prebuilt-read`, `prebuilt-layout` in sequence
- **Cascading text extraction** — `extract_best_available_text()` tries AI-aware extraction, Vision OCR, then plain PDF text
- **SharePoint-only I/O** — reads from and writes results back to SharePoint (no DBFS)
- Supports broader file types: PDF, JPG, JPEG, PNG, TIF, TIFF

Key configuration variables (top of file):
- `SHAREPOINT_FOLDER_PATH` — SharePoint source folder
- `ALLOWED_EXTENSIONS` — file types to process
- `SKIP_EXTENSIONS` — file types to skip (e.g., `.csv`)
- `MAX_TEXT_CHARS` — LLM input truncation limit (7000)

## Core Domain Concepts

### FAM Allowability Scoring

Both notebooks classify expenses on a 0–100 scale based on U.S. Department of State FAM guidelines:

| Score Range | Classification | Examples |
|-------------|---------------|----------|
| 80–100 | Allowable | Utilities, building maintenance, HOA fees, security |
| 50–79 | Needs Review | Emergency repairs, parking, ambiguous mixed invoices |
| 0–49 | Not Allowable | Furniture, appliances, personal services, entertainment |

The FAM guidelines are embedded as string constants (`FAM_GUIDELINES` / `FAM_BOE_GUIDELINES`) and injected into LLM system prompts.

### Bilingual Processing

All source documents are in Spanish. The system:
1. Extracts text in Spanish
2. Classifies and reasons in the LLM prompt
3. Translates key output fields to English
4. Preserves both Spanish originals (`*_spanish` columns) and English translations

### Document Processing Pipeline

**invoice_processing.txt**:
```
PDF → Document Intelligence (prebuilt-invoice) → Text + Metadata
    → LLM Classification (with FAM prompt) → Translation → Output
```

**boe_invoice_processing.txt**:
```
File → extract_best_available_text() → LLM Triage
    → [if needed] Document Intelligence (multi-model) → extract_invoice_fields()
    → LLM Classification (with FAM prompt) → Translation → Output
```

## Code Conventions

### Notebook Format
- Databricks notebook source format: cells separated by `# COMMAND ----------`
- Markdown cells prefixed with `# MAGIC %md`
- Magic commands prefixed with `# MAGIC`
- Each notebook starts with `%run /setup_python` to initialize Funhouse services

### Python Style
- Type hints on all function signatures (using `typing` module)
- Docstrings with Args/Returns sections on public functions
- Configuration constants at the top of the notebook, UPPER_SNAKE_CASE
- Error handling via try/except with fallback values (never crashes the batch)
- `print()` statements for progress logging within Databricks

### Funhouse SDK Patterns
- Services are initialized globally via `%run /setup_python` and assigned to module-level variables
- SharePoint objects (`fh_sp_client`, `fh_sp_file`, `fh_sp_list`) are available globally after setup
- LLM calls use `prompter.chat()` with system/user message pairs, `temperature=0.0` or `0.1`, and `response_format="json"`
- Document Intelligence calls use `doc_client.analyze_document(document_bytes=..., model_id=...)`
- Translation calls use `translator.translate(text=..., source_language="es", target_language="en")`
- SharePoint file operations use `sp_file.download_file(path, return_bytes=True)` and `sp_file.upload_bytes(data, path, overwrite=True)`

### LLM Prompt Patterns
- System prompts embed the full FAM guidelines constant
- User prompts include extracted text (truncated to prevent token overflow) and any structured DI fields
- Responses are always requested as JSON with a defined schema
- Classification outputs include: vendor_name, service details, amount, allowability_score, assessment, and reasoning

## Development Notes

### No Build System or Tests
This project has no package manager, build tool, linting configuration, or test framework. The notebooks are executed directly within the Databricks runtime.

### Editing Notebooks
When modifying `.txt` notebook files:
- Preserve the `# COMMAND ----------` cell separators exactly
- Preserve `# MAGIC` prefixes on markdown and magic command lines
- Do not add `if __name__ == "__main__"` blocks — these run as notebooks, not scripts
- Do not import Funhouse services — they are provided by `%run /setup_python`

### Funhouse SDK Reference
The `agentsmd.file` contains extensive Funhouse SDK documentation (270k+ lines). Consult it for details on service APIs, method signatures, and configuration options. Key classes documented there:
- `SharePointClient`, `SharePointFileManager`, `SharePointListManager`
- Document Intelligence, Vision, Translator, Prompter, Language services

### Adding New Notebooks
Follow the existing pattern:
1. Start with `%run /setup_python`
2. Define configuration constants
3. Embed relevant FAM guidelines
4. Build system/user prompt pairs for LLM classification
5. Implement extraction with fallbacks
6. Output to SharePoint and/or DBFS
7. Use `.txt` extension for version control
