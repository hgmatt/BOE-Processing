# Databricks notebook source
# MAGIC %md
# MAGIC # BOE Spanish Document + Invoice Processing (Funhouse + SharePoint)
# MAGIC
# MAGIC This script processes Spanish BOE documents from SharePoint using native Funhouse objects
# MAGIC initialized by `%run /setup_python`.

# COMMAND ----------

# MAGIC %run /setup_python

# COMMAND ----------

import json
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from funhouse.utils import extract_text

# COMMAND ----------

# Configuration (adjust as needed)
SHAREPOINT_FOLDER_PATH = "/Shared Documents/BOE"
ALLOWED_EXTENSIONS = (".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff")
MAX_TEXT_CHARS = 7000

FAM_BOE_GUIDELINES = """
Residential lease BOE allowability categories (U.S. Department of State FAM aligned):

ALLOWABLE (score 80-100):
- Building/common utilities tied to leased residence (water, gas, electric, sewage, trash)
- Mandatory building maintenance and common area fees
- Security, elevator, required building systems maintenance
- Mandatory HOA/condo/common charges required by lease

NEEDS REVIEW (score 50-79):
- Charges that might be lease-related but need lease-term confirmation
- Ambiguous repairs or mixed invoices with both allowable and personal items
- Fees that may be allowable only under specific post policy/lease language

NOT ALLOWABLE (score 0-49):
- Furniture, appliances, personal household services
- Personal internet/cable upgrades, entertainment, non-mandatory amenities
- Personal improvements, cosmetic upgrades, refundable deposits, penalties
- Vehicle or non-residential expenses
""".strip()

# COMMAND ----------

doc_client = fh_doc
translator = fh_translator
prompter = fh_prompter
sp_file = fh_sp_file

# COMMAND ----------


def build_classifier_system_prompt() -> str:
    return f"""You are a compliance analyst for BOE reimbursements on residential operating leases.
All source documents are in Spanish.

Task:
1) Extract invoice/expense details.
2) Output descriptions in ENGLISH.
3) Classify each expense using: allowable, needs review, not allowable.
4) Base classification on this policy:\n\n{FAM_BOE_GUIDELINES}

Return JSON only in this schema:
{{
  "vendor_name": "",
  "service_description": "",
  "service_type": "",
  "amount": "",
  "currency": "",
  "invoice_id": "",
  "invoice_date": "",
  "allowability_score": 0,
  "assessment": "allowable|needs review|not allowable",
  "assessment_reason": ""
}}
"""


def build_classifier_user_prompt(file_name: str, extracted_text: str, invoice_fields: Dict[str, Any]) -> str:
    return f"""Analyze this Spanish BOE document.

File name: {file_name}
Document text:
{extracted_text[:MAX_TEXT_CHARS]}

Document Intelligence invoice fields (if present):
{json.dumps(invoice_fields, ensure_ascii=False)}

Use the text and fields together. Keep output in English."""


def extract_invoice_fields(di_result: Dict[str, Any]) -> Dict[str, Any]:
    if not di_result or "documents" not in di_result or not di_result["documents"]:
        return {}

    fields = di_result["documents"][0].get("fields", {})
    normalized: Dict[str, Any] = {}

    for key in ["VendorName", "InvoiceId", "InvoiceDate", "InvoiceTotal"]:
        value = fields.get(key)
        if not value:
            continue
        if key == "InvoiceTotal" and value.get("valueCurrency"):
            normalized[key] = {
                "amount": value["valueCurrency"].get("amount"),
                "currency": value["valueCurrency"].get("currencyCode"),
            }
        else:
            normalized[key] = value.get("content") or value.get("valueString") or value.get("valueDate")

    return normalized


def translate_if_needed(text: str) -> str:
    if not text:
        return ""
    return translator.translate(text=text, source_language="es", target_language="en") or text


def build_output_row(file_path: str, model_json: Dict[str, Any]) -> Dict[str, Any]:
    desc = model_json.get("service_description", "")
    reason = model_json.get("assessment_reason", "")

    return {
        "file_path": file_path,
        "vendor_name": model_json.get("vendor_name", ""),
        "description_of_service": translate_if_needed(desc),
        "type_of_service": model_json.get("service_type", ""),
        "amount": model_json.get("amount", ""),
        "currency": model_json.get("currency", ""),
        "invoice_id": model_json.get("invoice_id", ""),
        "date": model_json.get("invoice_date", ""),
        "allowability_assessment": model_json.get("assessment", "needs review"),
        "allowability_score": model_json.get("allowability_score", 0),
        "assessment_reason": translate_if_needed(reason),
        "processed_utc": datetime.utcnow().isoformat(),
    }


# COMMAND ----------

# Native SharePoint listing; no custom wrapper around list/download behavior
sp_items = sp_file.ls(SHAREPOINT_FOLDER_PATH, recursive=True, type="file", include_properties=False)
file_paths = [
    item if isinstance(item, str) else item.get("path") or item.get("serverRelativeUrl")
    for item in sp_items
]
file_paths = [p for p in file_paths if p and p.lower().endswith(ALLOWED_EXTENSIONS)]

print(f"Found {len(file_paths)} candidate files in SharePoint folder: {SHAREPOINT_FOLDER_PATH}")

# COMMAND ----------

results: List[Dict[str, Any]] = []
system_prompt = build_classifier_system_prompt()

for idx, file_path in enumerate(file_paths, 1):
    print(f"Processing {idx}/{len(file_paths)}: {file_path}")
    try:
        content = sp_file.download_file(file_path, return_bytes=True)

        # Prefer invoice model first, then OCR fallback for broader docs
        di_result = doc_client.analyze_document(document_bytes=content, model_id="prebuilt-invoice")
        invoice_fields = extract_invoice_fields(di_result)
        extracted_text = di_result.get("content", "") or extract_text(content, ai=True)

        user_prompt = build_classifier_user_prompt(file_path.split("/")[-1], extracted_text, invoice_fields)
        response = prompter.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            response_format="json",
            max_tokens=800,
        )
        model_json = json.loads(response)

        results.append(build_output_row(file_path, model_json))

    except Exception as ex:
        print(f"  ERROR: {ex}")
        results.append(
            {
                "file_path": file_path,
                "vendor_name": "",
                "description_of_service": "",
                "type_of_service": "",
                "amount": "",
                "currency": "",
                "invoice_id": "",
                "date": "",
                "allowability_assessment": "needs review",
                "allowability_score": 0,
                "assessment_reason": f"Processing error: {ex}",
                "processed_utc": datetime.utcnow().isoformat(),
            }
        )

# COMMAND ----------

results_df = pd.DataFrame(results)
display(results_df)

output_csv = "/dbfs/mnt/output/boe_invoice_assessments.csv"
results_df.to_csv(output_csv, index=False)
print(f"Saved output to {output_csv}")
