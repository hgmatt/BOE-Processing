# Databricks notebook source
# MAGIC %md
# MAGIC # Spanish Invoice Processing and Classification System
# MAGIC 
# MAGIC This notebook processes Spanish language invoices (machine-readable, scanned, and handwritten),
# MAGIC extracts key information, and classifies expenses as allowable operating expenses for residential 
# MAGIC leases per the U.S. Department of State's Foreign Affairs Manual (FAM) guidelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initialize Funhouse Environment

# COMMAND ----------

# Run Funhouse setup to initialize authenticated default services
%run /setup_python

# This provides:
# - fh_sp_client: Default SharePoint client
# - fh_sp_file: Default SharePoint file manager
# - fh_sp_list: Default SharePoint list manager
# - Other Funhouse framework initialization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Notes

# COMMAND ----------

# MAGIC This notebook is designed for the Funhouse runtime.
# MAGIC Avoid restarting Python after `/setup_python`, since that would clear authenticated service objects.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Setup

# COMMAND ----------

import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import re

from funhouse.utils import extract_text

# Note: SharePoint services (fh_sp_client, fh_sp_file, fh_sp_list) are available 
# by default after running %run /setup_python - no import needed

# Databricks imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Service Initialization

# COMMAND ----------

# Storage Configuration
# If USE_SHAREPOINT is False, provide a DBFS path that contains PDF invoices.
INPUT_STORAGE_PATH = "/mnt/invoices-input"

# SharePoint Configuration (Optional - for reading invoices from SharePoint)
USE_SHAREPOINT = False  # Set to True to read invoices from SharePoint
SHAREPOINT_LIBRARY_NAME = "Invoices"  # Document library name
SHAREPOINT_FOLDER_PATH = "/Spanish Invoices"  # Folder path within library

# Initialize authenticated Funhouse services from /setup_python
document_intelligence = fh_doc
translator = fh_translator
prompter = fh_prompter

# Note: SharePoint connection objects (fh_sp_client, fh_sp_file, fh_sp_list) are 
# available by default after running %run /setup_python
# No need to initialize them - they're already configured and ready to use!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Foreign Affairs Manual (FAM) - Residential Lease Operating Expense Guidelines

# COMMAND ----------

FAM_GUIDELINES = """
FOREIGN AFFAIRS MANUAL (FAM) - ALLOWABLE OPERATING EXPENSES FOR RESIDENTIAL LEASES ABROAD

Per U.S. Department of State guidelines for operating leases of residences for diplomatic personnel stationed abroad:

ALLOWABLE EXPENSES (High Score: 80-100):
- Utilities included in lease (electricity, water, gas, heating/cooling)
- Building maintenance and repairs (if landlord's responsibility per lease)
- Common area maintenance fees (HOA fees, condo fees)
- Property taxes (if tenant's responsibility per lease terms)
- Building insurance premiums (if required by lease)
- Trash/waste removal services
- Water/sewage fees
- Internet and basic telecommunications (if included in lease)
- Elevator maintenance (if part of common area fees)
- Security services (building security, gate access)
- Pest control services
- Landscaping/grounds maintenance (common areas)
- Snow removal (common areas)
- Swimming pool maintenance (if shared amenity)
- Gym/fitness facility fees (if part of building amenities)

POTENTIALLY ALLOWABLE (Medium Score: 50-79):
- Emergency repairs necessary for habitability
- Minor repairs to make residence suitable for occupancy
- Utilities if separately metered and reasonable
- Parking fees (if included in lease)
- Storage unit fees (if part of lease)
- Building access cards/keys replacement
- Move-in/move-out cleaning fees
- Mandatory homeowner association fees
- Pet deposits (if authorized)

NOT ALLOWABLE (Low Score: 0-49):
- Furniture purchases or rentals
- Appliance purchases (washer, dryer, refrigerator, etc.)
- Electronics and entertainment systems
- Interior decorating or cosmetic improvements
- Personal property insurance
- Telephone service beyond basic
- Cable/satellite TV premium packages
- Personal internet upgrades beyond basic
- Gym memberships external to building
- Cleaning services/housekeeping
- Gardening services (for private yard)
- Private security services
- Vehicle-related expenses (parking tickets, car washes)
- Damage deposits (refundable)
- Personal modifications or alterations
- Key money or goodwill payments beyond standard deposit
- Broker fees or finder's fees (handled separately)
- Non-essential amenities or luxury upgrades
- Meals, catering, entertainment
- Personal services (drivers, domestic help)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Translation Functions

# COMMAND ----------

def translate_text(text: str, source_lang: str = "es", target_lang: str = "en") -> str:
    """
    Translate text using Funhouse Azure Translator service.
    
    Args:
        text: Text to translate
        source_lang: Source language code (default: "es" for Spanish)
        target_lang: Target language code (default: "en" for English)
        
    Returns:
        Translated text
    """
    if not text or len(text.strip()) == 0:
        return ""
    
    try:
        # Use Funhouse translator service
        translated_text = translator.translate(
            text=text[:10000],  # Limit to 10K characters per request
            source_language=source_lang,
            target_language=target_lang
        )
        
        return translated_text if translated_text else text
        
    except Exception as e:
        print(f"Translation error: {str(e)}")
        return text  # Return original text on error


def translate_invoice_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translate specific fields from Spanish to English.
    
    Args:
        data: Dictionary containing invoice data
        
    Returns:
        Dictionary with translated fields
    """
    fields_to_translate = ['description', 'expense_classification', 'allowability_reasoning']
    
    translated_data = data.copy()
    
    for field in fields_to_translate:
        if field in translated_data and translated_data[field]:
            original = translated_data[field]
            translated = translate_text(original)
            # Store both original and translated
            translated_data[f'{field}_spanish'] = original
            translated_data[field] = translated
    
    return translated_data


def batch_translate_dataframe(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Translate multiple columns in a DataFrame efficiently.
    
    Args:
        df: Pandas DataFrame
        columns: List of column names to translate
        
    Returns:
        DataFrame with translated columns
    """
    df_translated = df.copy()
    
    for column in columns:
        if column in df_translated.columns:
            print(f"Translating column: {column}")
            # Store original in new column
            df_translated[f'{column}_spanish'] = df_translated[column]
            # Translate and replace
            df_translated[column] = df_translated[column].apply(
                lambda x: translate_text(str(x)) if pd.notna(x) and str(x).strip() != '' else x
            )
    
    return df_translated

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Invoice Text Extraction Functions

# COMMAND ----------

def download_file_from_sharepoint(file_name: str, library_name: str, folder_path: str = "") -> Optional[bytes]:
    """
    Download a file from SharePoint using default Funhouse SharePoint services.
    
    Args:
        file_name: Name of the file to download
        library_name: SharePoint document library name
        folder_path: Folder path within the library (default: root)
        
    Returns:
        File content as bytes, or None if download fails
    """
    # Check if default SharePoint connection is available
    if 'fh_sp_file' not in globals() or fh_sp_file is None:
        print("SharePoint not configured. Run %run /setup_python first.")
        return None
    
    try:
        # Construct a single SharePoint path compatible with fh_sp_file.download_file(path, return_bytes=True)
        path_parts = [library_name.strip("/")] if library_name else []
        if folder_path:
            path_parts.append(folder_path.strip("/"))
        path_parts.append(file_name.strip("/"))
        sp_path = "/" + "/".join([part for part in path_parts if part])

        # Download file using default Funhouse SharePoint file manager
        file_content = fh_sp_file.download_file(sp_path, return_bytes=True)

        return file_content
        
    except Exception as e:
        print(f"Error downloading {file_name} from SharePoint: {str(e)}")
        return None


def get_invoice_files_from_sharepoint(library_name: str, folder_path: str = "") -> List[str]:
    """
    Get list of PDF invoice files from SharePoint.
    
    Args:
        library_name: SharePoint document library name
        folder_path: Folder path within the library (default: root)
        
    Returns:
        List of file names
    """
    # Check if default SharePoint connection is available
    if 'fh_sp_file' not in globals() or fh_sp_file is None:
        print("SharePoint not configured. Run %run /setup_python first.")
        return []
    
    try:
        # Build SharePoint folder path and list files
        path_parts = [library_name.strip("/")] if library_name else []
        if folder_path:
            path_parts.append(folder_path.strip("/"))
        sp_folder = "/" + "/".join([part for part in path_parts if part])

        files = fh_sp_file.list_files(sp_folder)

        # Normalize different response shapes (string paths vs metadata dicts)
        normalized_files = []
        for file_item in files:
            if isinstance(file_item, str):
                normalized_files.append(file_item)
            elif isinstance(file_item, dict):
                normalized_files.append(file_item.get("name") or file_item.get("file_name") or "")

        # Filter for PDF files only
        pdf_files = [f for f in normalized_files if f and f.lower().endswith('.pdf')]

        return pdf_files
        
    except Exception as e:
        print(f"Error listing files from SharePoint: {str(e)}")
        return []


def update_invoice_tracking_list(invoice_data: Dict[str, Any], list_name: str = "Invoice Tracking") -> bool:
    """
    Update a SharePoint list with invoice processing results.
    
    Args:
        invoice_data: Dictionary containing invoice information
        list_name: Name of the SharePoint list (default: "Invoice Tracking")
        
    Returns:
        True if successful, False otherwise
    """
    # Check if default SharePoint connection is available
    if 'fh_sp_list' not in globals() or fh_sp_list is None:
        return False
    
    try:
        # Prepare list item data
        list_item = {
            "Title": invoice_data.get("invoice_number", "Unknown"),
            "VendorName": invoice_data.get("vendor_name", ""),
            "ServiceType": invoice_data.get("service_type", ""),
            "Amount": invoice_data.get("amount", "0"),
            "Currency": invoice_data.get("currency", "EUR"),
            "AllowabilityScore": invoice_data.get("allowability_score", 0),
            "Description": invoice_data.get("description", ""),
            "ProcessedDate": datetime.now().isoformat(),
            "Status": "Processed"
        }
        
        # Add item to SharePoint list using default manager
        fh_sp_list.add_list_item(
            list_name=list_name,
            item_data=list_item
        )
        
        return True
        
    except Exception as e:
        print(f"Error updating SharePoint list: {str(e)}")
        return False

def extract_text_from_pdf(pdf_path: str, pdf_bytes: Optional[bytes] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Extract text from PDF using Funhouse Azure Document Intelligence service.
    Handles machine-readable, scanned, and handwritten text.
    
    Args:
        pdf_path: Path to PDF file (used for display/logging)
        pdf_bytes: Optional PDF content as bytes (for SharePoint files)
        
    Returns:
        Tuple of (extracted_text, metadata_dict)
    """
    try:
        # Read PDF file if bytes not provided
        if pdf_bytes is None:
            with open(pdf_path, "rb") as f:
                pdf_content = f.read()
        else:
            pdf_content = pdf_bytes
        
        # Use Funhouse Document Intelligence service with prebuilt-invoice model
        result = document_intelligence.analyze_document(
            document_bytes=pdf_content,
            model_id="prebuilt-invoice"
        )
        
        # Extract all text content (fallback to utility extraction if needed)
        full_text = result.get("content", "")
        if not full_text:
            full_text = extract_text(pdf_content, ai=True)
        
        # Extract structured fields if available
        metadata = {
            "vendor_name": None,
            "invoice_id": None,
            "invoice_date": None,
            "total_amount": None,
            "currency": None,
            "confidence_score": 0.0
        }
        
        # Parse invoice fields from result
        if "documents" in result and len(result["documents"]) > 0:
            invoice_fields = result["documents"][0].get("fields", {})
            
            # Extract vendor information
            if "VendorName" in invoice_fields:
                vendor_field = invoice_fields["VendorName"]
                metadata["vendor_name"] = vendor_field.get("content") or vendor_field.get("valueString")
                metadata["confidence_score"] = max(metadata["confidence_score"], 
                                                   vendor_field.get("confidence", 0))
            
            # Extract invoice ID
            if "InvoiceId" in invoice_fields:
                invoice_id_field = invoice_fields["InvoiceId"]
                metadata["invoice_id"] = invoice_id_field.get("content") or invoice_id_field.get("valueString")
                metadata["confidence_score"] = max(metadata["confidence_score"], 
                                                   invoice_id_field.get("confidence", 0))
            
            # Extract date
            if "InvoiceDate" in invoice_fields:
                date_field = invoice_fields["InvoiceDate"]
                metadata["invoice_date"] = str(date_field.get("content") or date_field.get("valueDate"))
                metadata["confidence_score"] = max(metadata["confidence_score"], 
                                                   date_field.get("confidence", 0))
            
            # Extract total
            if "InvoiceTotal" in invoice_fields:
                total_field = invoice_fields["InvoiceTotal"]
                if "valueCurrency" in total_field:
                    metadata["total_amount"] = total_field["valueCurrency"].get("amount")
                    metadata["currency"] = total_field["valueCurrency"].get("currencyCode", "EUR")
                else:
                    metadata["total_amount"] = total_field.get("content")
                    metadata["currency"] = "EUR"
                metadata["confidence_score"] = max(metadata["confidence_score"], 
                                                   total_field.get("confidence", 0))
        
        return full_text, metadata
        
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {str(e)}")
        return "", {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. AI-Powered Classification and Extraction

# COMMAND ----------

def classify_and_extract_invoice_info(invoice_text: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Use Funhouse Prompter API to classify expense type and extract relevant information from Spanish invoices.
    Translates key fields to English while preserving Spanish originals.
    
    Args:
        invoice_text: Extracted text from invoice
        metadata: Pre-extracted metadata from Document Intelligence
        
    Returns:
        Dictionary with classification results (English) and Spanish originals
    """
    
    system_prompt = f"""You are an expert in analyzing Spanish language invoices and classifying operating expenses for residential leases according to the U.S. Department of State's Foreign Affairs Manual (FAM) guidelines.

Context: These are expenses related to leased residences for U.S. diplomatic personnel stationed abroad.

Your task is to:
1. Extract key information from the invoice
2. Classify the expense type
3. Determine if it's an allowable operating expense for a residential lease per FAM
4. Provide an allowability score (0-100)

FAM Guidelines for Allowable Residential Lease Operating Expenses:
{FAM_GUIDELINES}

Important: Focus on whether the expense is:
- A normal operating cost of the leased residence
- Essential for habitability and basic living
- Part of common building fees or required by the lease
- NOT furniture, personal services, or luxury items

Respond ONLY with valid JSON in this exact format:
{{
    "vendor_name": "Company name",
    "invoice_number": "Invoice ID",
    "service_type": "Category of service (in English)",
    "description": "Brief description of service (1-2 sentences in Spanish)",
    "expense_classification": "Specific expense category (in English)",
    "allowability_score": 85,
    "allowability_reasoning": "Brief explanation of score referencing FAM residential lease guidelines (in Spanish)",
    "extracted_amount": "123.45",
    "currency": "EUR",
    "confidence": "high/medium/low"
}}
"""

    user_prompt = f"""Analyze this Spanish invoice and extract the required information:

INVOICE TEXT:
{invoice_text[:4000]}  

EXTRACTED METADATA:
Vendor: {metadata.get('vendor_name', 'Not detected')}
Invoice ID: {metadata.get('invoice_id', 'Not detected')}
Amount: {metadata.get('total_amount', 'Not detected')} {metadata.get('currency', '')}
Date: {metadata.get('invoice_date', 'Not detected')}

Please classify this expense and provide the JSON response. Keep description and allowability_reasoning in Spanish - they will be translated separately."""

    try:
        # Use Funhouse Prompter API
        response = prompter.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=1000,
            response_format="json"
        )
        
        # Parse JSON response
        result = json.loads(response)
        
        # Fallback to metadata if extraction failed
        if not result.get("vendor_name") and metadata.get("vendor_name"):
            result["vendor_name"] = metadata["vendor_name"]
        if not result.get("invoice_number") and metadata.get("invoice_id"):
            result["invoice_number"] = metadata["invoice_id"]
        if not result.get("extracted_amount") and metadata.get("total_amount"):
            result["extracted_amount"] = str(metadata["total_amount"])
        if not result.get("currency") and metadata.get("currency"):
            result["currency"] = metadata["currency"]
        
        # Translate Spanish fields to English
        print(f"  Translating fields for {result.get('vendor_name', 'Unknown')}...")
        result = translate_invoice_fields(result)
            
        return result
        
    except Exception as e:
        print(f"Error in AI classification: {str(e)}")
        return {
            "vendor_name": metadata.get("vendor_name", "Unknown"),
            "invoice_number": metadata.get("invoice_id", "Unknown"),
            "service_type": "Unable to classify",
            "description": "Error during classification",
            "description_spanish": "Error durante la clasificación",
            "expense_classification": "Error",
            "allowability_score": 0,
            "allowability_reasoning": f"Classification error: {str(e)}",
            "allowability_reasoning_spanish": f"Error de clasificación: {str(e)}",
            "extracted_amount": str(metadata.get("total_amount", "0")),
            "currency": metadata.get("currency", "EUR"),
            "confidence": "low"
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Batch Processing Function

# COMMAND ----------

def process_invoice_batch(pdf_paths: List[str], use_sharepoint: bool = False) -> pd.DataFrame:
    """
    Process a batch of invoice PDFs and return a summary DataFrame.
    Includes both English translations and Spanish originals.
    
    Args:
        pdf_paths: List of paths/names to PDF files
        use_sharepoint: If True, retrieve files from SharePoint instead of local/blob storage
        
    Returns:
        Pandas DataFrame with processed results
    """
    results = []
    
    for i, pdf_path in enumerate(pdf_paths):
        print(f"Processing {i+1}/{len(pdf_paths)}: {pdf_path}")
        
        try:
            pdf_bytes = None
            
            # Retrieve file from SharePoint if configured
            if use_sharepoint and USE_SHAREPOINT:
                pdf_bytes = download_file_from_sharepoint(
                    file_name=pdf_path,
                    library_name=SHAREPOINT_LIBRARY_NAME,
                    folder_path=SHAREPOINT_FOLDER_PATH
                )
                if pdf_bytes is None:
                    print(f"  Warning: Could not download {pdf_path} from SharePoint")
                    continue
            
            # Extract text and metadata (pass bytes if from SharePoint)
            invoice_text, metadata = extract_text_from_pdf(pdf_path, pdf_bytes=pdf_bytes)
            
            if not invoice_text:
                print(f"  Warning: No text extracted from {pdf_path}")
                continue
            
            # Classify and extract information (includes translation)
            classification = classify_and_extract_invoice_info(invoice_text, metadata)
            
            # Combine results with both English and Spanish fields
            result = {
                "file_name": os.path.basename(pdf_path) if not use_sharepoint else pdf_path,
                "file_path": pdf_path,
                "source": "SharePoint" if use_sharepoint else "DBFS",
                "vendor_name": classification.get("vendor_name", "Unknown"),
                "invoice_number": classification.get("invoice_number", "Unknown"),
                "service_type": classification.get("service_type", "Unknown"),
                "description": classification.get("description", ""),  # English translation
                "description_spanish": classification.get("description_spanish", ""),  # Spanish original
                "expense_classification": classification.get("expense_classification", "Unknown"),
                "expense_classification_spanish": classification.get("expense_classification_spanish", ""),
                "allowability_score": classification.get("allowability_score", 0),
                "allowability_reasoning": classification.get("allowability_reasoning", ""),  # English translation
                "allowability_reasoning_spanish": classification.get("allowability_reasoning_spanish", ""),  # Spanish original
                "amount": classification.get("extracted_amount", "0"),
                "currency": classification.get("currency", "EUR"),
                "extraction_confidence": classification.get("confidence", "unknown"),
                "ocr_confidence": metadata.get("confidence_score", 0),
                "processed_date": datetime.now().isoformat()
            }
            
            results.append(result)
            print(f"  ✓ Processed: {result['vendor_name']} - Score: {result['allowability_score']}")
            
            # Optionally update SharePoint tracking list
            if use_sharepoint and USE_SHAREPOINT:
                update_invoice_tracking_list(result)
            
        except Exception as e:
            print(f"  ✗ Error processing {pdf_path}: {str(e)}")
            results.append({
                "file_name": os.path.basename(pdf_path) if not use_sharepoint else pdf_path,
                "file_path": pdf_path,
                "source": "SharePoint" if use_sharepoint else "DBFS",
                "vendor_name": "Error",
                "invoice_number": "Error",
                "service_type": "Error",
                "description": f"Processing error: {str(e)}",
                "description_spanish": f"Error de procesamiento: {str(e)}",
                "expense_classification": "Error",
                "expense_classification_spanish": "Error",
                "allowability_score": 0,
                "allowability_reasoning": "Processing failed",
                "allowability_reasoning_spanish": "Procesamiento fallido",
                "amount": "0",
                "currency": "EUR",
                "extraction_confidence": "error",
                "ocr_confidence": 0,
                "processed_date": datetime.now().isoformat()
            })
    
    return pd.DataFrame(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Main Execution

# COMMAND ----------

# Determine source and get list of PDF files
if USE_SHAREPOINT:
    print("=" * 80)
    print("READING INVOICES FROM SHAREPOINT")
    print("=" * 80)
    print(f"Library: {SHAREPOINT_LIBRARY_NAME}")
    print(f"Folder: {SHAREPOINT_FOLDER_PATH}")
    
    # Get list of PDF files from SharePoint
    pdf_files = get_invoice_files_from_sharepoint(
        library_name=SHAREPOINT_LIBRARY_NAME,
        folder_path=SHAREPOINT_FOLDER_PATH
    )
    pdf_paths = pdf_files
    
    print(f"\nFound {len(pdf_paths)} PDF files in SharePoint")
    
else:
    print("=" * 80)
    print("READING INVOICES FROM DBFS STORAGE PATH")
    print("=" * 80)
    
    # Get list of PDF files from DBFS path
    try:
        pdf_files = dbutils.fs.ls(INPUT_STORAGE_PATH)
        pdf_paths = [file.path for file in pdf_files if file.path.lower().endswith('.pdf')]
        print(f"\nFound {len(pdf_paths)} PDF files in: {INPUT_STORAGE_PATH}")
    except Exception as e:
        print(f"Error listing files from {INPUT_STORAGE_PATH}: {e}")
        pdf_paths = []

# COMMAND ----------

# Process all invoices
if len(pdf_paths) > 0:
    results_df = process_invoice_batch(pdf_paths, use_sharepoint=USE_SHAREPOINT)
    
    # Display results
    display(results_df)
else:
    print("No PDF files found to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Generate Summary Statistics

# COMMAND ----------

print("=" * 80)
print("INVOICE PROCESSING SUMMARY")
print("=" * 80)
print(f"\nTotal Invoices Processed: {len(results_df)}")
print(f"Successful Extractions: {len(results_df[results_df['allowability_score'] > 0])}")
print(f"Errors: {len(results_df[results_df['allowability_score'] == 0])}")

print("\n" + "-" * 80)
print("ALLOWABILITY BREAKDOWN")
print("-" * 80)
allowable = len(results_df[results_df['allowability_score'] >= 80])
potentially_allowable = len(results_df[(results_df['allowability_score'] >= 50) & 
                                       (results_df['allowability_score'] < 80)])
not_allowable = len(results_df[results_df['allowability_score'] < 50])

print(f"Allowable (80-100): {allowable} ({allowable/len(results_df)*100:.1f}%)")
print(f"Potentially Allowable (50-79): {potentially_allowable} ({potentially_allowable/len(results_df)*100:.1f}%)")
print(f"Not Allowable (0-49): {not_allowable} ({not_allowable/len(results_df)*100:.1f}%)")

print("\n" + "-" * 80)
print("TOP EXPENSE CATEGORIES")
print("-" * 80)
top_categories = results_df['expense_classification'].value_counts().head(10)
print(top_categories)

print("\n" + "-" * 80)
print("AVERAGE ALLOWABILITY SCORE BY SERVICE TYPE")
print("-" * 80)
avg_scores = results_df.groupby('service_type')['allowability_score'].mean().sort_values(ascending=False)
print(avg_scores.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Export Results

# COMMAND ----------

# Export to Excel
output_excel_path = "/dbfs/mnt/output/invoice_summary.xlsx"
results_df.to_excel(output_excel_path, index=False, engine='openpyxl')
print(f"Results exported to: {output_excel_path}")

# Export to CSV
output_csv_path = "/dbfs/mnt/output/invoice_summary.csv"
results_df.to_csv(output_csv_path, index=False)
print(f"Results exported to: {output_csv_path}")

# Save to Delta table for further analysis
results_spark_df = spark.createDataFrame(results_df)
results_spark_df.write.format("delta").mode("overwrite").saveAsTable("invoice_processing_results")
print("Results saved to Delta table: invoice_processing_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Generate Detailed Report for Review

# COMMAND ----------

# Filter invoices requiring manual review (medium scores)
review_needed = results_df[(results_df['allowability_score'] >= 40) & 
                           (results_df['allowability_score'] <= 70)]

print(f"\n{len(review_needed)} invoices require manual review:")
print("=" * 80)

for idx, row in review_needed.iterrows():
    print(f"\nVendor: {row['vendor_name']}")
    print(f"Invoice: {row['invoice_number']}")
    print(f"Service: {row['service_type']}")
    print(f"Score: {row['allowability_score']}")
    print(f"Description (EN): {row['description']}")
    print(f"Description (ES): {row['description_spanish']}")
    print(f"Reasoning (EN): {row['allowability_reasoning']}")
    print(f"Reasoning (ES): {row['allowability_reasoning_spanish']}")
    print(f"Amount: {row['amount']} {row['currency']}")
    print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC 
# MAGIC ### Setup Required:
# MAGIC 1. Run `%run /setup_python` in the first cell (already included in Section 0)
# MAGIC    - This initializes all Funhouse services automatically
# MAGIC    - Provides default SharePoint connections: fh_sp_client, fh_sp_file, fh_sp_list
# MAGIC    - No manual configuration needed
# MAGIC 
# MAGIC 2. Choose your invoice source (in Section 3):
# MAGIC    ```python
# MAGIC    # For DBFS storage path
# MAGIC    USE_SHAREPOINT = False
# MAGIC    INPUT_STORAGE_PATH = "/mnt/invoices-input"
# MAGIC    
# MAGIC    # For SharePoint
# MAGIC    USE_SHAREPOINT = True
# MAGIC    SHAREPOINT_LIBRARY_NAME = "Invoices"
# MAGIC    SHAREPOINT_FOLDER_PATH = "/Spanish Invoices"
# MAGIC    ```
# MAGIC 
# MAGIC 3. Upload PDFs to your chosen source
# MAGIC 
# MAGIC 4. Run all cells
# MAGIC 
# MAGIC ### Output:
# MAGIC - Excel and CSV files with complete invoice summaries
# MAGIC - Both English translations and Spanish originals preserved
# MAGIC - Delta table for querying and analysis
# MAGIC - Summary statistics and reports
# MAGIC - Optional: SharePoint list tracking (if using SharePoint source)
# MAGIC 
# MAGIC ### Funhouse Services Used:
# MAGIC - `fh_doc`: OCR and invoice field extraction
# MAGIC - `fh_translator`: Spanish to English translation
# MAGIC - `fh_prompter`: AI-powered classification and extraction
# MAGIC - `fh_sp_client`: Default SharePoint client (from /setup_python)
# MAGIC - `fh_sp_file`: Default SharePoint file manager (from /setup_python)
# MAGIC - `fh_sp_list`: Default SharePoint list manager (from /setup_python)
