import os
import re
import json
import shutil
import mimetypes
import xml.etree.ElementTree as ET
import pandas as pd
import eml_parser
from fastapi import UploadFile
from utils.logger import logger  # Ensure logger is configured
import pandas as pd
import os
import json
import xml.etree.ElementTree as ET
import csv
import io
import pdfplumber
import pyarrow.parquet as pq
from typing import Union

def parse_csv_file(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def parse_excel_file(file_path: str) -> pd.DataFrame:
    return pd.read_excel(file_path)

def parse_json_file(file_path: str) -> pd.DataFrame:
    with open(file_path, 'r') as f:
        data = json.load(f)
    return pd.json_normalize(data)

def parse_parquet_file(file_path: str) -> pd.DataFrame:
    return pd.read_parquet(file_path)



def parse_xml_file(file_path: str) -> pd.DataFrame:
    tree = ET.parse(file_path)
    root = tree.getroot()
    all_data = []
    for child in root:
        row_data = {subchild.tag: subchild.text for subchild in child}
        all_data.append(row_data)
    return pd.DataFrame(all_data)

def parse_pdf_file(file_path: str) -> pd.DataFrame:
    all_text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            all_text += page.extract_text() + "\n"
    return pd.DataFrame({'text': [line.strip() for line in all_text.strip().split('\n') if line.strip()]})

# Add a generic file parser
def parse_file(file_path: str, extension: str) -> pd.DataFrame:
    extension = extension.lower()
    if extension == '.csv':
        return parse_csv_file(file_path)
    elif extension in ['.xls', '.xlsx']:
        return parse_excel_file(file_path)
    elif extension == '.json':
        return parse_json_file(file_path)
    elif extension == '.parquet':
        return parse_parquet_file(file_path)
    elif extension == '.txt':
        return parse_txt_file(file_path)
    elif extension == '.xml':
        return parse_xml_file(file_path)
    elif extension == '.pdf':
        return parse_pdf_file(file_path)
    else:
        raise ValueError(f"Unsupported file type: {extension}")
        





# Optional dependencies
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    import hl7
except ImportError:
    hl7 = None

SUPPORTED_FORMATS = [
    ".csv", ".xlsx", ".xls", ".json", ".parquet", ".txt",
    ".xml", ".sql", ".log", ".hl7", ".pdf", ".eml"
]

UPLOAD_DIR = "data/uploaded"

# ----------------------------
# File Handling
# ----------------------------
def save_uploaded_file(file: UploadFile, destination_folder: str = UPLOAD_DIR) -> str:
    os.makedirs(destination_folder, exist_ok=True)
    file_path = os.path.join(destination_folder, file.filename)

    with open(file_path, "wb") as f:
        f.write(file.file.read())

    logger.info(f"✅ File saved: {file_path}")
    return file_path

def is_supported_format(file_path: str) -> bool:
    ext = os.path.splitext(file_path)[1].lower()
    return ext in SUPPORTED_FORMATS

def detect_file_format(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()
    if ext in SUPPORTED_FORMATS:
        return ext
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or "unknown"

# ----------------------------
# Dispatcher
# ----------------------------
def parse_file(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    parser_map = {
        ".csv": parse_csv,
        ".xlsx": parse_excel,
        ".xls": parse_excel,
        ".json": parse_json,
        ".parquet": parse_parquet,
        ".txt": parse_txt,
        ".xml": parse_xml,
        ".sql": parse_sql,
        ".log": parse_log,
        ".hl7": parse_hl7,
        ".pdf": parse_pdf,
        ".eml": parse_eml,
    }

    if ext not in parser_map:
        logger.error(f"❌ Unsupported file format: {ext}")
        return pd.DataFrame()

    try:
        df = parser_map[ext](file_path)
        logger.info(f"📄 Parsed {ext} file with shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"❌ Error parsing {ext} file: {e}")
        return pd.DataFrame()

# ----------------------------
# Format-specific Parsers
# ----------------------------
def parse_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path, encoding="utf-8", errors="replace")

def parse_excel(file_path: str) -> pd.DataFrame:
    return pd.read_excel(file_path)

def parse_json(file_path: str) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.json_normalize(data)

def parse_parquet(file_path: str) -> pd.DataFrame:
    if not pq:
        raise ImportError("pyarrow is required for .parquet files")
    return pd.read_parquet(file_path)

def parse_xml(file_path: str) -> pd.DataFrame:
    tree = ET.parse(file_path)
    root = tree.getroot()
    records = []
    for elem in root:
        record = {sub.tag: sub.text for sub in elem}
        records.append(record)
    return pd.DataFrame(records)

def parse_sql(file_path: str) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    insert_values = re.findall(r"INSERT INTO .*? VALUES\s*(\(.*?\));", content, re.DOTALL)
    if not insert_values:
        raise ValueError("No INSERT INTO statements found.")

    records = [eval(val) for val in insert_values]  # Use with care
    return pd.DataFrame(records)

def parse_log(file_path: str) -> pd.DataFrame:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    return pd.DataFrame({"log_line": [line.strip() for line in lines if line.strip()]})

def parse_hl7(file_path: str) -> pd.DataFrame:
    if not hl7:
        raise ImportError("hl7 library is not installed.")
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    h = hl7.parse(content)
    rows = [[str(seg[i]) for i in range(len(seg))] for seg in h]
    return pd.DataFrame(rows)

def parse_pdf(file_path: str) -> pd.DataFrame:
    if not fitz:
        raise ImportError("PyMuPDF (fitz) is not installed.")
    text = ""
    with fitz.open(file_path) as doc:
        for page in doc:
            text += page.get_text()
    lines = text.split('\n')
    return pd.DataFrame({"content": [line.strip() for line in lines if line.strip()]})

def parse_eml(file_path: str) -> pd.DataFrame:
    with open(file_path, "rb") as f:
        raw_email = f.read()
    ep = eml_parser.EmlParser()
    parsed = ep.decode_email_bytes(raw_email)
    return pd.json_normalize(parsed)


# utils/file_parser.py

def parse_sql_file(file_path: str) -> pd.DataFrame:
    import pandas as pd
    import sqlite3

    with open(file_path, 'r') as f:
        sql_script = f.read()

    # Create a temporary in-memory SQLite DB
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    try:
        cursor.executescript(sql_script)
        conn.commit()

        # Try to find tables created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            raise ValueError("No tables found in SQL file.")

        # Select the first table
        table_name = tables[0][0]
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df
    finally:
        conn.close()
        
def parse_text_file(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        data = [line.strip().split(",") for line in lines if line.strip()]
        df = pd.DataFrame(data[1:], columns=data[0])  # assuming first row = headers
        return df
    except Exception as e:
        raise ValueError(f"Error parsing TXT file: {e}")
        
