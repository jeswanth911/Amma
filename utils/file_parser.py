import os
import re
import json
import shutil
import sqlite3
import mimetypes
import xmltodict
import pandas as pd

from typing import Optional
from email import policy
from email.parser import BytesParser
from fastapi import UploadFile

from utils.logger import logger  # Ensure this exists in your utils/ folder

# Optional imports
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import hl7
except ImportError:
    hl7 = None

# Supported file formats
SUPPORTED_FORMATS = [
    ".csv", ".xlsx", ".xls", ".json", ".parquet", ".txt",
    ".xml", ".sql", ".log", ".hl7", ".pdf", ".eml"
]

UPLOAD_DIR = "data/uploaded"

# Save uploaded file to local directory
def save_uploaded_file(uploaded_file: UploadFile, upload_dir: str = UPLOAD_DIR) -> str:
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, uploaded_file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(uploaded_file.file, buffer)
    logger.info(f"✅ File saved: {file_path}")
    return file_path

# Check if file format is supported
def is_supported_format(file_path: str) -> bool:
    ext = os.path.splitext(file_path)[1].lower()
    return ext in SUPPORTED_FORMATS

# Detect MIME/file type
def detect_file_format(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()
    if ext in SUPPORTED_FORMATS:
        return ext
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or "unknown"

# Entry point: Parse any supported file to DataFrame
def parse_file(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == '.csv':
            return parse_csv(file_path)
        elif ext in ['.xls', '.xlsx']:
            return parse_excel(file_path)
        elif ext == '.json':
            return parse_json(file_path)
        elif ext == '.parquet':
            return parse_parquet(file_path)
        elif ext == '.txt':
            return parse_txt(file_path)
        elif ext == '.xml':
            return parse_xml(file_path)
        elif ext == '.sql':
            return parse_sql(file_path)
        elif ext == '.log':
            return parse_log(file_path)
        elif ext == '.hl7':
            return parse_hl7(file_path)
        elif ext == '.pdf':
            return parse_pdf(file_path)
        elif ext == '.eml':
            return parse_eml(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
    except Exception as e:
        logger.error(f"❌ Failed to parse {file_path}: {str(e)}")
        return pd.DataFrame()

# Format-specific parsers below ⬇️
def parse_csv(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path, encoding="utf-8", errors="replace")
    except Exception as e:
        logger.error(f"❌ CSV parse error: {e}")
        return pd.DataFrame()

def parse_excel(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"❌ Excel parse error: {e}")
        return pd.DataFrame()

def parse_json(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return pd.json_normalize(data)
    except Exception as e:
        logger.error(f"❌ JSON parse error: {e}")
        return pd.DataFrame()

def parse_parquet(file_path: str) -> pd.DataFrame:
    if not pq:
        logger.error("❌ pyarrow not installed.")
        return pd.DataFrame()
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        logger.error(f"❌ Parquet parse error: {e}")
        return pd.DataFrame()

def parse_txt(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        return pd.DataFrame({"line": [line.strip() for line in lines if line.strip()]})
    except Exception as e:
        logger.error(f"❌ TXT parse error: {e}")
        return pd.DataFrame()

def parse_xml_file(file_path: str) -> pd.DataFrame:
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Flatten the XML tree into a list of dictionaries
        records = []
        for elem in root:
            record = {}
            for sub_elem in elem.iter():
                if sub_elem is not elem:
                    record[sub_elem.tag] = sub_elem.text
            records.append(record)

        df = pd.DataFrame(records)
        return df
    except Exception as e:
        raise ValueError(f"Error parsing XML file: {e}")
        

def parse_sql_file(file_path_or_bytes) -> pd.DataFrame:
    """
    Parses a .sql file with INSERT statements and returns a pandas DataFrame.
    """
    import re

    if isinstance(file_path_or_bytes, bytes):
        content = file_path_or_bytes.decode("utf-8")
    else:
        with open(file_path_or_bytes, "r", encoding="utf-8") as f:
            content = f.read()

    # Extract INSERT INTO values
    insert_values = re.findall(r"INSERT INTO .*? VALUES\s*(\(.*?\));", content, re.DOTALL)

    if not insert_values:
        raise ValueError("No INSERT INTO statements found in SQL file")

    # Extract tuples
    records = [eval(val) for val in insert_values]  # Caution: eval should be sandboxed in real systems
    df = pd.DataFrame(records)

    return df
    

def parse_log(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        pattern = r'^\[?(?P<timestamp>.*?)\]?\s+(?P<level>\w+):\s+(?P<message>.*)$'
        parsed = [re.match(pattern, line).groupdict() for line in lines if re.match(pattern, line)]
        return pd.DataFrame(parsed)
    except Exception as e:
        logger.error(f"❌ LOG parse error: {e}")
        return pd.DataFrame()

def parse_hl7_file(file_path: str) -> pd.DataFrame:
    import hl7
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        hl7_content = f.read()
    try:
        h = hl7.parse(hl7_content)
        data = [[str(segment[i]) for i in range(len(segment))] for segment in h]
        return pd.DataFrame(data)
    except Exception as e:
        raise ValueError(f"Failed to parse HL7 file: {e}")
        

def parse_pdf(file_path: str) -> pd.DataFrame:
    if not pdfplumber:
        logger.error("❌ pdfplumber not installed.")
        return pd.DataFrame()
    try:
        text = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                content = page.extract_text()
                if content:
                    text.append(content)
        return pd.DataFrame({'page': list(range(1, len(text) + 1)), 'text': text})
    except Exception as e:
        logger.error(f"❌ PDF parse error: {e}")
        return pd.DataFrame()

def parse_eml(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'rb') as f:
            msg = BytesParser(policy=policy.default).parse(f)
        content = {
            "subject": msg["subject"],
            "from": msg["from"],
            "to": msg["to"],
            "date": msg["date"],
            "body": msg.get_body(preferencelist=('plain', 'html')).get_content() if msg.get_body() else ""
        }
        return pd.DataFrame([content])
    except Exception as e:
        logger.error(f"❌ EML parse error: {e}")
        return pd.DataFrame()
        
