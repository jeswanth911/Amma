import os
import pandas as pd
import shutil
import json
import sqlite3
import re
import xmltodict
import pdfplumber
import hl7
import mimetypes
import fitz  # PyMuPDF
import xml.etree.ElementTree as ET
import pyarrow.parquet as pq

from bs4 import BeautifulSoup
from email import policy
from email.parser import BytesParser
from fastapi import UploadFile
from utils.logger import logger
from io import BytesIO
from email import message_from_file
from PyPDF2 import PdfReader


SUPPORTED_FORMATS = [
    ".csv", ".xlsx", ".xls", ".json", ".parquet", ".txt",
    ".xml", ".sql", ".log", ".hl7", ".pdf", ".eml"
]


UPLOAD_DIR = "data"

def is_supported_format(file_path: str) -> bool:
    ext = os.path.splitext(file_path)[1].lower()
    return ext in SUPPORTED_FORMATS

def save_uploaded_file(file: UploadFile, destination_folder: str = "data/uploaded") -> str:
    os.makedirs(destination_folder, exist_ok=True)
    file_path = os.path.join(destination_folder, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return file_path

def parse_file(file_path: str) -> pd.DataFrame:
    """
    Parse supported file formats into a Pandas DataFrame.
    Supported: .csv, .xlsx, .xls, .json, .parquet, .txt, .xml
    """
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".csv":
            return pd.read_csv(file_path, encoding="utf-8", errors="replace")

        elif ext in [".xlsx", ".xls"]:
            return pd.read_excel(file_path)

        elif ext == ".json":
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            return pd.json_normalize(data)

        elif ext == ".parquet":
            return pd.read_parquet(file_path)

        elif ext == ".txt":
            return pd.read_csv(file_path, delimiter="\t", encoding="utf-8", errors="replace")

        elif ext == ".xml":
            tree = ET.parse(file_path)
            root = tree.getroot()
            data = [
                {elem.tag: elem.text for elem in child}
                for child in root
            ]
            return pd.DataFrame(data)

        else:
            raise ValueError(f"Unsupported file format: {ext}")

    except Exception as e:
        raise RuntimeError(f"Failed to parse file '{file_path}': {str(e)}")
                
        

def parse_file_to_df(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)
    elif ext == ".json":
        with open(file_path, 'r') as f:
            data = json.load(f)
        return pd.json_normalize(data)
    elif ext == ".txt":
        return pd.read_csv(file_path, delimiter="\t", engine='python')
    elif ext == ".parquet":
        return pd.read_parquet(file_path)
    elif ext == ".xml":
        return pd.read_xml(file_path)
    elif ext == ".log":
        return pd.read_csv(file_path, delimiter="\t", engine='python', error_bad_lines=False)
    elif ext == ".sql":
        with open(file_path, "r") as f:
            sql_script = f.read()
        return pd.DataFrame({"sql": [sql_script]})
    elif ext == ".pdf":
        from PyPDF2 import PdfReader
        reader = PdfReader(file_path)
        text = "\n".join([page.extract_text() for page in reader.pages])
        return pd.DataFrame({"text": [text]})
    elif ext == ".eml":
        from email import message_from_file
        with open(file_path, 'r') as f:
            msg = message_from_file(f)
        return pd.DataFrame({"subject": [msg["subject"]], "body": [msg.get_payload()]})
    elif ext == ".hl7":
        with open(file_path, "r") as f:
            hl7_raw = f.read()
        segments = hl7_raw.strip().split('\n')
        data = {seg.split("|")[0]: seg for seg in segments}
        return pd.DataFrame([data])
    else:
        raise ValueError(f"❌ Unsupported file format: {ext}")
        
            
            
def save_uploaded_file(file: UploadFile, destination_folder: str = "data/uploaded") -> str:
    os.makedirs(destination_folder, exist_ok=True)
    file_path = os.path.join(destination_folder, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return file_path

def validate_file_format(filename: str) -> bool:
    ext = os.path.splitext(filename)[1].lower()
    return ext in SUPPORTED_EXTENSIONS


def detect_file_format(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()
    if ext in SUPPORTED_EXTENSIONS:
        return ext
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type or "unknown"

def parse_sql_file(file_path: str) -> pd.DataFrame:
    """
    Dummy parser for SQL file input. Needs manual schema logic or SQLAlchemy integration.
    """
    with open(file_path, "r") as f:
        sql_query = f.read()

    # For now, just return the query as a 1-row DataFrame for placeholder
    return pd.DataFrame({"sql_query": [sql_query]})

def parse_xml_file(file_path: str) -> pd.DataFrame:
    with open(file_path, "r") as f:
        xml_content = f.read()
    return pd.DataFrame({"xml_data": [xml_content]})

def parse_hl7_file(file_path: str) -> pd.DataFrame:
    with open(file_path, "r") as f:
        hl7_content = f.read()
    return pd.DataFrame({"hl7_data": [hl7_content]})

def parse_pdf_file(file_path: str) -> pd.DataFrame:
    with open(file_path, "rb") as f:
        content = f.read()
    return pd.DataFrame({"pdf_binary": [content]})

def parse_log_file(file_path: str) -> pd.DataFrame:
    with open(file_path, "r") as f:
        lines = f.readlines()
    return pd.DataFrame({"log_lines": lines})

def parse_eml_file(file_path: str) -> pd.DataFrame:
    with open(file_path, "r") as f:
        email_content = f.read()
    return pd.DataFrame({"email_text": [email_content]})



def parse_file(file_path: str) -> dict:
    try:
        ext = file_path.lower().split('.')[-1]

        if ext == 'csv':
            try:
                df = pd.read_csv(file_path, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding="latin1")  # 👈 fallback for broken utf-8

        elif ext in ['xls', 'xlsx']:
            df = pd.read_excel(file_path)
        elif ext == 'json':
            df = pd.read_json(file_path)
        elif ext == 'parquet':
            df = pd.read_parquet(file_path)
        elif ext == 'txt':
            df = pd.read_csv(file_path, delimiter="\t", encoding="utf-8")
        else:
            return {
                "status": "error",
                "error": f"Unsupported file format: {ext}",
                "dataframe": None
            }

        return {
            "status": "success",
            "dataframe": df,
            "file_path": file_path
        }

    except Exception as e:
        return {
            "status": "error",
            "error": f"File parsing failed: {str(e)}",
            "dataframe": None
        }


def parse_file(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)
    elif ext == ".json":
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return pd.json_normalize(data)
    elif ext == ".parquet":
        return pd.read_parquet(file_path)
    elif ext == ".txt":
        return pd.read_csv(file_path, delimiter="\t")
    elif ext == ".xml":
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = [{elem.tag: elem.text for elem in child} for child in root]
        return pd.DataFrame(data)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
