import os
from utils.logger import logger
from utils.file_parser import parse_file
from data_engine.cleaner import clean_data
from data_engine.analyzer import analyze_data
from data_engine.sql_agent import convert_to_sqlite


def run_full_workflow(file_path: str) -> dict:
    """
    Run the full data pipeline:
    1. Clean the uploaded file
    2. Analyze the cleaned data
    3. Convert to SQLite DB

    Args:
        file_path (str): Path to the uploaded file

    Returns:
        dict: JSON-compatible dictionary with status, analysis summary, and db path
    """
    try:
        logger.info(f"Starting full workflow for file: {file_path}")

        # Step 1: Clean the file
        cleaned_data, cleaned_file_path = clean_data(file_path)
        logger.info(f"File cleaned and saved at: {cleaned_file_path}")

        # Step 2: Analyze the cleaned data
        analysis_summary = analyze_data(cleaned_data)
        logger.info("Analysis complete")

        # Step 3: Convert to SQLite
        db_path = convert_to_sqlite(cleaned_data, cleaned_file_path)
        logger.info(f"SQLite DB created at: {db_path}")

        return {
            "status": "success",
            "analysis_summary": analysis_summary,
            "db_path": db_path
        }

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "analysis_summary": {},
            "db_path": ""
        }
      
