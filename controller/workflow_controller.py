
from utils.logger import logger
from data_engine.sql_agent import convert_to_sqlite
from fastapi import APIRouter, UploadFile, File, HTTPException
from data_engine.cleaner import clean_data
from data_engine.analyzer import analyze_data
from data_engine.sql_agent import NL2SQLAgent
from utils.file_parser import parse_file
from pathlib import Path
import shutil
import os
import uuid

router = APIRouter()


@router.post("/run-workflow/")
async def run_workflow(file: UploadFile = File(...)):
    try:
        # Save uploaded file to a temp location
        temp_dir = "data/uploads"
        os.makedirs(temp_dir, exist_ok=True)
        temp_filename = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = os.path.join(temp_dir, temp_filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Parse the file
        df = parse_file(file_path)

        # Clean the data
        cleaned_df, cleaning_report = clean_data(df)

        # Analyze the data
        analysis_summary = analyze_data(cleaned_df)

        # Save cleaned data to SQLite
        sqlite_path = file_path.replace(".", "_cleaned.").rsplit("/", 1)[-1].replace(" ", "_") + ".db"
        sqlite_path = os.path.join("data/sqlite", sqlite_path)
        os.makedirs("data/sqlite", exist_ok=True)
        table_name = Path(file.filename).stem.replace(" ", "_")
        cleaned_df.to_sql(table_name, f"sqlite:///{sqlite_path}", index=False, if_exists="replace")

        # Use NL2SQLAgent to ask a sample question
        agent = NL2SQLAgent(sqlite_path)
        sample_question = "Show top 5 rows"
        result, sql_query, explanation = agent.query(sample_question, table_name)

        return {
            "status": "success",
            "message": "File processed. You can now ask questions.",
            "analysis_summary": analysis_summary,
            "sqlite_path": sqlite_path,
            "sample_question": sample_question,
            "sample_answer": result,
            "sql_used": sql_query,
            "explanation": explanation
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Workflow failed: {str(e)}")
        
