from data_engine.cleaner import clean_data
from utils.file_parser import parse_file
import os

def clean_file_pipeline(file_path: str, save_cleaned: bool = True) -> dict:
    try:
        df = parse_file(file_path)

        if df is None or df.empty:
            raise ValueError("Parsed dataframe is empty")

        cleaned_path = ""
        if save_cleaned:
            base_name = os.path.basename(file_path)
            cleaned_path = os.path.join("data/cleaned", f"cleaned_{base_name}")

        cleaned_df, saved_path = clean_data(df, output_path=cleaned_path if save_cleaned else None)

        return {
            "status": "success",
            "cleaned_file": saved_path,
            "rows": cleaned_df.shape[0],
            "columns": cleaned_df.shape[1],
            "preview": cleaned_df.head(5).to_dict(orient="records"),
        }

    except Exception as e:
        return {
            "status": "error",
            "cleaned_file": "",
            "rows": 0,
            "columns": 0,
            "preview": [],
            "error": str(e)
        }
