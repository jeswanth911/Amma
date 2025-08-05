# sql_agent.py

import os
import json
import sqlite3
import logging
import requests
from typing import List, Dict
from dotenv import load_dotenv

load_dotenv()

# Logger setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class NL2SQLAgent:
    def __init__(self, db_path: str, model: str = "mistralai/mistral-7b-instruct:free"):
        self.db_path = db_path
        self.model = model
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise EnvironmentError("❌ OPENROUTER_API_KEY not set in environment variables.")

        self.api_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",  # Required by OpenRouter
            "X-Title": "MyBAI-SQLAgent"
        }

    def get_schema(self) -> str:
        """Extracts the schema from the SQLite DB and returns it as a readable string."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            if not tables:
                raise ValueError("❌ No tables found in the database.")

            schema = ""
            for (table_name,) in tables:
                schema += f"\nTable: {table_name}\n"
                cursor.execute(f"PRAGMA table_info({table_name});")
                for col in cursor.fetchall():
                    schema += f" - {col[1]} ({col[2]})\n"

            return schema.strip()

        except Exception as e:
            logger.error(f"⚠️ Failed to extract schema: {e}")
            raise RuntimeError(f"Schema extraction failed: {e}")
        finally:
            conn.close()

    def generate_prompt(self, question: str, schema: str) -> str:
        """Generates the prompt for the LLM based on question and DB schema."""
        return f"""
You are a senior data analyst. Write a valid SQLite SELECT query only.

## Database Schema:
{schema}

## User Question:
{question}

## Instructions:
- Use only SELECT queries
- Use correct table and column names
- Do not include explanations, markdown, or comments
- Return only raw SQL
"""

    def call_llm(self, prompt: str) -> str:
        """Sends prompt to OpenRouter and extracts SQL from the response."""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You convert natural language into SQL queries."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2,
            "max_tokens": 300
        }

        try:
            logger.info("📤 Sending prompt to OpenRouter API...")
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=30)

            if response.status_code == 400:
                raise RuntimeError(f"❌ 400 Bad Request: {response.text}")
            response.raise_for_status()

            result = response.json()
            content = result["choices"][0]["message"]["content"].strip()

            sql_query = self._extract_sql(content)
            logger.info(f"✅ SQL generated: {sql_query}")
            return sql_query

        except requests.exceptions.RequestException as e:
            logger.error(f"🛑 API Request failed: {e}")
            raise RuntimeError(f"LLM API request failed: {e}")
        except Exception as e:
            logger.error(f"🔴 Error parsing LLM response: {e}")
            raise RuntimeError(f"LLM failed to generate SQL: {e}")

    def _extract_sql(self, content: str) -> str:
        """Extracts raw SQL from LLM response."""
        content = content.strip()
        if content.startswith("```sql"):
            content = content.replace("```sql", "").replace("```", "").strip()

        if not content.lower().startswith("select"):
            raise ValueError("Generated query is not a valid SELECT statement.")

        return content.strip().rstrip(";")

    def execute_sql(self, query: str) -> List[Dict]:
        """Executes SQL against the SQLite DB and returns rows as dictionaries."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"❌ SQL execution error: {e}")
            raise RuntimeError(f"Failed to execute SQL: {e}")
        finally:
            conn.close()

    def ask(self, question: str) -> Dict:
        """Main method: from natural language to final SQL result."""
        try:
            schema = self.get_schema()
            prompt = self.generate_prompt(question, schema)
            sql_query = self.call_llm(prompt)
            results = self.execute_sql(sql_query)

            return {
                "status": "success",
                "question": question,
                "query": sql_query,
                "result": results
            }

        except Exception as e:
            logger.exception("🛑 Failed to answer question.")
            return {
                "status": "error",
                "error": str(e)
            }
            
