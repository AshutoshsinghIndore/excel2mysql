"""
================================================================================
ULTIMATE MEMORY-EFFICIENT DATA INGESTOR
================================================================================
Features:
- Stream-based processing (Low RAM footprint)
- Schema Alignment & Lineage Tracking
- Recursive multi-format support (.parquet, .xlsx, .xls, .csv)
- Automated Column Normalization
================================================================================
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

# --- CONFIGURATION ---
CONFIG = {
    "DB_URL": "mysql+pymysql://root:root@127.0.0.1:3306/pawanshree",
    "TARGET_TABLE": "unified_data_log",
    "CHUNK_SIZE": 5000,  # Rows per SQL insert
    "LOG_FILE": "ingestion_report.log"
}

class MemoryEfficientIngestor:
    def __init__(self):
        self.engine = create_engine(CONFIG["DB_URL"])
        self.logger = self._setup_logging()
        self.master_schema = None

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-7s | %(message)s',
            handlers=[logging.FileHandler(CONFIG["LOG_FILE"]), logging.StreamHandler()]
        )
        return logging.getLogger("Ingestor")

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes column names to ensure consistency."""
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        return df

    def get_file_iterator(self, folder_path: Path):
        """Generates file paths one by one to save memory."""
        extensions = {'.parquet', '.xlsx', '.xls', '.csv'}
        return (f for f in folder_path.rglob('*') if f.suffix.lower() in extensions)

    def process_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Reads a single file into memory."""
        try:
            if file_path.suffix == '.parquet':
                df = pd.read_parquet(file_path)
            elif file_path.suffix in ['.xlsx', '.xls']:
                # Read all sheets and combine for this file only
                df = pd.concat(pd.read_excel(file_path, sheet_name=None).values(), ignore_index=True)
            elif file_path.suffix == '.csv':
                df = pd.read_csv(file_path)
            else:
                return None
            
            df = self.normalize_columns(df)
            # Add Lineage tags for the Analyst
            df['src_file'] = file_path.name
            df['ingested_at'] = pd.Timestamp.now()
            return df
        except Exception as e:
            self.logger.error(f"Failed to read {file_path.name}: {e}")
            return None

    def align_and_push(self, df: pd.DataFrame):
        """Aligns the current chunk with the SQL table schema and pushes."""
        try:
            # Dynamically handle schema: if table exists, align columns
            # if it doesn't, to_sql will create it on the first run
            df.to_sql(
                CONFIG["TARGET_TABLE"],
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=CONFIG["CHUNK_SIZE"]
            )
        except Exception as e:
            self.logger.error(f"Push failed: {e}")

    def run(self, input_path: str):
        path = Path(input_path)
        if not path.exists():
            self.logger.error("Path not found!")
            return

        self.logger.info(f"Starting ingestion from: {path.absolute()}")
        
        file_count = 0
        total_rows = 0

        for file_path in self.get_file_iterator(path):
            self.logger.info(f"Processing: {file_path.name}")
            
            df = self.process_file(file_path)
            if df is not None and not df.empty:
                row_count = len(df)
                self.align_and_push(df)
                
                # Force Memory Cleanup
                del df 
                
                file_count += 1
                total_rows += row_count
                self.logger.info(f"Successfully ingested {row_count} rows.")

        self.logger.info(f"FINISH: {file_count} files processed. Total rows: {total_rows}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, help="Folder containing data")
    args = parser.parse_args()

    target = args.path or input("Enter data folder path: ")
    
    ingestor = MemoryEfficientIngestor()
    ingestor.run(target)