"""
CLI wrapper for Healthcare ETL pipeline. 
Run with:
    python -m healthcare_etl.scripts.run_etl
Or directly:
    python src/healthcare_etl/scripts/run_etl.py
"""
import logging
from healthcare_etl.core.db import create_tables
from healthcare_etl.core.logging_setup import setup_logging
from healthcare_etl.services.etl import run_etl

if __name__ == "__main__":
    setup_logging()
    log = logging.getLogger(__name__)
    
    log.info("Starting Healthcare ETL Pipeline")
    create_tables()
    stats = run_etl()
    
    log.info(f"Pipeline complete: {stats}")