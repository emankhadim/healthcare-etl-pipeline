"""
ETL service - orchestrates extract, transform, and load operations
"""
import logging
from healthcare_etl.transforms.transform_patients import main as transform_patients
from healthcare_etl.transforms.transform_encounters import main as transform_encounters
from healthcare_etl.transforms.transform_diagnoses import main as transform_diagnoses
from healthcare_etl.load.load_to_db import load_all

log = logging.getLogger(__name__)

def run_etl():
    """Execute the complete ETL pipeline"""
    try:
        log.info("Running transforms...")
        transform_patients()
        transform_encounters()
        transform_diagnoses()
        
        log.info("Loading data to database...")
        stats = load_all()
        
        log.info(f"Pipeline complete: {stats}")
        return stats
        
    except Exception as e:
        log.error(f"ETL pipeline failed: {e}", exc_info=True)
        raise