"""
Basic tests for ETL pipeline
"""
import pytest
from sqlalchemy import create_engine, text
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

def test_database_connection():
    """Test that we can connect to the database"""
    db_user = os.getenv("DB_USER", "etl_user")
    db_pass = os.getenv("DB_PASSWORD", "etl_pass")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5433")
    db_name = os.getenv("DB_NAME", "healthcare_db")
    
    conn_str = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1
        print("Database connection successful")
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")

def test_etl_output_files_exist():
    """Test that ETL creates cleaned output files"""
    assert Path('data/cleaned/patients_clean.csv').exists(), "Patients cleaned file not found"
    assert Path('data/cleaned/encounters_clean.csv').exists(), "Encounters cleaned file not found"
    assert Path('data/cleaned/diagnoses_clean.csv').exists(), "Diagnoses cleaned file not found"
    print("All ETL output files exist")

def test_data_loaded_to_database():
    """Test that data was loaded to database tables"""
    db_user = os.getenv("DB_USER", "etl_user")
    db_pass = os.getenv("DB_PASSWORD", "etl_pass")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5433")
    db_name = os.getenv("DB_NAME", "healthcare_db")
    
    conn_str = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(conn_str)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM patients"))
        patient_count = result.fetchone()[0]
        assert patient_count > 0, "No patients in database"
        
        result = conn.execute(text("SELECT COUNT(*) FROM encounters"))
        encounter_count = result.fetchone()[0]
        assert encounter_count > 0, "No encounters in database"
        
        result = conn.execute(text("SELECT COUNT(*) FROM diagnoses"))
        diagnosis_count = result.fetchone()[0]
        assert diagnosis_count > 0, "No diagnoses in database"
        
        print(f"Data loaded: {patient_count} patients, {encounter_count} encounters, {diagnosis_count} diagnoses")