
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# project paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR   = BASE_DIR / "data"
RAW_DIR    = DATA_DIR / "raw"
CLEAN_DIR  = DATA_DIR / "cleaned"
LOGS_DIR   = DATA_DIR / "logs"

# input files
PATIENTS_FILE   = RAW_DIR / "patients.csv"
ENCOUNTERS_FILE = RAW_DIR / "encounters.csv"
DIAGNOSES_FILE  = RAW_DIR / "diagnoses.xml"

# output files
PATIENTS_CLEAN   = CLEAN_DIR / "patients_clean.csv"
ENCOUNTERS_CLEAN = CLEAN_DIR / "encounters_clean.csv"
DIAGNOSES_CLEAN  = CLEAN_DIR / "diagnoses_clean.csv"

# logs
PATIENTS_LOGS   = LOGS_DIR / "patients_logs.csv"
ENCOUNTERS_LOGS = LOGS_DIR / "encounters_logs.csv"
DIAGNOSES_LOGS  = LOGS_DIR / "diagnoses_logs.csv"

# Database
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise ValueError("Missing required DB environment variables")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"