"""
Extract patients, raw DataFrame.
"""
import logging
from pathlib import Path
import pandas as pd
from healthcare_etl.core.config import PATIENTS_FILE

log = logging.getLogger(__name__)

def read_patients() -> pd.DataFrame:
    """Read raw patients CSV """
    df = pd.read_csv(PATIENTS_FILE)
    df.columns = df.columns.str.strip()
    df["source_file"] = Path(PATIENTS_FILE).name
    log.info("Extracted patients: %s (%d rows)", PATIENTS_FILE, len(df))
    return df

