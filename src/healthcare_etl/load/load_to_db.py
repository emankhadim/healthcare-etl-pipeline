"""
Load cleaned CSV data into PostgreSQL database.
- Assumes CSVs are already cleaned by the transform step.
- No FK checks here. Just coerce types and insert everything present in the clean files.
"""

from __future__ import annotations
import logging
import pandas as pd
from sqlalchemy.orm import Session
from healthcare_etl.core.logging_setup import setup_logging
from healthcare_etl.core.db import get_engine
from healthcare_etl.models import Patient, Encounter, Diagnosis
from healthcare_etl.core.config import PATIENTS_CLEAN, ENCOUNTERS_CLEAN, DIAGNOSES_CLEAN

log = logging.getLogger(__name__)

def _to_bool(val):
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if s in {"true", "1", "yes"}:  return True
    if s in {"false", "0", "no"}:  return False
    return None

def _nan_to_none_dicts(df: pd.DataFrame, cols: list[str]) -> list[dict]:
    """Convert a DataFrame subset to list-of-dicts and replace NaN/NaT with None."""
    out = []
    for rec in df[cols].to_dict("records"):
        out.append({k: (None if pd.isna(v) else v) for k, v in rec.items()})
    return out

def load_patients(session: Session) -> int:
    df = pd.read_csv(PATIENTS_CLEAN)
    log.info("Patients: reading %d rows", len(df))

    if "patient_id" in df.columns:
        df["patient_id"] = df["patient_id"].astype(str).str.strip()
    if "dob" in df.columns:
        df["dob"] = pd.to_datetime(df["dob"], errors="coerce").dt.date
    for col in ("height", "weight"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    allowed = [
        "patient_id", "given_name", "family_name",
        "sex", "dob", "height", "weight",
        "qa_flags", "source_file",
    ]
    cols = [c for c in df.columns if c in allowed]

    objs = [Patient(**rec) for rec in _nan_to_none_dicts(df, cols)]
    session.bulk_save_objects(objs)
    log.info("Patients: inserted %d", len(objs))
    return len(objs)

def load_encounters(session: Session) -> int:
    df = pd.read_csv(ENCOUNTERS_CLEAN)
    log.info("Encounters: reading %d rows", len(df))

    for c in ("encounter_id", "patient_id"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    if "admit_dt" in df.columns:
        df["admit_dt"] = pd.to_datetime(df["admit_dt"], utc=True, errors="coerce")
    if "discharge_dt" in df.columns:
        df["discharge_dt"] = pd.to_datetime(df["discharge_dt"], utc=True, errors="coerce")

    allowed = [
        "encounter_id", "patient_id",
        "admit_dt", "discharge_dt",
        "encounter_type", "encounter_status",
        "qa_flags", "source_file",
    ]
    cols = [c for c in df.columns if c in allowed]

    objs = [Encounter(**rec) for rec in _nan_to_none_dicts(df, cols)]
    session.bulk_save_objects(objs)
    log.info("Encounters: inserted %d", len(objs))
    return len(objs)

def load_diagnoses(session: Session) -> int:
    df = pd.read_csv(DIAGNOSES_CLEAN)
    log.info("Diagnoses: reading %d rows", len(df))

    if "encounter_id" in df.columns:
        df["encounter_id"] = df["encounter_id"].astype(str).str.strip()
    if "recorded_at" in df.columns:
        df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True, errors="coerce")
    if "is_primary" in df.columns:
        df["is_primary"] = df["is_primary"].apply(_to_bool)

    allowed = [
        "encounter_id", "code_system", "diagnosis_code",
        "is_primary", "recorded_at",
        "qa_flags", "source_file",
    ]
    cols = [c for c in df.columns if c in allowed]

    objs = [Diagnosis(**rec) for rec in _nan_to_none_dicts(df, cols)]
    session.bulk_save_objects(objs)
    log.info("Diagnoses: inserted %d", len(objs))
    return len(objs)

def load_all() -> dict:
    setup_logging()
    log.info("Starting DB load")
    engine = get_engine()

    with Session(engine) as session:
        try:
            p_count = load_patients(session)
            e_count = load_encounters(session)
            d_count = load_diagnoses(session)
            session.commit()
            log.info("Load committed successfully")
        except Exception as e:
            session.rollback()
            log.error("Load failed; rolled back: %s", e, exc_info=True)
            raise

    log.info("Load summary: patients=%d, encounters=%d, diagnoses=%d", p_count, e_count, d_count)
    return {"patients": p_count, "encounters": e_count, "diagnoses": d_count}

if __name__ == "__main__":
    load_all()
