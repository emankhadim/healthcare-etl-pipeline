"""
Transform patients: clean fields, validate basics, write tidy CSV and log dropped rows.
"""

from __future__ import annotations
import logging
from healthcare_etl.core.logging_setup import setup_logging
import re
from datetime import datetime
from pathlib import Path
import pandas as pd
from healthcare_etl.extract.extract_patients import read_patients
from healthcare_etl.core.config import (
    PATIENTS_CLEAN,
    LOGS_DIR,
    PATIENTS_LOGS,
)
log = logging.getLogger(__name__)

# constants
TARGET_HEADERS = ["patient_id", "given_name", "family_name", "sex", "dob", "height", "weight"]
ALLOWED_SEX = {"M", "F", "O", "U"}  # Male, Female, Other, Unknown
MISSING_TOKENS = {t.lower() for t in ["", " ", "NA", "N/A", "NULL"]}

HEIGHT_MIN_CM, HEIGHT_MAX_CM = 40, 250
WEIGHT_MIN_KG, WEIGHT_MAX_KG = 3, 300

# helpers
def _normalize_missing(series: pd.Series) -> pd.Series:
    """
    Map common missing tokens (case/space-insensitive) to NaN..
    """
    s = series.astype(str)
    mask = s.str.strip().str.lower().isin(MISSING_TOKENS)
    out = series.copy()
    out[mask] = pd.NA
    return out

def standardize_date(x) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def convert_height_to_cm(h) -> float | None:
    if pd.isna(h):
        return None
    s = str(h).lower().strip()
    nums = re.findall(r"\d+\.?\d*", s)
    if not nums:
        return None
    v = float(nums[0])
    if "ft" in s or "feet" in s:
        inches = float(nums[1]) if len(nums) > 1 else 0.0
        return round((v * 12 + inches) * 2.54, 1)
    if "in" in s or "inch" in s:
        return round(v * 2.54, 1)
    return round(v, 1)

def convert_weight_to_kg(w) -> float | None:
    if pd.isna(w):
        return None
    s = str(w).lower().strip()
    nums = re.findall(r"\d+\.?\d*", s)
    if not nums:
        return None
    v = float(nums[0])
    if "lb" in s or "pound" in s:
        return round(v * 0.453592, 1)
    return round(v, 1)

def normalize_sex(x) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    s = {"MALE": "M", "FEMALE": "F", "UNKNOWN": "U"}.get(s, s)
    return s if s else None

def build_flags(row: pd.Series, today_utc: pd.Timestamp) -> str:
    flags = []

    # DOB checks
    dob = row.get("dob")
    if pd.isna(dob):
        flags.append("MISSING_DOB")
    else:
        dob_ts = pd.to_datetime(dob, errors="coerce", utc=True)
        if pd.isna(dob_ts):
            flags.append("INVALID_DOB")
        else:
            if dob_ts > today_utc:
                flags.append("FUTURE_DOB")
            age = (today_utc - dob_ts).days / 365.25
            if age > 120:
                flags.append("AGE_GT_120Y")

    # Height/Weight presence
    h = row.get("height")
    w = row.get("weight")
    if pd.isna(h):
        flags.append("MISSING_HEIGHT")
    if pd.isna(w):
        flags.append("MISSING_WEIGHT")

    # Outliers
    if pd.notna(h):
        try:
            if not (HEIGHT_MIN_CM <= float(h) <= HEIGHT_MAX_CM):
                flags.append("HEIGHT_OUTLIER")
        except Exception:
            flags.append("HEIGHT_OUTLIER")
    if pd.notna(w):
        try:
            if not (WEIGHT_MIN_KG <= float(w) <= WEIGHT_MAX_KG):
                flags.append("WEIGHT_OUTLIER")
        except Exception:
            flags.append("WEIGHT_OUTLIER")

    # Sex checks
    sx = row.get("sex")
    if pd.isna(sx):
        flags.append("MISSING_SEX")
    else:
        if str(sx).upper() not in ALLOWED_SEX:
            flags.append("INVALID_SEX")

    return "OK" if not flags else "|".join(flags)

# main transform
def main():
    log.info("Loading patients (extract)…")
    df_raw = read_patients()
    df_raw.columns = df_raw.columns.str.strip()

    missing = [c for c in TARGET_HEADERS if c not in df_raw.columns]
    if missing:
        log.warning("Missing expected columns: %s", missing)
    present = [c for c in TARGET_HEADERS if c in df_raw.columns]

    present = present + (["source_file"] if "source_file" in df_raw.columns else [])
    df = df_raw[present].copy()

    if "patient_id" in df.columns:
        df["patient_id"] = df["patient_id"].astype(str).str.strip()

    # duplicate logging
    dup_mask = df.duplicated(subset=["patient_id"], keep="first")
    dup_rows = df.loc[dup_mask].copy()
    if not dup_rows.empty:
        Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
        keep_cols = [c for c in TARGET_HEADERS if c in dup_rows.columns] + ["source_file", "qa_flags"]
        dup_rows = dup_rows.reindex(columns=[c for c in keep_cols if c in dup_rows.columns])
        dup_rows["qa_flags"] = "DUPLICATE_PATIENT_ID"
        dup_rows.to_csv(PATIENTS_LOGS, index=False)
        log.info("Logged duplicates: %s (%d rows)", PATIENTS_LOGS, len(dup_rows))

    # drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["patient_id"], keep="first")
    removed = before - len(df)
    if removed:
        log.warning("Removed %d duplicate patient_id rows", removed)

    for col in ["dob", "height", "weight", "sex"]:
        if col in df.columns:
            df[col] = _normalize_missing(df[col])

    if "given_name" in df.columns:
        df["given_name"] = df["given_name"].astype(str).str.strip().str.title()
    if "family_name" in df.columns:
        df["family_name"] = df["family_name"].astype(str).str.strip().str.title()

    # standardize fields
    if "dob" in df.columns:
        df["dob"] = df["dob"].apply(standardize_date)
    if "height" in df.columns:
        df["height"] = df["height"].apply(convert_height_to_cm)
    if "weight" in df.columns:
        df["weight"] = df["weight"].apply(convert_weight_to_kg)
    if "sex" in df.columns:
        df["sex"] = df["sex"].apply(normalize_sex)

    # QA flags
    today_utc = pd.Timestamp.now(tz="UTC")
    df["qa_flags"] = df.apply(lambda r: build_flags(r, today_utc), axis=1)
    out_cols = [c for c in TARGET_HEADERS if c in df.columns] + ["qa_flags", "source_file"]
    df = df[out_cols]

    Path(PATIENTS_CLEAN).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PATIENTS_CLEAN, index=False)
    log.info("Saved cleaned patients:  %s (%d rows)", PATIENTS_CLEAN, len(df))

if __name__ == "__main__":
    setup_logging()
    main()
