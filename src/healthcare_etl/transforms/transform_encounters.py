"""
Transform encounters: clean fields, validate basics, validate referential integrity, write tidy CSV and log dropped rows.
"""

from __future__ import annotations
import logging
import re
from pathlib import Path
import numpy as np
import pandas as pd
from healthcare_etl.core.logging_setup import setup_logging
from healthcare_etl.extract.extract_encounters import read_encounters
from healthcare_etl.core.config import ENCOUNTERS_CLEAN, ENCOUNTERS_LOGS, LOGS_DIR, PATIENTS_CLEAN

log = logging.getLogger(__name__)

TYPE_MAP = {
    "ip": "INPATIENT", "inpatient": "INPATIENT",
    "op": "OUTPATIENT", "outpatient": "OUTPATIENT",
    "ed": "ED", "er": "ED", "emergency": "ED",
}
VALID_TYPES = {"INPATIENT", "OUTPATIENT", "ED"}

def _parse_dt(x):
    if pd.isna(x) or str(x).strip() == "":
        return pd.NaT
    s = str(x).strip()
    try:
        if re.search(r"\d{4}-\d{2}-\d{2}", s):
            return pd.to_datetime(s, utc=True, errors="coerce")
        if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
            return pd.to_datetime(s, utc=True, dayfirst=False, errors="coerce")
        if re.search(r"\d{1,2}-\d{1,2}-\d{2,4}", s):
            return pd.to_datetime(s, utc=True, dayfirst=True, errors="coerce")
        return pd.to_datetime(s, utc=True, errors="coerce")
    except Exception:
        return pd.NaT

def _add_flag(df: pd.DataFrame, mask: pd.Series, flag: str) -> None:
    if "qa_flags" not in df.columns:
        df["qa_flags"] = ""
    has = df["qa_flags"].ne("")
    df.loc[mask & ~has, "qa_flags"] = flag
    df.loc[mask &  has, "qa_flags"] = df.loc[mask & has, "qa_flags"] + "|" + flag

def main():
    log.info("Loading encounters (extract)…")
    df = read_encounters()

    log.info("Starting encounters transform")
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    drop_parts = []

    df["encounter_id"] = (
        df["encounter_id"].astype(str).str.upper()
          .str.replace(r"^ENC[\s\-_]*([0-9]+)$", lambda m: f"ENC-{int(m.group(1)):06d}", regex=True)
    )
    df["patient_id"] = (
        df["patient_id"].astype(str).str.upper()
          .str.replace(r"^P[\s\-_]*([0-9]+)$", lambda m: f"P-{int(m.group(1)):04d}", regex=True)
    )
    df["encounter_type"] = df["encounter_type"].astype(str).str.lower().map(TYPE_MAP).fillna("UNKNOWN")
    df.loc[~df["encounter_type"].isin(VALID_TYPES), "encounter_type"] = "UNKNOWN"
    df["source_file"] = df["source_file"].astype(str).str.replace(r"^.*[\\/]", "", regex=True)

    df["admit_dt_raw"] = df["admit_dt"]
    df["discharge_dt_raw"] = df["discharge_dt"]

    df["admit_dt"] = df["admit_dt"].apply(_parse_dt)
    df["discharge_dt"] = df["discharge_dt"].apply(_parse_dt)

    df["qa_flags"] = ""
    _add_flag(df, df["admit_dt"].isna(),      "MISSING_ADMIT")
    _add_flag(df, df["discharge_dt"].isna(),  "MISSING_DISCHARGE")

    df["los_hours"] = (df["discharge_dt"] - df["admit_dt"]).dt.total_seconds() / 3600.0
    invalid_dates = df["admit_dt"].notna() & df["discharge_dt"].notna() & (df["los_hours"] < 0)
    _add_flag(df, invalid_dates, "DISCHARGE_BEFORE_ADMIT")

    bad = df[invalid_dates].copy()
    if not bad.empty:
        drop_cols = [
            "encounter_id", "patient_id", "encounter_type",
            "admit_dt_raw", "discharge_dt_raw",
            "admit_dt", "discharge_dt",
            "qa_flags", "source_file",
        ]
        drop_parts.append(bad[[c for c in drop_cols if c in bad.columns]])
        df = df[~invalid_dates]
        log.warning("Dropping %d rows with invalid dates (discharge before admit)", len(bad))

    df["encounter_status"] = np.where(df["discharge_dt"].isna(), "OPEN", "CLOSED")

    df.loc[df["qa_flags"].eq(""), "qa_flags"] = "OK"

    df["is_valid_dates"] = ~df["qa_flags"].str.contains("DISCHARGE_BEFORE_ADMIT", na=False)
    df["completeness"] = df[["admit_dt", "discharge_dt", "encounter_type", "patient_id"]].notna().sum(axis=1)

    df_sorted = df.sort_values(
        ["encounter_id", "is_valid_dates", "completeness", "discharge_dt", "source_file"],
        ascending=[True, False, False, False, True],
    )
    survivors = df_sorted.drop_duplicates(subset=["encounter_id"], keep="first").copy()

    removed = df_sorted.loc[~df_sorted.index.isin(survivors.index)].copy()
    if not removed.empty:
        removed = removed.copy()
        removed["qa_flags"] = removed["qa_flags"].replace("", "DEDUP_SURVIVORSHIP")
        removed.loc[removed["qa_flags"].ne("DEDUP_SURVIVORSHIP"), "qa_flags"] += "|DEDUP_SURVIVORSHIP"

        drop_cols = [
            "encounter_id", "patient_id", "encounter_type",
            "admit_dt_raw", "discharge_dt_raw",
            "admit_dt", "discharge_dt",
            "qa_flags", "source_file",
        ]
        drop_parts.append(removed[[c for c in drop_cols if c in removed.columns]])
        log.info("De-dup removed %d rows (kept best per encounter_id)", len(removed))

    dup_ids = df_sorted["encounter_id"][df_sorted.duplicated("encounter_id", keep=False)].unique()
    _add_flag(survivors, survivors["encounter_id"].isin(dup_ids), "DUP_ENCOUNTER_MERGED")

    for c in ["admit_dt", "discharge_dt"]:
        m = survivors[c].notna()
        survivors.loc[m, c] = survivors.loc[m, c].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    out_cols = [
        "encounter_id", "patient_id",
        "admit_dt", "discharge_dt",
        "encounter_type", "encounter_status",
        "qa_flags", "source_file",
    ]
    clean_df = survivors[out_cols].copy()

    log.info("Validating referential integrity (patient_id foreign key)...")
    patients_df = pd.read_csv(PATIENTS_CLEAN)
    valid_patient_ids = set(patients_df['patient_id'])
    
    fk_mask = ~df_sorted['patient_id'].isin(valid_patient_ids)
    fk_violations = df_sorted[fk_mask].copy()
    
    if not fk_violations.empty:
        fk_violations['qa_flags'] = fk_violations['qa_flags'].apply(
            lambda x: f"{x}|FK_VIOLATION" if x and x != "OK" else "FK_VIOLATION"
        )
        
        drop_cols = [
            "encounter_id", "patient_id", "encounter_type",
            "admit_dt_raw", "discharge_dt_raw",
            "admit_dt", "discharge_dt",
            "qa_flags", "source_file",
        ]
        drop_parts.append(fk_violations[[c for c in drop_cols if c in fk_violations.columns]])
        
        log.warning("Found %d foreign key violations (patient_id)", len(fk_violations))
        log.warning("Invalid patient_id references: %s", fk_violations['patient_id'].unique().tolist())
        
        clean_df = clean_df[clean_df['patient_id'].isin(valid_patient_ids)]
        log.info("Removed FK violations: %d valid records remaining", len(clean_df))
    else:
        log.info("Referential integrity validated ✓")
    if drop_parts:
        drops = pd.concat(drop_parts, ignore_index=True)
        Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
        drops.to_csv(ENCOUNTERS_LOGS, index=False)
        log.warning("Wrote drop log: %s (%d rows)", ENCOUNTERS_LOGS, len(drops))
    else:
        log.info("No dropped rows to log")

    log.info("Encounters transform complete: %d rows", len(clean_df))

    Path(ENCOUNTERS_CLEAN).parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_csv(ENCOUNTERS_CLEAN, index=False)
    log.info("Saved cleaned encounters: %s", ENCOUNTERS_CLEAN)

if __name__ == "__main__":
    setup_logging()
    main()