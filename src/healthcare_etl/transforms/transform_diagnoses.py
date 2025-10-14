"""
Transform diagnoses: clean fields, validate basics, validate referential integrity, write tidy CSV and log dropped rows.
"""

from __future__ import annotations
import logging
from healthcare_etl.core.logging_setup import setup_logging
import re
from pathlib import Path
import pandas as pd
from healthcare_etl.extract.extract_diagnoses import read_diagnoses
from healthcare_etl.core.config import DIAGNOSES_FILE, DIAGNOSES_CLEAN, DIAGNOSES_LOGS, LOGS_DIR, ENCOUNTERS_CLEAN

log = logging.getLogger(__name__)

# validation regex
ICD10_RX = re.compile(r"^[A-Z][0-9]{2}(?:\.[0-9A-Z]{1,4})?$", re.IGNORECASE)
ENC_RX   = re.compile(r"^ENC-\d{6}$", re.IGNORECASE)

OUT_COLS = [
    "encounter_id",
    "code_system",
    "diagnosis_code",
    "is_primary",
    "recorded_at",
    "qa_flags",
    "source_file",
]

DROP_LOG_COLS = [
    "encounter_id",
    "code_system",
    "diagnosis_code",
    "is_primary",
    "recorded_at_raw",
    "recorded_at",
    "qa_flags",
    "source_file",
]

def _parse_bool(x):
    if pd.isna(x):
        return None
    s = str(x).strip().lower()
    if s in {"true", "1", "yes"}:  return True
    if s in {"false", "0", "no"}:  return False
    return None

def _to_utc(x):
    if not x or (isinstance(x, float) and pd.isna(x)):
        return pd.NaT
    return pd.to_datetime(x, utc=True, errors="coerce")

def _iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(ts) else None

def _add_flag(df: pd.DataFrame, mask: pd.Series, flag: str) -> None:
    if "qa_flags" not in df.columns:
        df["qa_flags"] = ""
    has = df["qa_flags"].ne("")
    df.loc[mask & ~has, "qa_flags"] = flag
    df.loc[mask &  has, "qa_flags"] = df.loc[mask & has, "qa_flags"] + "|" + flag

def transform(df: pd.DataFrame) -> pd.DataFrame:
    now_utc = pd.Timestamp.now(tz="UTC")
    df = df.copy()
    df["is_primary"] = df["is_primary_raw"].apply(_parse_bool)
    df["recorded_at"] = df["recorded_at_raw"].apply(_to_utc)

    qa = []
    for _, r in df.iterrows():
        f = []
        enc = r.get("encounter_id")
        code = r.get("diagnosis_code")
        ts_raw = r.get("recorded_at_raw")
        ts_parsed = r.get("recorded_at")

        if not enc:
            f.append("MISSING_ENCOUNTER_ID")
        elif not ENC_RX.match(str(enc)):
            f.append("INVALID_ENCOUNTER_ID")

        if not code:
            f.append("MISSING_CODE")
        elif not ICD10_RX.match(str(code)):
            f.append("INVALID_CODE")

        if r.get("is_primary") is None:
            f.append("MISSING_ISPRIMARY")

        if ts_raw and pd.isna(ts_parsed):
            f.append("INVALID_DATE")
        if pd.notna(ts_parsed) and ts_parsed > now_utc:
            f.append("FUTURE_DATE")

        qa.append("|".join(f))
    df["qa_flags"] = qa

    before = len(df)
    df = df.drop_duplicates()
    if len(df) < before:
        log.info("Removed %d exact duplicate rows", before - len(df))

    key = ["encounter_id", "diagnosis_code"]
    vc = df.groupby(key, dropna=False).size()
    dup_keys = set(vc[vc > 1].index)

    df = df.sort_values(key + ["recorded_at"], na_position="last")
    df = df.drop_duplicates(subset=key, keep="first").copy()
    _add_flag(df, df.set_index(key).index.isin(dup_keys), "DUP_DIAGNOSIS_MERGED")

    fatal_tokens = ("MISSING_ENCOUNTER_ID", "INVALID_ENCOUNTER_ID", "FUTURE_DATE", "INVALID_CODE")
    fatal_mask = df["qa_flags"].str.contains("|".join(fatal_tokens), na=False)

    dropped = df[fatal_mask].copy()
    if not dropped.empty:
        Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
        dropped = dropped.reindex(columns=[c for c in DROP_LOG_COLS if c in dropped.columns])
        dropped.to_csv(DIAGNOSES_LOGS, index=False)
        log.warning("Diagnoses: dropped %d rows → %s", len(dropped), DIAGNOSES_LOGS)

    kept = df[~fatal_mask].copy()
    kept.loc[kept["qa_flags"].eq(""), "qa_flags"] = "OK"
    kept["recorded_at"] = kept["recorded_at"].apply(_iso)

    return kept.reindex(columns=OUT_COLS)

def main():
    df_raw = read_diagnoses(DIAGNOSES_FILE)
    df_clean = transform(df_raw)
    
    log.info("Validating referential integrity (encounter_id foreign key)...")
    encounters_df = pd.read_csv(ENCOUNTERS_CLEAN)
    valid_encounter_ids = set(encounters_df['encounter_id'])
    
    fk_violations = df_clean[~df_clean['encounter_id'].isin(valid_encounter_ids)]
    
    if not fk_violations.empty:
        fk_violations = fk_violations.copy()
        fk_violations['qa_flags'] = fk_violations['qa_flags'].apply(
            lambda x: f"{x}|FK_VIOLATION" if x and x != "OK" else "FK_VIOLATION"
        )

        Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
        
        if Path(DIAGNOSES_LOGS).exists():
            existing_log = pd.read_csv(DIAGNOSES_LOGS)
            combined_log = pd.concat([existing_log, fk_violations], ignore_index=True)
        else:
            combined_log = fk_violations
        
        combined_log.to_csv(DIAGNOSES_LOGS, index=False)
        log.warning("Found %d foreign key violations (encounter_id)", len(fk_violations))
        log.warning("Invalid encounter_id references: %s", fk_violations['encounter_id'].unique().tolist())
        df_clean = df_clean[df_clean['encounter_id'].isin(valid_encounter_ids)]
        log.info("Removed FK violations: %d valid records remaining", len(df_clean))
    else:
        log.info("Referential integrity validated ✓")
    
    Path(DIAGNOSES_CLEAN).parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(DIAGNOSES_CLEAN, index=False)
    log.info("Saved cleaned diagnoses: %s (%d rows)", DIAGNOSES_CLEAN, len(df_clean))

if __name__ == "__main__":
    setup_logging() 
    main()