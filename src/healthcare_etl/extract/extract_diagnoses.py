"""
Extract diagnoses from XML, raw DataFrame.
"""

from __future__ import annotations
import logging
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import pandas as pd
from healthcare_etl.core.config import DIAGNOSES_FILE

log = logging.getLogger(__name__)

NS = {"d": "http://example.org/diagnosis"}
ENC_EXTRACT = re.compile(r"^ENC[\s\-_]*([0-9]+)$", re.IGNORECASE)

def _clean(x):
    s = str(x).strip() if x is not None else ""
    return s or None

def _normalize_encounter_id(enc: str | None) -> str | None:
    if not enc:
        return None
    enc = str(enc).strip()
    m = ENC_EXTRACT.match(enc)
    if m:
        return f"ENC-{int(m.group(1)):06d}"
    return enc

def read_diagnoses(xml_path: str | Path = DIAGNOSES_FILE) -> pd.DataFrame:
    xml_path = str(xml_path)
    log.info("Loading diagnoses XML: %s", xml_path)

    tree = ET.parse(xml_path)
    root = tree.getroot()
    src_name = Path(xml_path).name

    rows = []
    for dx in root.findall("d:Diagnosis", NS):
        enc      = _clean(dx.findtext("d:encounterId", None, NS))
        code_el  = dx.find("d:code", NS)
        code_txt = _clean(code_el.text if code_el is not None else None)
        code_sys = _clean(code_el.get("system")) if code_el is not None else "ICD-10"
        is_prim  = _clean(dx.findtext("d:isPrimary", None, NS))
        rec_at   = _clean(dx.findtext("d:recordedAt", None, NS))

        rows.append({
            "encounter_id": _normalize_encounter_id(enc),
            "code_system": (code_sys.upper() if code_sys else "ICD-10"),
            "diagnosis_code": (code_txt.upper() if code_txt else None),
            "is_primary_raw": is_prim,
            "recorded_at_raw": rec_at,
            "source_file": src_name,
        })

    df = pd.DataFrame(rows)
    if "encounter_id" in df.columns:
        df["encounter_id"] = df["encounter_id"].astype(str).str.strip()
    log.info("Extracted %d diagnosis rows", len(df))
    return df
