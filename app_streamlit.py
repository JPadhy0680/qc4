# qc_twofile_compare_tabular.py
import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, date
import io, re, calendar
from typing import Optional, Dict, Any, List, Tuple, Set

# ---------------- UI setup ----------------
st.set_page_config(page_title="E2B_R3 Two-File Comparator (Tabular, Box-wise)", layout="wide")
st.title("🧪📄📄 E2B_R3 Two‑File Comparator — Tabular, Box‑wise")

# ---------------- Utilities ----------------
NS = {'hl7': 'urn:hl7-org:v3', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
UNKNOWN_TOKENS = {"unk", "asku", "unknown"}

# Admin identifiers
SENDER_ID_OID      = "2.16.840.1.113883.3.989.2.1.3.1"   # Sender ID
WWID_OID           = "2.16.840.1.113883.3.989.2.1.3.2"   # WWID
FIRST_SENDER_OID   = "2.16.840.1.113883.3.989.2.1.1.3"   # First sender of case (1=Regulator, 2=Other)
FIRST_SENDER_MAP   = {"1": "Regulator", "2": "Other"}

# Reporter qualification mapping (as in your triage app)
REPORTER_MAP = {
    "1": "Physician",
    "2": "Pharmacist",
    "3": "Other health professional",
    "4": "Lawyer",
    "5": "Consumer or other non-health professional",
}

# TD priority paths (for Day Zero: Source=TD, Processed=LRD)
TD_PATHS = [
    './/hl7:transmissionWrapper/hl7:creationTime',
    './/hl7:ControlActProcess/hl7:effectiveTime',
    './/hl7:ClinicalDocument/hl7:effectiveTime',
    './/hl7:creationTime',
]

# --- UI styling ---
BOX_CSS = """
<style>
.box {
  border: 1px solid #e0e0e0; border-radius: 8px; padding: 10px 12px; margin: 8px 0;
  background: #fafafa;
}
.box h5 { margin: 0 0 8px 0; }
.smallnote { color:#666; font-size: 0.9em; }
</style>
"""
st.markdown(BOX_CSS, unsafe_allow_html=True)

# ---------------- Small helpers ----------------
def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", (s or "").strip())

def format_date(date_str: str) -> str:
    if not date_str: return ""
    digits = _digits_only(date_str)
    try:
        if len(digits) >= 8:
            return datetime.strptime(digits[:8], "%Y%m%d").strftime("%d-%b-%Y")
        elif len(digits) >= 6:
            return datetime.strptime(digits[:6], "%Y%m").strftime("%b-%Y")
        elif len(digits) >= 4:
            return digits[:4]
    except Exception:
        pass
    return ""

def parse_date_obj(date_str: str) -> Optional[date]:
    if not date_str: return None
    digits = _digits_only(date_str)
    try:
        if len(digits) >= 8:
            return datetime.strptime(digits[:8], "%Y%m%d").date()
        elif len(digits) >= 6:
            y, m = int(digits[:4]), int(digits[4:6])
            last = calendar.monthrange(y, m)[1]
            return date(y, m, last)
        elif len(digits) >= 4:
            y = int(digits[:4]); return date(y, 12, 31)
    except Exception:
        pass
    return None

def clean_value(v: Any) -> str:
    if v is None: return ""
    s = str(v).strip()
    return "" if (not s or s.lower() in UNKNOWN_TOKENS) else s

def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r'[^a-z0-9\s\+\-]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def map_gender(code: str) -> str:
    return {"1":"Male", "2":"Female", "M":"Male", "F":"Female"}.get(code, "Unknown")

def get_text(elem) -> str:
    return clean_value(elem.text) if (elem is not None and elem.text) else ""

def find_first(root, xpath) -> Optional[ET.Element]:
    return root.find(xpath, NS)

def findall(root, xpath) -> List[ET.Element]:
    return root.findall(xpath, NS)

def mismatch_marker(a: Any, b: Any, is_date=False) -> str:
    if is_date:
        da, db = parse_date_obj(a or ""), parse_date_obj(b or "")
        if da == db and da is not None:
            return ""
    return " 🔴" if (str(a) or "") != (str(b) or "") else ""

def has_value(x: str) -> bool:
    return bool((x or "").strip())

def safe_disp(v: str) -> str:
    return v if v else "—"

# ---------------- Canonical extraction ----------------
def extract_id_by_oid(root: ET.Element, oid: str) -> str:
    e = find_first(root, f'.//hl7:id[@root="{oid}"]')
    return clean_value(e.attrib.get('extension', '')) if e is not None else ""

def extract_sender_id(root: ET.Element) -> str:
    return extract_id_by_oid(root, SENDER_ID_OID)

def extract_wwid(root: ET.Element) -> str:
    return extract_id_by_oid(root, WWID_OID)

def extract_first_sender_type(root: ET.Element) -> str:
    for el in root.iter():
        local = el.tag.split('}')[-1] if '}' in el.tag else el.tag
        if local == 'code' and el.attrib.get('codeSystem') == FIRST_SENDER_OID:
            raw = (el.attrib.get('code') or "").strip()
            return FIRST_SENDER_MAP.get(raw, raw or "")
    return ""

def extract_td_frd_lrd(root: ET.Element) -> Dict[str, str]:
    out = {"TD_raw":"", "TD":"", "FRD_raw":"", "FRD":"", "LRD_raw":"", "LRD":""}
    # TD (priority)
    for p in TD_PATHS:
        e = find_first(root, p)
        if e is not None:
            val = e.attrib.get('value') or get_text(e)
            if val:
                out["TD_raw"] = val; out["TD"] = format_date(val)
                break
    # LRD: explicit availabilityTime (first)
    for el in root.iter():
        ln = el.tag.split('}')[-1] if '}' in el.tag else el.tag
        if ln == 'availabilityTime':
            v = el.attrib.get('value')
            if v:
