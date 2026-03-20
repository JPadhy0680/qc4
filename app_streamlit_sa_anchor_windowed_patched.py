# qc_twofile_compare_tabular.py

import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, date
import io, re, calendar, zipfile
from typing import Optional, Dict, Any, List, Tuple, Set

# ---------------- UI setup ----------------
st.set_page_config(page_title="📄XML_R3 Comparator📄", layout="wide")
st.title("📄XML_R3 Comparator📄")

# ---------------- Utilities ----------------
NS = {'hl7': 'urn:hl7-org:v3', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
UNKNOWN_TOKENS = {"unk", "asku", "unknown"}

# Admin identifiers
SENDER_ID_OID = "2.16.840.1.113883.3.989.2.1.3.1"  # Sender ID
WWID_OID = "2.16.840.1.113883.3.989.2.1.3.2"       # WWID
FIRST_SENDER_OID = "2.16.840.1.113883.3.989.2.1.1.3"  # First sender of case (1=Regulator, 2=Other)
FIRST_SENDER_MAP = {"1": "Regulator", "2": "Other"}

# Reporter qualification OID
REPORTER_QUAL_OID = "2.16.840.1.113883.3.989.2.1.1.6"
REPORTER_MAP = {
    "1": "Physician",
    "2": "Pharmacist",
    "3": "Other health professional",
    "4": "Lawyer",
    "5": "Consumer or other non-health professional",
}

# Reporter SOURCE anchor OID
REPORT_SOURCE_OID = "2.16.840.1.113883.3.989.2.1.1.22"  # displayName="sourceReport"

# Patient OIDs
AGE_OID = "2.16.840.1.113883.3.989.2.1.1.19"
PATIENT_RECORD_OID = "2.16.840.1.113883.3.989.2.1.3.7"

# ---- Action Taken ----
ACTION_TAKEN_OID = "2.16.840.1.113883.3.989.2.1.1.15"
ACTION_TAKEN_MAP = {
    "1": "Drug withdrawn",
    "2": "Dose reduced",
    "3": "Dose increased",
    "4": "Dose not changed",
    "0": "Unknown",
    "9": "Not applicable",
}

# MedDRA / Clinical section OIDs
MEDDRA_LLT_OID = "2.16.840.1.113883.6.163"               # LLT codes in observations
MH_SECTION_OID = "2.16.840.1.113883.3.989.2.1.1.20"      # clinical sections
STATUS_OID = "2.16.840.1.113883.3.989.2.1.1.19"          # status & flags (causality/intervention/…)
INTERVENTION_CHAR_CODE = "20"
CAUSALITY_CODE = "39"

# TD priority paths (for Day Zero: Source=TD, Processed=LRD)
TD_PATHS = [
    './/hl7:transmissionWrapper/hl7:creationTime',
    './/hl7:ControlActProcess/hl7:effectiveTime',
    './/hl7:ClinicalDocument/hl7:effectiveTime',
    './/hl7:creationTime',
]

# --- UI styling ---
BOX_CSS = """
"""
st.markdown(BOX_CSS, unsafe_allow_html=True)

# ---------------- Small helpers ----------------
def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", (s or "").strip())

def format_date(date_str: str) -> str:
    if not date_str:
        return ""
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
    if not date_str:
        return None
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
    if v is None:
        return ""
    s = str(v).strip()
    return "" if (not s or s.lower() in UNKNOWN_TOKENS) else s

def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r'[^a-z0-9\s+\-]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def map_gender(code: str) -> str:
    return {"1":"Male", "2":"Female", "M":"Male", "F":"Female"}.get(code, "Unknown")

def local_name(tag: str) -> str:
    return tag.split('}')[-1] if '}' in tag else tag

def get_text(elem) -> str:
    return (elem.text or "").strip() if (elem is not None and elem.text) else ""

def read_text_or_mask(elem: Optional[ET.Element]) -> str:
    if elem is None:
        return ""
    if elem.attrib.get('nullFlavor') == 'MSK':
        return "Masked"
    return (elem.text or "").strip()

# ✅ Simple finders with fixed namespace
def find_first(root, xpath, ns=None) -> Optional[ET.Element]:
    return root.find(xpath, NS)

def findall(root, xpath, ns=None) -> List[ET.Element]:
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

# ---------------- Dependency-free XLSX reader ----------------
def _col_letters_to_index(col_letters: str) -> int:
    res = 0
    for ch in col_letters:
        if not ch.isalpha():
            break
        res = res * 26 + (ord(ch.upper()) - ord('A') + 1)
    return res - 1

def _parse_sheet_xml(sheet_xml_bytes: bytes, shared_strings: List[str]) -> pd.DataFrame:
    from xml.etree.ElementTree import fromstring
    root = fromstring(sheet_xml_bytes)
    ns = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    rows = []
    max_col = 0
    for row in root.findall('.//a:sheetData/a:row', ns):
        values: Dict[int, str] = {}
        for c in row.findall('a:c', ns):
            r = c.attrib.get('r', '')
            col_letters = ''.join([ch for ch in r if ch.isalpha()]) or 'A'
            col_idx = _col_letters_to_index(col_letters)
            if col_idx > max_col:
                max_col = col_idx
            value = ""
            t = c.attrib.get('t', '')
            v = c.find('a:v', ns)
            is_node = c.find('a:is', ns)
            if t == 's':
                if v is not None and v.text and v.text.isdigit():
                    ss_idx = int(v.text)
                    if 0 <= ss_idx < len(shared_strings):
                        value = shared_strings[ss_idx]
            elif t == 'inlineStr' and is_node is not None:
                tnode = is_node.find('a:t', ns)
                value = (tnode.text or '') if tnode is not None else ''
            else:
                value = (v.text or '') if v is not None else ''
            values[col_idx] = value
        if values:
            row_list = ["" for _ in range(max_col + 1)]
            for idx, val in values.items():
                if idx <= max_col:
                    row_list[idx] = val
            rows.append(row_list)
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:] if len(rows) > 1 else []
    header = [h if h else f"col_{i+1}" for i, h in enumerate(header)]
    return pd.DataFrame(data, columns=header)

def _read_xlsx_no_openpyxl(uploaded_file) -> pd.DataFrame:
    data = uploaded_file.read()
    zf = zipfile.ZipFile(io.BytesIO(data))
    shared_strings: List[str] = []
    try:
        sst = zf.read('xl/sharedStrings.xml')
        sroot = ET.fromstring(sst)
        s_ns = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        for si in sroot.findall('.//a:si', s_ns):
            parts = []
            for tnode in si.findall('.//a:t', s_ns):
                parts.append(tnode.text or '')
            shared_strings.append(''.join(parts))
    except KeyError:
        shared_strings = []
    sheet_bytes = None
    try:
        sheet_bytes = zf.read('xl/worksheets/sheet1.xml')
    except KeyError:
        for name in zf.namelist():
            if name.startswith('xl/worksheets/') and name.endswith('.xml'):
                sheet_bytes = zf.read(name); break
    if sheet_bytes is None:
        raise ValueError("No worksheet XML found in XLSX.")
    return _parse_sheet_xml(sheet_bytes, shared_strings)

# ---- MedDRA mapping loader ----
def load_meddra_mapping(uploaded_file) -> Dict[str, Dict[str, str]]:
    if not uploaded_file:
        return {}
    fname = (uploaded_file.name or "").lower()
    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        elif fname.endswith(".xlsx"):
            df = _read_xlsx_no_openpyxl(uploaded_file)
        else:
            st.error("Unsupported MedDRA file format. Please upload .xlsx or .csv")
            return {}
    except Exception as e:
        st.error(f"Could not read MedDRA mapping: {e}")
        return {}

    cols = {c.strip().lower(): c for c in df.columns}
    required = ['llt code', 'llt term', 'pt code', 'pt term']
    if not all(k in cols for k in required):
        st.error("MedDRA mapping must contain columns: LLT Code, LLT Term, PT Code, PT Term")
        return {}

    mapping: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        try:
            llt_code = str(row[cols['llt code']]).strip()
            if not llt_code or llt_code.lower() in {"nan", "none"}:
                continue
            mapping[llt_code] = {
                "LLT Term": str(row[cols['llt term']]).strip() if pd.notna(row[cols['llt term']]) else "",
                "PT Code": str(row[cols['pt code']]).strip() if pd.notna(row[cols['pt code']]) else "",
                "PT Term": str(row[cols['pt term']]).strip() if pd.notna(row[cols['pt term']]) else "",
            }
        except Exception:
            continue
    st.success(f"MedDRA Mapping Loaded — {len(mapping):,} LLT rows")
    return mapping

# ---- Value helpers ----
def read_numeric_with_unit(value_node: Optional[ET.Element]) -> str:
    if value_node is None:
        return ""
    v = (value_node.attrib.get('value') or '').strip()
    u = (value_node.attrib.get('unit') or '').strip()
    if v or u:
        return f"{v} {u}".strip()
    center = value_node.find('.//hl7:center', NS)
    if center is not None:
        cv = (center.attrib.get('value') or '').strip()
        cu = (center.attrib.get('unit') or '').strip()
        if cv or cu:
            return f"{cv} {cu}".strip()
    low = value_node.find('.//hl7:low', NS)
    high = value_node.find('.//hl7:high', NS)
    lv = (low.attrib.get('value') or '').strip() if low is not None else ''
    lu = (low.attrib.get('unit') or '').strip() if low is not None else ''
    hv = (high.attrib.get('value') or '').strip() if high is not None else ''
    hu = (high.attrib.get('unit') or '').strip() if high is not None else ''
    if lv or hv:
        lo = f"{lv} {lu}".strip() if (lv or lu) else ""
        hi = f"{hv} {hu}".strip() if (hv or hu) else ""
        return f"{lo} – {hi}".strip(' –')
    return get_text(value_node)

# ---------------- Admin extraction ----------------
def extract_id_by_oid(root: ET.Element, oid: str) -> str:
    e = find_first(root, f'.//hl7:id[@root="{oid}"]')
    return clean_value(e.attrib.get('extension', '')) if e is not None else ""

def extract_sender_id(root: ET.Element) -> str:
    return extract_id_by_oid(root, SENDER_ID_OID)

def extract_wwid(root: ET.Element) -> str:
    return extract_id_by_oid(root, WWID_OID)

def extract_first_sender_type(root: ET.Element) -> str:
    for el in root.iter():
        if local_name(el.tag) == 'code' and el.attrib.get('codeSystem') == FIRST_SENDER_OID:
            raw = (el.attrib.get('code') or "").strip()
            return FIRST_SENDER_MAP.get(raw, raw or "")
    return ""

def extract_td_frd_lrd(root: ET.Element) -> Dict[str, str]:
    out = {"TD_raw":"", "TD":"", "FRD_raw":"", "FRD":"", "LRD_raw":"", "LRD":""}
    # TD
    for p in TD_PATHS:
        e = find_first(root, p)
        if e is not None:
            val = e.attrib.get('value') or get_text(e)
            if val:
                out["TD_raw"] = val; out["TD"] = format_date(val)
                break
    # LRD
    for el in root.iter():
        if local_name(el.tag) == 'availabilityTime':
            v = el.attrib.get('value')
            if v:
                out["LRD_raw"] = v; out["LRD"] = format_date(v); break
    # FRD (earliest <low/>)
    lows = []
    for el in root.iter():
        if local_name(el.tag) == 'low':
            v = el.attrib.get('value')
            if v:
                lows.append(v)
    if lows:
        pairs = [(parse_date_obj(v), v) for v in lows if parse_date_obj(v)]
        if pairs:
            pairs.sort(key=lambda t: t[0])
            out["FRD_raw"] = pairs[0][1]; out["FRD"] = format_date(pairs[0][1])
    return out

# ---------------- Patient extraction ----------------
def get_pq_value_by_code(root: ET.Element, display_name: Optional[str] = None, code_system_oid: Optional[str] = None) -> Tuple[str, str]:
    for obs in root.findall('.//hl7:observation', NS):
        code_el = obs.find('hl7:code', NS)
        if code_el is None:
            continue
        ok = False
        if display_name and (code_el.attrib.get('displayName') or '').strip().lower() == display_name.lower():
            ok = True
        if (not ok) and code_system_oid and (code_el.attrib.get('codeSystem') == code_system_oid):
            ok = True
        if not ok:
            continue
        val_el = obs.find('hl7:value', NS)
        if val_el is None:
            continue
        v = (val_el.attrib.get('value') or '').strip()
        u = (val_el.attrib.get('unit') or '').strip()
        return v, u
    return "", ""

def find_mask_aware_id_by_root(root: ET.Element, oid: str) -> str:
    for el in root.iter():
        if local_name(el.tag) != 'id':
            continue
        if el.attrib.get('root') == oid:
            if el.attrib.get('nullFlavor') == 'MSK':
                return "Masked"
            ext = (el.attrib.get('extension') or '').strip()
            return ext
    return ""

def extract_patient(root: ET.Element) -> Dict[str, str]:
    # Gender
    gender_elem = find_first(root, './/hl7:administrativeGenderCode')
    gender_code = gender_elem.attrib.get('code', '') if gender_elem is not None else ''
    gender = clean_value(map_gender(gender_code))

    # Age
    age_val, age_unit_raw = get_pq_value_by_code(root, display_name="age", code_system_oid=AGE_OID)
    unit_map = {'a': 'year', 'b': 'month'}
    age_unit_label = unit_map.get((age_unit_raw or '').lower(), age_unit_raw or '')
    age = ""
    if clean_value(age_val):
        age = clean_value(age_val)
        if clean_value(age_unit_label):
            age = f"{age} {age_unit_label}"

    # Age Group
    age_group_map = {
        "0":"Foetus","1":"Neonate","2":"Infant","3":"Child",
        "4":"Adolescent","5":"Adult","6":"Elderly"
    }
    ag_elem = find_first(root, './/hl7:code[@displayName="ageGroup"]/../hl7:value')
    age_group = ""
    if ag_elem is not None:
        c = ag_elem.attrib.get('code','')
        nf = ag_elem.attrib.get('nullFlavor','')
        age_group = age_group_map.get(c, "[Masked/Unknown]" if (c in ["MSK","UNK","ASKU","NI"] or nf in ["MSK","UNK","ASKU","NI"]) else "")

    # Weight
    w_el = find_first(root, './/hl7:code[@displayName="bodyWeight"]/../hl7:value')
    w_val = w_el.attrib.get('value','') if w_el is not None else ''
    w_unit = w_el.attrib.get('unit','') if w_el is not None else ''
    if not (w_val or w_unit):
        for obs in root.findall('.//hl7:observation', NS):
            val = obs.find('hl7:value', NS)
            if val is None:
                continue
            u = (val.attrib.get('unit') or '').strip().lower()
            if u in {'kg','lb','lbs'}:
                w_val = (val.attrib.get('value') or '').strip()
                w_unit = (val.attrib.get('unit') or '').strip()
                if w_val:
                    break
    weight = ""
    if clean_value(w_val):
        weight = clean_value(w_val)
        if clean_value(w_unit):
            weight = f"{weight} {w_unit}"

    # Height
    h_el = find_first(root, './/hl7:code[@displayName="height"]/../hl7:value')
    h_val = h_el.attrib.get('value','') if h_el is not None else ''
    h_unit = h_el.attrib.get('unit','') if h_el is not None else ''
    if not (h_val or h_unit):
        for obs in root.findall('.//hl7:observation', NS):
            val = obs.find('hl7:value', NS)
            if val is None:
                continue
            u = (val.attrib.get('unit') or '').strip().lower()
            if u in {'cm','m','in'}:
                h_val = (val.attrib.get('value') or '').strip()
                h_unit = (val.attrib.get('unit') or '').strip()
                if h_val:
                    break
    height = ""
    if clean_value(h_val):
        height = clean_value(h_val)
        if clean_value(h_unit):
            height = f"{height} {h_unit}"

    # Initials (mask-aware)
    initials = ""
    nm = find_first(root, './/hl7:player1/hl7:name')
    if nm is not None:
        if nm.attrib.get('nullFlavor') == 'MSK':
            initials = "Masked"
        else:
            parts = []
            for g in nm.findall('hl7:given', NS):
                if g.text and g.text.strip():
                    parts.append(g.text.strip()[0].upper())
            fam = nm.find('hl7:family', NS)
            if fam is not None and fam.text and fam.text.strip():
                parts.append(fam.text.strip()[0].upper())
            initials = "".join(parts) or clean_value(get_text(nm))
# DOB and DOD
dob_raw = ""
dob_el = find_first(root, './/hl7:birthTime')
if dob_el is not None:
    dob_raw = (dob_el.attrib.get('value') or '').strip()
dob = format_date(dob_raw)

dod_raw = ""
dod_el = find_first(root, './/hl7:deceasedTime')
if dod_el is not None:
    dod_raw = (dod_el.attrib.get('value') or '').strip()
dod = format_date(dod_raw)


    return {
        "Gender": clean_value(gender),
        "Age": clean_value(age),
        "Age Group": clean_value(age_group),
        "Height": clean_value(height),
        "Weight": clean_value(weight),
        "Initials": clean_value(initials),
        "Patient Record Number": clean_value(patient_record_no),
    "DOB": clean_value(dob),
    "DOD": clean_value(dod),
}

# ---------------- Helper: parent map ----------------
def build_parent_map(root: ET.Element) -> Dict[ET.Element, ET.Element]:
    return {c: p for p in root.iter() for c in list(p)}

# ---------------- Reaction map: RID -> LLT term ----------------
def build_reaction_id_to_term(root: ET.Element, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for obs in findall(root, './/hl7:observation'):
        code_el = obs.find('hl7:code', NS)
        if code_el is None or (code_el.attrib.get('displayName') or '').strip().lower() != 'reaction':
            continue
        id_el = find_first(obs, './/hl7:id')
        rid_root = (id_el.attrib.get('root') or '').strip() if id_el is not None else ''
        rid_ext = (id_el.attrib.get('extension') or '').strip() if id_el is not None else ''
        llt_term = ""
        val_el = obs.find('hl7:value', NS)
        llt_code = (val_el.attrib.get('code') or '').strip() if val_el is not None else ''
        if meddra_map and llt_code in meddra_map:
            llt_term = (meddra_map[llt_code].get("LLT Term") or "").strip()
        if not llt_term and val_el is not None:
            llt_term = (val_el.attrib.get('displayName') or '').strip()
        if not llt_term and val_el is not None:
            ot = val_el.find('hl7:originalText', NS)
            if ot is not None and (ot.text or '').strip():
                llt_term = ot.text.strip()
        if llt_term:
            if rid_root:
                out[rid_root] = llt_term
            if rid_ext:
                out[rid_ext] = llt_term
    return out

# ---------------- Medical History extraction ----------------
def extract_medical_history(root: ET.Element, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    pmap = build_parent_map(root)
    anchors: List[ET.Element] = []
    for code in root.findall('.//hl7:code', NS):
        if (code.attrib.get('codeSystem') or '').strip() != MH_SECTION_OID:
            continue
        c = (code.attrib.get('code') or '').strip()
        disp = (code.attrib.get('displayName') or '').strip().lower()
        if c == "1" or disp == "relevantmedicalhistoryandconcurrentconditions":
            anchors.append(code)

    for anc in anchors:
        container = pmap.get(anc, None) or root
        for obs in container.findall('.//hl7:observation', NS):
            code = obs.find('hl7:code', NS)
            if code is None:
                continue
            if (code.attrib.get('codeSystem') or '').strip() != MEDDRA_LLT_OID:
                continue
            llt_code = (code.attrib.get('code') or '').strip()
            llt_term, pt_code, pt_term = "", "", ""
            if meddra_map and llt_code in meddra_map:
                m = meddra_map[llt_code]
                llt_term = (m.get("LLT Term") or "").strip()
                pt_code = (m.get("PT Code") or "").strip()
                pt_term = (m.get("PT Term") or "").strip()
            if not llt_term:
                llt_term = (code.attrib.get('displayName') or '').strip()
            if not llt_term:
                ot = code.find('hl7:originalText', NS)
                if ot is not None and (ot.text or '').strip():
                    llt_term = ot.text.strip()

# ---- Continue + Comment extraction (from STATUS_OID)
mh_continue = ""
mh_comment  = ""
for inb2 in obs.findall('.//hl7:inboundRelationship/hl7:observation', NS):
    sc2 = inb2.find('hl7:code', NS)
    val2 = inb2.find('hl7:value', NS)
    if sc2 is None:
        continue
    cs2 = (sc2.attrib.get('codeSystem') or '').strip()
    cd2 = (sc2.attrib.get('code') or '').strip()
    dn2 = (sc2.attrib.get('displayName') or '').strip().lower()
    # comment -> code '10' or displayName 'comment'
    if cs2 == STATUS_OID and (cd2 == '10' or dn2 == 'comment'):
        if val2 is not None:
            mh_comment = (val2.text or val2.attrib.get('value') or '').strip()
    # continuing -> code '13' or displayName 'continuing'
    if cs2 == STATUS_OID and (cd2 == '13' or dn2 == 'continuing'):
        if val2 is not None:
            raw = (val2.attrib.get('value') or '').strip().lower()
            mh_continue = 'Yes' if raw in {'true','1','yes','y'} else 'No' if raw in {'false','0','no','n'} else (raw or '')

# Dates
            low = obs.find('.//hl7:effectiveTime/hl7:low', NS)
            high = obs.find('.//hl7:effectiveTime/hl7:high', NS)
            sd_raw = (low.attrib.get('value') or '').strip() if low is not None else ''
            ed_raw = (high.attrib.get('value') or '').strip() if high is not None else ''
            sd = format_date(sd_raw)
            ed = format_date(ed_raw)

            key = llt_code or normalize_text(llt_term)
            if not key:
                continue
            items.append({
                "LLT Code": clean_value(llt_code),
                "LLT Term": clean_value(llt_term),
                "Status": ", ".join(statuses) if statuses else "",
                    \"Status (Continue)\": clean_value(mh_continue),
    \"Comment\": clean_value(mh_comment),
"Start Date (raw)": sd_raw,
                "Start Date": sd,
                "End Date (raw)": ed_raw,
                "End Date": ed,
                "_key": key,
            })
    return items

# ---------------- Lab Details extraction ----------------
def extract_labs(root: ET.Element, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    pmap = build_parent_map(root)
    anchors: List[ET.Element] = []
    for code in root.findall('.//hl7:code', NS):
        if (code.attrib.get('codeSystem') or '').strip() != MH_SECTION_OID:
            continue
        c = (code.attrib.get('code') or '').strip()
        disp = (code.attrib.get('displayName') or '').strip().lower()
        if c == "3" or disp == "testsandproceduresrelevanttotheinvestigation":
            anchors.append(code)

    for anc in anchors:
        container = pmap.get(anc, None) or root
        for obs in container.findall('.//hl7:observation', NS):
            code = obs.find('hl7:code', NS)
            if code is None or (code.attrib.get('codeSystem') or '').strip() != MEDDRA_LLT_OID:
                continue
            llt_code = (code.attrib.get('code') or '').strip()
            llt_term = ""
            if meddra_map and llt_code in meddra_map:
                m = meddra_map[llt_code]
                llt_term = (m.get("LLT Term") or "").strip()
            if not llt_term:
                llt_term = (code.attrib.get('displayName') or '').strip()
            if not llt_term:
                ot = code.find('hl7:originalText', NS)
                if ot is not None and (ot.text or '').strip():
                    llt_term = ot.text.strip()

            # Result
            value_node = obs.find('hl7:value', NS)
            result = read_numeric_with_unit(value_node)

            # Result Date
            date_val = ""
            eff = obs.find('hl7:effectiveTime', NS)
            if eff is not None:
                v = (eff.attrib.get('value') or '').strip()
                if v:
                    date_val = format_date(v)
                else:
                    low = eff.find('hl7:low', NS)
                    high = eff.find('hl7:high', NS)
                    lv = (low.attrib.get('value') or '').strip() if low is not None else ''
                    hv = (high.attrib.get('value') or '').strip() if high is not None else ''
                    date_val = format_date(lv or hv)

            key = llt_code or normalize_text(llt_term)
            if not key:
                continue
            items.append({
                "LLT Code": clean_value(llt_code),
                "LLT Term": clean_value(llt_term),
                "Result": clean_value(result),
                "Result Date": clean_value(date_val),
                "_key": key,
            })
    return items

# ---------------- Causality extraction (relaxed + improved assessor) ----------------
def _iter_components_in_doc_order(root: ET.Element) -> List[ET.Element]:
    return findall(root, './/hl7:component[@typeCode="COMP"]')

def _resolve_intervention_label(val_node: Optional[ET.Element]) -> str:
    if val_node is None:
        return ""
    dsn = (val_node.attrib.get('displayName') or '').strip()
    if dsn:
        return dsn
    code = (val_node.attrib.get('code') or '').strip()
    if code:
        return f"code:{code}"
    ot = val_node.find('hl7:originalText', NS)
    if ot is not None and (ot.text or '').strip():
        return ot.text.strip()
    return get_text(val_node)

def _extract_assessor_label(node: ET.Element) -> str:
    cand_texts: List[str] = []
    for xp in [
        './/hl7:author//hl7:assignedEntity//hl7:code/hl7:originalText',
        './/hl7:author//hl7:assignedAuthor//hl7:code/hl7:originalText',
    ]:
        el = find_first(node, xp)
        if el is not None and (el.text or '').strip():
            cand_texts.append(el.text.strip())
    for xp in [
        './/hl7:author//hl7:assignedEntity//hl7:code',
        './/hl7:author//hl7:assignedAuthor//hl7:code',
    ]:
        el = find_first(node, xp)
        if el is not None:
            for attr in ('displayName', 'code'):
                v = (el.attrib.get(attr) or '').strip()
                if v:
                    cand_texts.append(v)
    for t in cand_texts:
        low = t.lower()
        if 'company' in low:
            return "Company"
        if 'reporter' in low:
            return "Reporter"
    nm = find_first(node, './/hl7:author//hl7:assignedEntity//hl7:name')
    if nm is not None and get_text(nm):
        return get_text(nm)
    ot = find_first(node, './/hl7:author//hl7:assignedEntity//hl7:originalText')
    if ot is not None and get_text(ot):
        return get_text(ot)
    return cand_texts[0] if cand_texts else ""

def extract_causality(
    root: ET.Element,
    product_id_to_name: Optional[Dict[str, str]] = None,
    reaction_id_to_term: Optional[Dict[str, str]] = None
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:

# --- De-dup trackers
processed_nodes: Set[int] = set()  # prevent same CA node via nested components
seen_signatures: Set[Tuple[str, str, str, str, str, str]] = set()  # content-level de-dup
        for comp in _iter_components_in_doc_order(root):
            current_intervention = ""  # resets per component
            ca_nodes = comp.findall('.//hl7:causalityAssessment', NS)
            for node in ca_nodes:

# Node-level de-duplication
if id(node) in processed_nodes:
    continue
processed_nodes.add(id(node))
                ccode = node.find('hl7:code', NS)
                if ccode is None:
                    continue
                cs = (ccode.attrib.get('codeSystem') or '').strip()
                cd = (ccode.attrib.get('code') or '').strip()
                dsn = (ccode.attrib.get('displayName') or '').strip().lower()
                if cs != STATUS_OID:
                    continue

                # Intervention sentinel (tracked but not displayed)
                if dsn == 'interventioncharacterization' or cd == INTERVENTION_CHAR_CODE:
                    current_intervention = _resolve_intervention_label(find_first(node, './/hl7:value'))
                    continue

                # Causality rows
                if not (cd == CAUSALITY_CODE or dsn == 'causality'):
                    continue

                # Assessment (xsi:ST text or @value)
                val = find_first(node, './/hl7:value')
                assessment = ""
                if val is not None:
                    assessment = (val.attrib.get('value') or "").strip()
                    if not assessment:
                        assessment = (val.text or "").strip()

                method = get_text(find_first(node, './/hl7:methodCode/hl7:originalText'))
                assessor = _extract_assessor_label(node)

                # IDs -> names
                evt_id = ""
                prd_id = ""
                evt = find_first(node, './/hl7:subject1//hl7:adverseEffectReference//hl7:id')
                if evt is not None:
                    evt_id = (evt.attrib.get('root') or '').strip() or (evt.attrib.get('extension') or '').strip()
                prd = find_first(node, './/hl7:subject2//hl7:productUseReference//hl7:id')
                if prd is not None:
                    prd_id = (prd.attrib.get('root') or '').strip() or (prd.attrib.get('extension') or '').strip()

                reaction_name = reaction_id_to_term.get(evt_id, "") if (reaction_id_to_term and evt_id) else ""
                drug_name = product_id_to_name.get(prd_id, "") if (product_id_to_name and prd_id) else ""
                if not drug_name:
                    sa = comp.find('.//hl7:substanceAdministration', NS)
                    if sa is not None:
                        nm_txt = _resolve_drug_name(sa).strip()
                        if nm_txt:
                            drug_name = nm_txt

                # Pairing key (internal only)
                key = (evt_id or "") + "::" + (prd_id or "")
                if not key.strip(":"):
                    key = "assess::" + normalize_text(assessment or "") + "::" + normalize_text(method or "")
                
# Content-level de-duplication
row_sig = (
    (evt_id or "").strip().lower(),
    (prd_id or "").strip().lower(),
    normalize_text(reaction_name),
    normalize_text(drug_name),
    normalize_text(assessor),
    normalize_text((method or "") + "||" + (assessment or "")),
)
if row_sig in seen_signatures:
    continue
seen_signatures.add(row_sig)

out.append({
                    "Assessment": clean_value(assessment),
                    "Method": clean_value(method),
                    "Assessor": clean_value(assessor),
                    "Reaction": clean_value(reaction_name),
                    "Drug": clean_value(drug_name),
                    "_key": key,
                    "_evt_id": evt_id,
                    "_prd_id": prd_id,
                })
    except Exception as e:
        st.warning(f"Causality parse error: {e}")
    return out

# ---------------- Products extraction (IDs helpers and NEW categorization map) ----------------
def extract_suspect_ids(root: ET.Element) -> Set[str]:
    out = set()
    for c in findall(root, './/hl7:causalityAssessment'):
        sid = find_first(c, './/hl7:subject2/hl7:productUseReference/hl7:id')
        if sid is not None:
            rid = sid.attrib.get('root','')
            if rid:
                out.add(rid)
    return out

def extract_interacting_ids(root: ET.Element) -> Set[str]:
    ids = set()
    for obs in findall(root, './/hl7:observation'):
        code = obs.find('hl7:code', NS)
        disp = (code.attrib.get('displayName') or '').strip().lower() if code is not None else ''
        if 'interact' in disp:
            ref = obs.find('.//hl7:subject2/hl7:productUseReference/hl7:id', NS)
            if ref is not None:
                ids.add(ref.attrib.get('root',''))
    return ids

def extract_treatment_ids(root: ET.Element) -> Set[str]:
    ids = set()
    for obs in findall(root, './/hl7:observation'):
        code = obs.find('hl7:code', NS)
        disp = (code.attrib.get('displayName') or '').strip().lower() if code is not None else ''
        if ('treat' in disp) or ('therapeutic' in disp):
            ref = obs.find('.//hl7:subject2/hl7:productUseReference/hl7:id', NS)
            if ref is not None:
                ids.add(ref.attrib.get('root',''))
    return ids

# ---- Product categorization via causalityAssessment/interventionCharacterization ----
PRODUCT_TYPE_MAP: Dict[str, str] = {
    "1": "Suspect",
    "2": "Concomitant",
    "3": "Interacting",
    "4": "Drug Not Administered",
}

def build_product_type_by_pid(root: ET.Element) -> Dict[str, str]:
    """
    Build a map of product-id-root -> product type label based on:
      <causalityAssessment classCode="OBS" moodCode="EVN">
        <code code="20" codeSystem=STATUS_OID ... displayName="interventionCharacterization"/>
        <value xsi:type="CE" code="1|2|3|4" .../>
        <subject2 typeCode="SUBJ">
          <productUseReference classCode="SBADM" moodCode="EVN">
            <id root="..."/>
          </productUseReference>
        </subject2>
      </causalityAssessment>
    """
    result: Dict[str, str] = {}
    for node in findall(root, './/hl7:causalityAssessment'):
        ccode = node.find('hl7:code', NS)
        if ccode is None:
            continue
        cs = (ccode.attrib.get('codeSystem') or '').strip()
        cd = (ccode.attrib.get('code') or '').strip()
        dsn = (ccode.attrib.get('displayName') or '').strip().lower()
        if cs != STATUS_OID:
            continue
        # Only intervention characterization (code 20 or displayName match)
        if not (cd == INTERVENTION_CHAR_CODE or dsn == 'interventioncharacterization'):
            continue

        # value@code holds 1/2/3/4
        val = find_first(node, './/hl7:value')
        vcode = (val.attrib.get('code') or '').strip() if val is not None else ''
        if not vcode:
            continue

        # subject2/productUseReference/id@root gives the product id
        pid_el = find_first(node, './/hl7:subject2//hl7:productUseReference//hl7:id')
        pid = (pid_el.attrib.get('root') or '').strip() if pid_el is not None else ''
        if not pid:
            continue

        label = PRODUCT_TYPE_MAP.get(vcode, '')
        if label:
            # last one wins if duplicates; typically they should be consistent
            result[pid] = label

    return result

def _resolve_drug_name(admin: ET.Element) -> str:
    nm = find_first(admin, './/hl7:kindOfProduct/hl7:name')
    if nm is not None:
        t = (nm.text or '').strip()
        if t:
            return t
        disp = (nm.attrib.get('displayName') or '').strip()
        if disp:
            return disp
        ot = nm.find('hl7:originalText', NS)
        if ot is not None and (ot.text or '').strip():
            return ot.text.strip()
    alt = find_first(admin, './/hl7:manufacturedProduct/hl7:name')
    if alt is not None and (alt.text or '').strip():
        return alt.text.strip()
    mm = find_first(admin, './/hl7:manufacturedMaterial/hl7:name')
    if mm is not None and (mm.text or '').strip():
        return mm.text.strip()
    mm_code = find_first(admin, './/hl7:manufacturedMaterial/hl7:code')
    if mm_code is not None:
        disp = (mm_code.attrib.get('displayName') or '').strip()
        if disp:
            return disp
    amp = find_first(admin, './/hl7:asManufacturedProduct//hl7:name')
    if amp is not None and (amp.text or '').strip():
        return amp.text.strip()
    return ""



def extract_drug_history(root: ET.Element, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> List[Dict[str, Any]]:
    """
    Parse organizer with codeSystem MH_SECTION_OID and code '2'/displayName 'drugHistory'.
    For each descendant substanceAdministration SBADM/EVN, collect Drug, Indication (code 19), Reaction (code 29), and dates.
    """
    items: List[Dict[str, Any]] = []
    pmap = build_parent_map(root)
    anchors: List[ET.Element] = []
    for code in root.findall('.//hl7:code', NS):
        if (code.attrib.get('codeSystem') or '').strip() != MH_SECTION_OID:
            continue
        c = (code.attrib.get('code') or '').strip()
        disp = (code.attrib.get('displayName') or '').strip().lower()
        if c == '2' or disp == 'drughistory':
            anchors.append(code)

    def _map_llt(llt_code: str, fallback: str = "") -> str:
        if meddra_map and llt_code in meddra_map:
            term = (meddra_map[llt_code].get('LLT Term') or '').strip()
            if term:
                return term
        return fallback or llt_code

    for anc in anchors:
        container = pmap.get(anc, None) or root
        for sa in container.findall('.//hl7:substanceAdministration[@moodCode="EVN"][@classCode="SBADM"]', NS):
            drug = clean_value(_resolve_drug_name(sa))
            low = find_first(sa, './/hl7:effectiveTime/hl7:low')
            high = find_first(sa, './/hl7:effectiveTime/hl7:high')
            sd_raw = (low.attrib.get('value') or '').strip() if (low is not None and low.attrib.get('value')) else ''
            ed_raw = (high.attrib.get('value') or '').strip() if (high is not None and high.attrib.get('value')) else ''
            sd = 'Masked' if (low is not None and (low.attrib.get('nullFlavor') or '').upper() == 'MSK') else format_date(sd_raw)
            ed = 'Masked' if (high is not None and (high.attrib.get('nullFlavor') or '').upper() == 'MSK') else format_date(ed_raw)

            indications: list[str] = []
            reactions: list[str] = []
            for ob in sa.findall('.//hl7:outboundRelationship2/hl7:observation', NS):
                c = ob.find('hl7:code', NS)
                if c is None:
                    continue
                cs = (c.attrib.get('codeSystem') or '').strip()
                cd = (c.attrib.get('code') or '').strip()
                dn = (c.attrib.get('displayName') or '').strip().lower()
                if cs != STATUS_OID:
                    continue
                v = ob.find('hl7:value', NS)
                llt_code = (v.attrib.get('code') or '').strip() if v is not None else ''
                llt_disp = (v.attrib.get('displayName') or '').strip() if (v is not None and (v.attrib.get('displayName') or '').strip()) else ''
                if dn == 'indication' or cd == '19':
                    term = _map_llt(llt_code, llt_disp)
                    if has_value(term) and term not in indications:
                        indications.append(term)
                if dn == 'reaction' or cd == '29':
                    term = _map_llt(llt_code, llt_disp)
                    if has_value(term) and term not in reactions:
                        reactions.append(term)

            key = (normalize_text(drug) or f"sa::{sd_raw}::{ed_raw}")
            if not key:
                continue
            items.append({
                'Drug': drug,
                'Indication': "
".join(indications),
                'Reaction': "
".join(reactions),
                'Start Date (raw)': sd_raw,
                'Start Date': sd,
                'End Date (raw)': ed_raw,
                'End Date': ed,
                '_key': key,
            })
    return items
def _iter_drug_components(root: ET.Element) -> List[ET.Element]:
    comps = []
    for comp in root.findall('.//hl7:component[@typeCode="COMP"]', NS):
        sas = comp.findall('.//hl7:substanceAdministration', NS)
        if sas:
            comps.append(comp)
    return comps

def _add_unique(acc_list: List[str], value: str):
    v = clean_value(value)
    if not v:
        return
    if v not in acc_list:
        acc_list.append(v)

def extract_all_products(root: ET.Element, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> List[Dict[str, Any]]:
    """
    Anchor-windowed extraction. Create exactly ONE product row for each direct-child
    <substanceAdministration SBADM/EVN> that has a non-empty <id@root>.
    For a given anchor SA, we aggregate content from the current SA position up to
    (but not including) the next anchor SA within the SAME <component>.
    Non-anchored SAs (no id@root) DO NOT create their own rows; their content is
    absorbed into the current anchor's window.
    """
    suspects = extract_suspect_ids(root)
    interact = extract_interacting_ids(root)
    treatments = extract_treatment_ids(root)

    # NEW: build categorization map from causalityAssessment/interventionCharacterization
    product_type_by_pid = build_product_type_by_pid(root)

    out: List[Dict[str, Any]] = []
    comps = _iter_drug_components(root)

    for cidx, comp in enumerate(comps, start=1):
        comp_children = list(comp)

        # Identify direct-child SAs at this component level
        sa_positions = []  # list of (pos_index, sa_elem)
        for i, child in enumerate(comp_children):
            if local_name(child.tag) == 'substanceAdministration' and \
               child.attrib.get('moodCode') == 'EVN' and child.attrib.get('classCode') == 'SBADM':
                sa_positions.append((i, child))

        # Build list of ANCHORS = SA that have an id@root
        anchors = []  # list of (pos_index, sa_elem, pid)
        for pos, sa in sa_positions:
            id_el = find_first(sa, './/hl7:id')
            pid = (id_el.attrib.get('root') or '').strip() if id_el is not None else ''
            if pid:
                anchors.append((pos, sa, pid))
        if not anchors:
            # Only anchors produce boxes.
            continue

        # Sort anchors by document order position
        anchors.sort(key=lambda t: t[0])

        for a_idx, (pos, sa_anchor, pid) in enumerate(anchors, start=1):
            # Define window: from this anchor position to next anchor position (exclusive), else to end
            start_pos = pos
            end_pos = anchors[a_idx][0] if a_idx < len(anchors) else len(comp_children)
            window_nodes = comp_children[start_pos:end_pos]

            # Helper: search inside window
            def win_findall(xpath: str) -> List[ET.Element]:
                acc: List[ET.Element] = []
                for wn in window_nodes:
                    acc.extend(wn.findall(xpath, NS))
                return acc

            # ---- Resolve title from the ANCHOR SA only (stable name), fallback to pid
            title = clean_value(_resolve_drug_name(sa_anchor)) or pid

            # ---- Type from causalityAssessment/interventionCharacterization if available
            type_disp = product_type_by_pid.get(pid, "")

            # Fallback to previous heuristics when not available
            if not type_disp:
                tags = set()
                if pid in suspects: tags.add('Suspect')
                if pid in interact: tags.add('Interacting')
                if pid in treatments: tags.add('Treatment')
                if not tags: tags.add('Concomitant')
                type_disp = ', '.join(sorted(tags))

            # ---- Aggregate SA-native fields across ALL SA nodes within the window
            dosage_texts: List[str] = []
            dose_vals: List[str] = []
            dose_units: List[str] = []
            start_dates: List[str] = []
            stop_dates: List[str] = []
            routes: List[str] = []
            forms: List[str] = []
            lots: List[str] = []
            mahs: List[str] = []

            # consider every SA SBADM/EVN inside the window (including anchor)
            for wn in window_nodes:
                if local_name(wn.tag) != 'substanceAdministration':
                    # still check for nested SAs under this window node
                    sas = wn.findall('.//hl7:substanceAdministration[@moodCode="EVN"][@classCode="SBADM"]', NS)
                else:
                    sas = [wn]
                for sa in sas:
                    # dosage text
                    txt = get_text(find_first(sa, './/hl7:text'))
                    _add_unique(dosage_texts, txt)
                    # dose
                    dq = find_first(sa, './/hl7:doseQuantity')
                    if dq is not None:
                        _add_unique(dose_vals, (dq.attrib.get('value') or '').strip())
                        _add_unique(dose_units, (dq.attrib.get('unit') or '').strip())
                    # dates
                    low = find_first(sa, './/hl7:effectiveTime/hl7:low')
                    high = find_first(sa, './/hl7:effectiveTime/hl7:high')
                    sd = format_date((low.attrib.get('value') or '').strip() if low is not None else '')
                    ed = format_date((high.attrib.get('value') or '').strip() if high is not None else '')
                    _add_unique(start_dates, sd)
                    _add_unique(stop_dates, ed)
                    # route
                    rtxt = get_text(find_first(sa, './/hl7:routeCode/hl7:originalText'))
                    if not rtxt:
                        rc = find_first(sa, './/hl7:routeCode')
                        rtxt = (rc.attrib.get('displayName') or '').strip() if rc is not None else ''
                    _add_unique(routes, rtxt)
                    # form
                    form = get_text(find_first(sa, './/hl7:formCode/hl7:originalText'))
                    _add_unique(forms, form)
                    # lot
                    lot = get_text(find_first(sa, './/hl7:lotNumberText'))
                    _add_unique(lots, lot)
                    # MAH
                    mah = ''
                    for xp in [
                        './/hl7:playingOrganization/hl7:name',
                        './/hl7:manufacturerOrganization/hl7:name',
                        './/hl7:asManufacturedProduct/hl7:manufacturerOrganization/hl7:name',
                    ]:
                        node = find_first(sa, xp)
                        if node is not None and get_text(node):
                            mah = get_text(node); break
                    _add_unique(mahs, mah)

            # ---- Window-scoped observations (Action Taken, Obtain Country, Indication)
            action_taken_vals: List[str] = []
            for act_code in win_findall('.//hl7:act[@classCode="ACT"][@moodCode="EVN"]/hl7:code'):
                if (act_code.attrib.get('codeSystem') or '').strip() == ACTION_TAKEN_OID:
                    c = (act_code.attrib.get('code') or '').strip()
                    label = ACTION_TAKEN_MAP.get(c, c or '')
                    _add_unique(action_taken_vals, label)
            action_taken = '\n'.join([v for v in action_taken_vals if has_value(v)])

            obtain_countries: List[str] = []
            for cn in win_findall('.//hl7:country'):
                val = (cn.text or '').strip()
                _add_unique(obtain_countries, val)
            obtain_country = '\n'.join([v for v in obtain_countries if has_value(v)])

            indications: List[str] = []
            for ind_obs in win_findall('.//hl7:observation'):
                code_el = ind_obs.find('hl7:code', NS)
                if code_el is None:
                    continue
                cs = (code_el.attrib.get('codeSystem') or '').strip()
                cd = (code_el.attrib.get('code') or '').strip()
                dn = (code_el.attrib.get('displayName') or '').strip().lower()
                if cs == STATUS_OID and (dn == 'indication' or cd == '19'):
                    val = ind_obs.find('hl7:value', NS)
                    if val is not None:
                        llt_code = (val.attrib.get('code') or '').strip()
                        rrt = ''
                        ot = val.find('hl7:originalText', NS)
                        if ot is not None and (ot.text or '').strip():
                            rrt = ot.text.strip()
                        llt_display = ''
                        if meddra_map and llt_code in meddra_map:
                            llt_display = (meddra_map[llt_code].get('LLT Term') or '').strip()
                        if not llt_display:
                            llt_display = llt_code
                        frag = f'Indication: RRT: {rrt}; LLT: {llt_display}'.strip()
                        _add_unique(indications, frag)
            indication_txt = '\n'.join([v for v in indications if has_value(v)])

            # Joiners
            def join_vals(lst: List[str]) -> str:
                return "\n".join([v for v in lst if has_value(v)])

            out.append({
                'Drug': title,
                'Type': type_disp,
                'Dosage Text': join_vals(dosage_texts),
                'Dose Value': join_vals(dose_vals),
                'Dose Unit': join_vals(dose_units),
                'Start Date': join_vals(start_dates),
                'Stop Date': join_vals(stop_dates),
                'Route': join_vals(routes),
                'Formulation': join_vals(forms),
                'Lot No': join_vals(lots),
                'MAH': join_vals(mahs),
                'Action Taken': action_taken,
                'Drug Obtain Country': obtain_country,
                'Indication': indication_txt,
                '_gid': f"pid::{pid.lower()}",
                '_pid': pid,
            })
    return out

# ---------------- Reporter extraction (strict: sourceReport branches) ----------------
def find_all_source_report_containers(root: ET.Element) -> List[ET.Element]:
    code_nodes: List[ET.Element] = []
    for el in root.iter():
        if local_name(el.tag) == 'code' and el.attrib.get('codeSystem') == REPORT_SOURCE_OID:
            if (el.attrib.get('displayName') or '').strip().lower() == 'sourcereport':
                code_nodes.append(el)
    if not code_nodes:
        return []

    parent = build_parent_map(root)

    def ancestors(node: ET.Element) -> List[ET.Element]:
        acc = []
        cur = node
        while cur in parent:
            cur = parent[cur]
            acc.append(cur)
        return acc

    containers: List[ET.Element] = []
    for code_el in code_nodes:
        for anc in ancestors(code_el):
            lname = local_name(anc.tag)
            if lname in {'relatedInvestigation', 'subjectOf2', 'controlActEvent'}:
                for xp in [
                    './/hl7:author/hl7:assignedEntity',
                    './/hl7:author/hl7:assignedAuthor',
                    './/hl7:informant/hl7:assignedEntity',
                ]:
                    cand = anc.find(xp, NS)
                    if cand is not None:
                        containers.append(cand)
                        break
                break

    seen, uniq = set(), []
    for el in containers:
        if id(el) not in seen:
            seen.add(id(el)); uniq.append(el)
    return uniq

def extract_reporter_from_container(node: ET.Element) -> Dict[str, str]:
    result = {
        "Reporter Qualification": "",
        "Reporter IDs": "",
        "Reporter Title": "",
        "Reporter Given Name(s)": "",
        "Reporter Family Name": "",
        "Reporter Organization": "",
        "Reporter Street": "",
        "Reporter City/Town": "",
        "Reporter State/Province": "",
        "Reporter Postal Code": "",
        "Reporter Country": "",
        "Reporter Phone(s)": "",
        "Reporter Email(s)": "",
        "Reporter Fax(es)": "",
    }

    # IDs
    ids = []
    for id_el in node.findall('.//hl7:id', NS):
        ext = (id_el.attrib.get('extension') or '').strip()
        rt = (id_el.attrib.get('root') or '').strip()
        if ext and rt:
            ids.append(f"{ext} ({rt})")
        elif ext:
            ids.append(ext)
        elif rt:
            ids.append(rt)
    if ids:
        result["Reporter IDs"] = "; ".join(dict.fromkeys(ids))

    # Qualification
    qual = ""
    for code_el in node.iter():
        if local_name(code_el.tag) == 'code' and code_el.attrib.get('codeSystem') == REPORTER_QUAL_OID:
            c = (code_el.attrib.get('code') or '').strip()
            qual = REPORTER_MAP.get(c, c); break
    result["Reporter Qualification"] = qual

    # Name parts (mask-aware)
    name_el = node.find('.//hl7:assignedPerson/hl7:name', NS) or node.find('.//hl7:name', NS)
    title_vals, given_vals, family_val = [], [], ""
    if name_el is not None:
        for pfx in name_el.findall('hl7:prefix', NS):
            v = read_text_or_mask(pfx)
            if v:
                title_vals.append(v)
        for g in name_el.findall('hl7:given', NS):
            v = read_text_or_mask(g)
            if v:
                given_vals.append(v)
        fam_el = name_el.find('hl7:family', NS)
        family_val = read_text_or_mask(fam_el)
    result["Reporter Title"] = "; ".join(title_vals) if title_vals else ""
    result["Reporter Given Name(s)"] = "; ".join(given_vals) if given_vals else ""
    result["Reporter Family Name"] = family_val

    # Organization
    for xp in [
        './/hl7:assignedEntity/hl7:representedOrganization/hl7:name',
        './/hl7:representedOrganization/hl7:name',
        './/hl7:scopingOrganization/hl7:name',
    ]:
        el = node.find(xp, NS)
        if el is not None:
            txt = read_text_or_mask(el)
            if txt:
                result["Reporter Organization"] = txt
                break

    # Address
    addr = node.find('.//hl7:addr', NS)
    streets, city, state, postal, country = [], "", "", "", ""
    if addr is not None:
        for sl in addr.findall('hl7:streetAddressLine', NS):
            val = read_text_or_mask(sl)
            if val:
                streets.append(val)
        city = read_text_or_mask(addr.find('hl7:city', NS))
        state = read_text_or_mask(addr.find('hl7:state', NS))
        postal = read_text_or_mask(addr.find('hl7:postalCode', NS))
        country= read_text_or_mask(addr.find('hl7:country', NS))
    if not country:
        loc = node.find('.//hl7:asLocatedEntity/hl7:location/hl7:code', NS)
        if loc is not None and loc.attrib.get('code'):
            country = loc.attrib.get('code').strip()
    result["Reporter Street"] = ", ".join(streets)
    result["Reporter City/Town"] = city
    result["Reporter State/Province"] = state
    result["Reporter Postal Code"] = postal
    result["Reporter Country"] = country

    # Telecoms
    phones, emails, faxes = [], [], []
    for tel in node.findall('.//hl7:telecom', NS):
        raw = (tel.attrib.get('value') or '').strip()
        use = (tel.attrib.get('use') or '').upper()
        if not raw:
            continue
        low = raw.lower()
        if low.startswith('mailto:'):
            emails.append(raw.split(':', 1)[1])
        elif 'FAX' in use or low.startswith('fax:'):
            faxes.append(raw.split(':', 1)[-1] if ':' in raw else raw)
        elif low.startswith('tel:') or low.startswith('tel;'):
            phones.append(raw.split(':', 1)[1] if ':' in raw else raw)
        else:
            if '@' in raw:
                emails.append(raw.replace('mailto:', ''))
            else:
                digits = re.sub(r'\D','', raw)
                if len(digits) >= 7:
                    phones.append(raw)
                else:
                    phones.append(raw)
    if phones:
        result["Reporter Phone(s)"] = "; ".join(dict.fromkeys(phones))
    if emails:
        result["Reporter Email(s)"] = "; ".join(dict.fromkeys(emails))
    if faxes:
        result["Reporter Fax(es)"] = "; ".join(dict.fromkeys(faxes))
    return result

def extract_reporters_from_sourceReport(root: ET.Element) -> List[Dict[str, str]]:
    containers = find_all_source_report_containers(root)
    reporters: List[Dict[str, str]] = []
    for node in containers:
        rep = extract_reporter_from_container(node)
        if any(clean_value(v) for v in rep.values()):
            reporters.append(rep)
    return reporters

# ---------------- Events extraction ----------------
def extract_events(root: ET.Element, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> List[Dict[str, Any]]:
    seriousness_map = {
        "resultsInDeath": "Death",
        "isLifeThreatening": "LT",
        "requiresInpatientHospitalization": "Hospital",
        "resultsInPersistentOrSignificantDisability": "Disability",
        "congenitalAnomalyBirthDefect": "Congenital",
        "otherMedicallyImportantCondition": "IME"
    }
    outcome_map = {
        "1": "Recovered/Resolved",
        "2": "Recovering/Resolving",
        "3": "Not recovered/Ongoing",
        "4": "Recovered with sequelae",
        "5": "Fatal",
        "0": "Unknown"
    }
    out: List[Dict[str, Any]] = []
    try:
        rxns = findall(root, './/hl7:observation')
        for rxn in rxns:
            code_el = rxn.find('hl7:code', NS)
            if code_el is None or (code_el.attrib.get('displayName') or '').strip().lower() != 'reaction':
                continue

            # Raw LLT code (from value/@code)
            val_el = rxn.find('hl7:value', NS)
            llt_code = (val_el.attrib.get('code') or '').strip() if val_el is not None else ''

            # Decide what to display for the Event term:
            # - If mapping is present and code found -> show LLT Term
            # - Else -> show LLT Code
            event_term = ""
            if meddra_map and llt_code in meddra_map:
                event_term = (meddra_map[llt_code].get("LLT Term") or "").strip()
            if not event_term and val_el is not None:
                event_term = (val_el.attrib.get('displayName') or '').strip() or event_term
                if not event_term:
                    ot = val_el.find('hl7:originalText', NS)
                    if ot is not None and (ot.text or '').strip():
                        event_term = ot.text.strip()
            if not event_term:
                event_term = llt_code

            # --- RRT from value/originalText
            rrt_term = ""
            if val_el is not None:
                ot = val_el.find('hl7:originalText', NS)
                if ot is not None and (ot.text or '').strip():
                    rrt_term = ot.text.strip()

            # Seriousness flags
            flags: List[str] = []
            for crit, label in seriousness_map.items():
                crit_el = rxn.find(f'.//hl7:code[@displayName="{crit}"]/../hl7:value', NS)
                if crit_el is not None and (crit_el.attrib.get('value') or '').strip().lower() == 'true':
                    flags.append(label)
            seriousness_disp = "Non-serious" if not flags else ", ".join(sorted(set(flags)))

            # Outcome
            outcome_el = rxn.find('.//hl7:code[@displayName="outcome"]/../hl7:value', NS)
            outcome_code = (outcome_el.attrib.get('code') or '').strip() if outcome_el is not None else ''
            outcome = outcome_map.get(outcome_code, "Unknown" if outcome_code else "")

            # Dates
            low = rxn.find('.//hl7:effectiveTime/hl7:low', NS)
            high= rxn.find('.//hl7:effectiveTime/hl7:high', NS)
            start_raw = (low.attrib.get('value') or '').strip() if low is not None else ''
            end_raw = (high.attrib.get('value') or '').strip() if high is not None else ''
            start_disp= format_date(start_raw)
            end_disp = format_date(end_raw)

            # Country from location/.../code@code
            country = ""
            loc_code = rxn.find('.//hl7:location//hl7:locatedPlace//hl7:code', NS)
            if loc_code is not None and (loc_code.attrib.get('code') or '').strip():
                country = loc_code.attrib.get('code').strip()

            # Translation Term (displayName='reactionForTranslation' or code='30')
            translation_term = ""
            for ob in rxn.findall('.//hl7:outboundRelationship2[@typeCode="PERT"]/hl7:observation', NS):
                c = ob.find('hl7:code', NS)
                if c is None:
                    continue
                if (c.attrib.get('codeSystem') or '').strip() == STATUS_OID and \
                   ((c.attrib.get('displayName') or '').strip().lower() == 'reactionfortranslation' or (c.attrib.get('code') or '').strip() == '30'):
                    v = ob.find('hl7:value', NS)
                    if v is not None and (v.text or '').strip():
                        translation_term = v.text.strip()
                        break

            # Term highlighted by reporter (displayName='termHighlightedByReporter' or code='37')
            highlighted = ""
            for ob in rxn.findall('.//hl7:outboundRelationship2[@typeCode="PERT"]/hl7:observation', NS):
                c = ob.find('hl7:code', NS)
                if c is None:
                    continue
                if (c.attrib.get('codeSystem') or '').strip() == STATUS_OID and \
                   ((c.attrib.get('displayName') or '').strip().lower() == 'termhighlightedbyreporter' or (c.attrib.get('code') or '').strip() == '37'):
                    v = ob.find('hl7:value', NS)
                    code_val = (v.attrib.get('code') or '').strip() if v is not None else ''
                    if code_val == '1':
                        highlighted = "Yes"
                    elif code_val == '0':
                        highlighted = "No"
                    else:
                        highlighted = code_val
                    break

            # Stable key
            key = normalize_text(event_term) or normalize_text(rrt_term) or clean_value(llt_code)
            if not key:
                continue
            out.append({
                "Event Term": clean_value(event_term),
                "RRT": clean_value(rrt_term),
                "Country": clean_value(country),
                "Translation Term": clean_value(translation_term),
                "Highlighted by Reporter": clean_value(highlighted),
                "Seriousness": seriousness_disp,
                "Outcome": clean_value(outcome),
                "Event Start (raw)": start_raw,
                "Event Start": start_disp,
                "Event End (raw)": end_raw,
                "Event End": end_disp,
                "_key": key,
            })
        return out
    except Exception as e:
        st.warning(f"Events parse error: {e}")
        return out

# ---------------- Narrative extraction ----------------
def extract_narrative(root: ET.Element) -> str:
    narrative_elem = root.find('.//hl7:code[@code="PAT_ADV_EVNT"]/../hl7:text', NS)
    txt = narrative_elem.text if narrative_elem is not None else ''
    return clean_value(txt)

# ---------------- Model builder ----------------
def extract_model(xml_bytes: bytes, meddra_map: Optional[Dict[str, Dict[str,str]]] = None) -> Dict[str, Any]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        return {"_error": f"XML parse error: {e}"}

    model: Dict[str, Any] = {}
    model["Sender ID"] = extract_sender_id(root)
    model["WWID"] = extract_wwid(root)
    model["First Sender Type"] = extract_first_sender_type(root)
    model.update(extract_td_frd_lrd(root))

    model["Reporters"] = extract_reporters_from_sourceReport(root)
    model["Patient"] = extract_patient(root)
    model["MedicalHistory"] = extract_medical_history(root, meddra_map=meddra_map)
    model[\"DrugHistory\"]   = extract_drug_history(root, meddra_map=meddra_map)
    model["LabDetails"] = extract_labs(root, meddra_map=meddra_map)

    products = extract_all_products(root, meddra_map=meddra_map)  # pass map for Indication rule
    model["Products"] = products

    model["Events"] = extract_events(root, meddra_map=meddra_map)

    product_id_to_name = {
        (p.get("_pid") or "").strip(): (p.get("Drug") or "").strip()
        for p in products if (p.get("_pid") or "").strip()
    }
    reaction_id_to_term = build_reaction_id_to_term(root, meddra_map=meddra_map)
    model["Causality"] = extract_causality(
        root,
        product_id_to_name=product_id_to_name,
        reaction_id_to_term=reaction_id_to_term
    )

    model["Narrative"] = extract_narrative(root)
    return model

# --------------- Table builders ----------------
def compare_table(rows: List[Tuple[str, str, str]], treat_as_dates: bool = False) -> pd.DataFrame:
    disp = []
    for field, s, p in rows:
        s_str, p_str = (s or "").strip(), (p or "").strip()
        if not s_str and not p_str:
            continue
        marker = mismatch_marker(s, p, is_date=treat_as_dates)
        disp.append({"Field": field, "Source": safe_disp(s_str), "Processed": safe_disp(p_str) + marker})
    return pd.DataFrame(disp) if disp else pd.DataFrame(columns=["Field","Source","Processed"])

def make_admin_table(src: Dict[str,Any], prc: Dict[str,Any]) -> pd.DataFrame:
    rows: List[Tuple[str, str, str]] = []
    rows.append(("Sender ID", src.get("Sender ID",""), prc.get("Sender ID","")))
    rows.append(("WWID", src.get("WWID",""), prc.get("WWID","")))
    rows.append(("First Sender Type", src.get("First Sender Type",""), prc.get("First Sender Type","")))
    src_td_disp = src.get("TD", "") or format_date(src.get("TD_raw", ""))
    prc_lrd_disp = prc.get("LRD", "") or format_date(prc.get("LRD_raw", ""))
    rows.append(("Day Zero", src_td_disp, prc_lrd_disp))

    parts = [
        compare_table([rows[0]], treat_as_dates=False),
        compare_table([rows[1]], treat_as_dates=False),
        compare_table([rows[2]], treat_as_dates=False),
        compare_table([rows[3]], treat_as_dates=True),
    ]
    parts = [df for df in parts if not df.empty]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["Field","Source","Processed"])

def make_reporter_pair_table(src_rep: Dict[str,str], prc_rep: Dict[str,str]) -> pd.DataFrame:
    fields = [
        "Reporter Qualification","Reporter IDs","Reporter Title","Reporter Given Name(s)","Reporter Family Name",
        "Reporter Organization","Reporter Street","Reporter City/Town","Reporter State/Province",
        "Reporter Postal Code","Reporter Country","Reporter Phone(s)","Reporter Email(s)","Reporter Fax(es)",
    ]
    rows = [(f, src_rep.get(f,""), prc_rep.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

def make_patient_table(src_pat: Dict[str,str], prc_pat: Dict[str,str]) -> pd.DataFrame:
    fields = ["Gender","Age","Age Group","Height","Weight","Initials","Patient Record Number","DOB","DOD"]
    rows = [(f, src_pat.get(f,""), prc_pat.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

# ------ Drug UI helpers ------
def _drug_match_key(rec: Dict[str, Any]) -> str:
    title = (rec.get("Drug") or "").strip()
    if title:
        return f"name::{normalize_text(title)}"
    pid = (rec.get("_pid") or "").strip().lower()
    if pid:
        return f"pid::{pid}"
    gid = (rec.get("_gid") or "").strip().lower()
    return gid or "unknown"

def index_by_match_key(products: List[Dict[str,Any]]) -> Dict[str, Dict[str,Any]]:
    idx: Dict[str, Dict[str,Any]] = {}
    for rec in products:
        key = _drug_match_key(rec)
        if key and key not in idx:
            idx[key] = rec
    return idx

def make_drug_compare_table(src_rec: Dict[str,Any], prc_rec: Dict[str,Any]) -> pd.DataFrame:
    fields = [
        "Type","Dosage Text","Dose Value","Dose Unit","Start Date","Stop Date",
        "Route","Formulation","Lot No","MAH","Action Taken","Drug Obtain Country","Indication",
    ]
    rows = [(f, src_rec.get(f,""), prc_rec.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

# --------------- UI: Upload & Parse ----------------
st.markdown("### 📤 Upload the XML files to compare (and optional MedDRA File)")
col1, col2 = st.columns(2)
with col1:
    src_file = st.file_uploader("Source XML", type=["xml"], key="src_xml")
with col2:
    prc_file = st.file_uploader("Processed XML", type=["xml"], key="prc_xml")

mapping_file = st.file_uploader("MedDRA File", type=["xlsx", "csv"], key="meddra_map")
meddra_map = load_meddra_mapping(mapping_file) if mapping_file else {}

if not (src_file and prc_file):
    st.info("Please upload **both** Source and Processed XML files to view the tabular comparison.")
    st.stop()

src_bytes = src_file.read()
prc_bytes = prc_file.read()

with st.spinner("Parsing Source..."):
    src = extract_model(src_bytes, meddra_map=meddra_map)
with st.spinner("Parsing Processed..."):
    prc = extract_model(prc_bytes, meddra_map=meddra_map)

if src.get("_error") or prc.get("_error"):
    st.error(f"Source error: {src.get('_error','-')}\nProcessed error: {prc.get('_error','-')}")
    st.stop()

# ==========================================================
# DISPLAY — ORDER YOU REQUESTED
# ==========================================================

# 1) Admin
st.subheader("Admin")
admin_df = make_admin_table(src, prc)
if not admin_df.empty:
    st.table(admin_df)
else:
    st.markdown('<div style="color:#888">No header/admin values present.</div>', unsafe_allow_html=True)

# 2) Reporter
st.subheader("Reporter")
src_reps = src.get("Reporters", []) or []
prc_reps = prc.get("Reporters", []) or []
n_boxes = max(len(src_reps), len(prc_reps))
if n_boxes == 0:
    st.markdown('<div style="color:#888">No reporters (sourceReport) present.</div>', unsafe_allow_html=True)
else:
    for i in range(n_boxes):
        srep = src_reps[i] if i < len(src_reps) else {}
        prep = prc_reps[i] if i < len(prc_reps) else {}
        st.markdown(f'<h6 style="margin-top:0.5rem;margin-bottom:0.25rem;">Reporter {i+1}</h6><hr/>', unsafe_allow_html=True)
        r_df = make_reporter_pair_table(srep, prep)
        if not r_df.empty:
            st.table(r_df)
        else:
            st.markdown('<div style="color:#888">No values for this reporter.</div>', unsafe_allow_html=True)
        st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

# 3) Patient
st.subheader("Patient")
pat_df = make_patient_table(src.get("Patient",{}), prc.get("Patient",{}))
if not pat_df.empty:
    st.table(pat_df)
else:
    st.markdown('<div style="color:#888">No patient values present.</div>', unsafe_allow_html=True)

# Medical History
st.subheader("Medical History")
src_mh = src.get("MedicalHistory", []) or []
prc_mh = prc.get("MedicalHistory", []) or []

def _idx_mh(lst: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    return {e.get("_key",""): e for e in lst if e.get("_key","")}

src_mh_idx = _idx_mh(src_mh)
prc_mh_idx = _idx_mh(prc_mh)
all_mh_keys = sorted(set(src_mh_idx) | set(prc_mh_idx))

def make_mh_box_for_ui(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
    st.markdown(f'<h6 style="margin-top:0.5rem;margin-bottom:0.25rem;">Medical History: {title}</h6><hr/>', unsafe_allow_html=True)
    # Only LLT is relevant for term display; PT fields removed from UI
    llc = src_rec.get("LLT Code","") or ""
    llt = src_rec.get("LLT Term","") or ""
    plc = prc_rec.get("LLT Code","") or ""
    plt = prc_rec.get("LLT Term","") or ""
    # If term exists (mapping), blank out code; else keep code and blank term
    if llt: llc = ""
    if plt: plc = ""
    pairs = [
        ("LLT", (llt or llc), (plt or plc)),
        ("Status", src_rec.get("Status",""), prc_rec.get("Status","")),
    ("Status (Continue)", src_rec.get("Status (Continue)",""), prc_rec.get("Status (Continue)","")),
    ("Comment", src_rec.get("Comment",""), prc_rec.get("Comment","")),
        ("Start Date", src_rec.get("Start Date","") or format_date(src_rec.get("Start Date (raw)","")),
                        prc_rec.get("Start Date","") or format_date(prc_rec.get("Start Date (raw)",""))),
        ("End Date", src_rec.get("End Date","") or format_date(src_rec.get("End Date (raw)","")),
                      prc_rec.get("End Date","") or format_date(prc_rec.get("End Date (raw)",""))),
    ]
    mh_df = compare_table(pairs, treat_as_dates=True)
    if not mh_df.empty:
        st.table(mh_df)
    else:
        st.markdown('<div style="color:#888">No values for this item.</div>', unsafe_allow_html=True)
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

if not all_mh_keys:
    st.markdown('<div style="color:#888">No medical history found.</div>', unsafe_allow_html=True)
else:
    for key in all_mh_keys:
        se = src_mh_idx.get(key, {})
        pe = prc_mh_idx.get(key, {})
        title = se.get("LLT Term") or pe.get("LLT Term") or (se.get("LLT Code") or pe.get("LLT Code") or "(Unnamed history)")
        

# Drug History
st.subheader("Drug History")
src_dh = src.get("DrugHistory", []) or []
prc_dh = prc.get("DrugHistory", []) or []

def _idx_dh(lst: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    return {e.get("_key","" ): e for e in lst if e.get("_key","" )}

src_dh_idx = _idx_dh(src_dh)
prc_dh_idx = _idx_dh(prc_dh)
all_dh_keys = sorted(set(src_dh_idx) | set(prc_dh_idx))

def make_drughist_box_for_ui(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
    st.markdown(f'<h6 style="margin-top:0.5rem;margin-bottom:0.25rem;">Drug History: {title}</h6><hr/>', unsafe_allow_html=True)
    rows = [
        ("Drug", src_rec.get("Drug",""), prc_rec.get("Drug","")),
        ("Indication", src_rec.get("Indication",""), prc_rec.get("Indication","")),
        ("Reaction", src_rec.get("Reaction",""), prc_rec.get("Reaction","")),
        ("Start Date", src_rec.get("Start Date","") or format_date(src_rec.get("Start Date (raw)","")),
                       prc_rec.get("Start Date","") or format_date(prc_rec.get("Start Date (raw)",""))),
        ("End Date",   src_rec.get("End Date","") or format_date(src_rec.get("End Date (raw)","")),
                       prc_rec.get("End Date","") or format_date(prc_rec.get("End Date (raw)",""))),
    ]
    df = compare_table(rows, treat_as_dates=True)
    if not df.empty:
        st.table(df)
    else:
        st.markdown('<div style="color:#888">No values for this drug-history item.</div>', unsafe_allow_html=True)
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

if not all_dh_keys:
    st.markdown('<div style="color:#888">No drug history found.</div>', unsafe_allow_html=True)
else:
    for key in all_dh_keys:
        se = src_dh_idx.get(key, {})
        pe = prc_dh_idx.get(key, {})
        title = (se.get("Drug") or pe.get("Drug") or "(Unnamed drug)")
        make_drughist_box_for_ui(se, pe, title)

# 4) Drug

st.subheader("Drug")
src_prods = src.get("Products", [])
prc_prods = prc.get("Products", [])
src_idx = index_by_match_key(src_prods)
prc_idx = index_by_match_key(prc_prods)

ordered_keys: List[str] = []
seen_keys = set()
for rec in src_prods:
    k = _drug_match_key(rec)
    if k and k not in seen_keys:
        seen_keys.add(k); ordered_keys.append(k)
for rec in prc_prods:
    k = _drug_match_key(rec)
    if k and k not in seen_keys:
        seen_keys.add(k); ordered_keys.append(k)

if not ordered_keys:
    st.markdown('<div style="color:#888">No products found in either file.</div>', unsafe_allow_html=True)
else:
    for key in ordered_keys:
        srec = src_idx.get(key, {})
        prec = prc_idx.get(key, {})
        title = (
            (srec.get("Drug") or "").strip()
            or (prec.get("Drug") or "").strip()
            or (srec.get("_pid") or "").strip()
            or (prec.get("_pid") or "").strip()
            or "(Unnamed drug)"
        )
        st.markdown(f'<h6 style="margin-top:0.5rem;margin-bottom:0.25rem;">Drug: {title}</h6><hr/>', unsafe_allow_html=True)
        d_df = make_drug_compare_table(srec, prec)
        if not d_df.empty:
            st.table(d_df)
        else:
            st.markdown('<div style="color:#888">No values to display for this drug.</div>', unsafe_allow_html=True)
        st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

# 5) Event
st.subheader("Event")
src_evts = src.get("Events", []) or []
prc_evts = prc.get("Events", []) or []

def _idx_events(lst: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    return {e.get("_key",""): e for e in lst if e.get("_key","")}

src_evt_idx = _idx_events(src_evts)
prc_evt_idx = _idx_events(prc_evts)
all_evt_keys = sorted(set(src_evt_idx) | set(prc_evt_idx))

def make_event_box_for_ui(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
    st.markdown(f'<h6 style="margin-top:0.5rem;margin-bottom:0.25rem;">Event: {title}</h6><hr/>', unsafe_allow_html=True)
    fields = [
        ("Event Term","text"),
        ("RRT","text"),
        ("Country","text"),
        ("Translation Term","text"),
        ("Highlighted by Reporter","text"),
        ("Seriousness","text"),
        ("Outcome","text"),
        ("Event Start","date"),
        ("Event End","date"),
    ]
    rows = []
    for field, kind in fields:
        s_val = src_rec.get(field,"")
        p_val = prc_rec.get(field,"")
        if kind == "date":
            s_val = s_val or format_date(src_rec.get(field + " (raw)",""))
            p_val = p_val or format_date(prc_rec.get(field + " (raw)",""))
        rows.append((field, s_val, p_val))
    e_df = compare_table(rows, treat_as_dates=True)
    if not e_df.empty:
        st.table(e_df)
    else:
        st.markdown('<div style="color:#888">No values to display for this event.</div>', unsafe_allow_html=True)
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

if not all_evt_keys:
    st.markdown('<div style="color:#888">No events found in either file.</div>', unsafe_allow_html=True)
else:
    for key in all_evt_keys:
        se = src_evt_idx.get(key, {})
        pe = prc_evt_idx.get(key, {})
        title = se.get("Event Term") or pe.get("Event Term") or se.get("RRT") or pe.get("RRT") or "(Unnamed event)"
        make_event_box_for_ui(se, pe, title)

# 6) Lab
st.subheader("Lab")
src_lab = src.get("LabDetails", []) or []
prc_lab = prc.get("LabDetails", []) or []

def _idx_lab(lst: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    return {e.get("_key",""): e for e in lst if e.get("_key","")}

src_lab_idx = _idx_lab(src_lab)
prc_lab_idx = _idx_lab(prc_lab)
all_lab_keys = sorted(set(src_lab_idx) | set(prc_lab_idx))

def make_lab_box_for_ui(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
    st.markdown(f'<h6 style="margin-top:0.5rem;margin-bottom:0.25rem;">Lab: {title}</h6><hr/>', unsafe_allow_html=True)
    ll_s = src_rec.get("LLT Term","") or ""
    lc_s = src_rec.get("LLT Code","") or ""
    ll_p = prc_rec.get("LLT Term","") or ""
    lc_p = prc_rec.get("LLT Code","") or ""
    if ll_s: lc_s = ""  # show only term if available
    if ll_p: lc_p = ""
    display_ll_s = ll_s if ll_s else lc_s
    display_ll_p = ll_p if ll_p else lc_p
    fields = [("LLT","text"), ("Result","text"), ("Result Date","date")]
    rows = []
    rows.append(("LLT", display_ll_s, display_ll_p))
    rows.append(("Result", src_rec.get("Result",""), prc_rec.get("Result","")))
    rows.append(("Result Date", src_rec.get("Result Date",""), prc_rec.get("Result Date","")))
    df = compare_table(rows, treat_as_dates=True)
    if not df.empty:
        st.table(df)
    else:
        st.markdown('<div style="color:#888">No values for this lab item.</div>', unsafe_allow_html=True)
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

if not all_lab_keys:
    st.markdown('<div style="color:#888">No lab details found.</div>', unsafe_allow_html=True)
else:
    for key in all_lab_keys:
        se = src_lab_idx.get(key, {})
        pe = prc_lab_idx.get(key, {})
        title = (se.get("LLT Term") or pe.get("LLT Term") or se.get("LLT Code") or pe.get("LLT Code") or "(Unnamed lab)")
        make_lab_box_for_ui(se, pe, title)

# 7) Narrative
st.subheader("Narrative")
src_narr_full = src.get("Narrative","") or ""
prc_narr_full = prc.get("Narrative","") or ""
if not has_value(src_narr_full) and not has_value(prc_narr_full):
    st.markdown('<div style="color:#888">No narrative present in either file.</div>', unsafe_allow_html=True)
else:
    st.markdown('<h6>Source</h6>', unsafe_allow_html=True)
    st.markdown(f'<div style="white-space:pre-wrap">{src_narr_full if src_narr_full else "—"}</div>', unsafe_allow_html=True)
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)
    st.markdown('<h6>Processed</h6>', unsafe_allow_html=True)
    st.markdown(f'<div style="white-space:pre-wrap">{prc_narr_full if prc_narr_full else "—"}</div>', unsafe_allow_html=True)
    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

# 8) Causality — SINGLE CONSOLIDATED TABLE
st.subheader("Causality")

def _caus_df(lst: List[Dict[str,Any]]) -> pd.DataFrame:
    if not lst:
        return pd.DataFrame(columns=["Drug","Reaction","Assessor","Method","Assessment"])
    rows = []
    for r in lst:
        rows.append({
            "Drug": r.get("Drug",""),
            "Reaction": r.get("Reaction",""),
            "Assessor": r.get("Assessor",""),
            "Method": r.get("Method",""),
            "Assessment": r.get("Assessment",""),
        })
    return pd.DataFrame(rows)

st.markdown("#### Source")
src_caus_df = _caus_df(src.get("Causality", []) or [])
if not src_caus_df.empty:
    st.dataframe(src_caus_df, use_container_width=True)
else:
    st.markdown('<div style="color:#888">No causality rows in Source.</div>', unsafe_allow_html=True)

st.markdown("#### Processed")
prc_caus_df = _caus_df(prc.get("Causality", []) or [])
if not prc_caus_df.empty:
    st.dataframe(prc_caus_df, use_container_width=True)
else:
    st.markdown('<div style="color:#888">No causality rows in Processed.</div>', unsafe_allow_html=True)
