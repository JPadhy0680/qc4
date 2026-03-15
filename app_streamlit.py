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

# Optional debug control (Events parsing warnings)
DEBUG_EVENTS = st.sidebar.checkbox("Debug events parsing", value=False)

# ---------------- Utilities ----------------
NS = {'hl7': 'urn:hl7-org:v3', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
UNKNOWN_TOKENS = {"unk", "asku", "unknown"}

# Admin identifiers
SENDER_ID_OID      = "2.16.840.1.113883.3.989.2.1.3.1"   # Sender ID
WWID_OID           = "2.16.840.1.113883.3.989.2.1.3.2"   # WWID
FIRST_SENDER_OID   = "2.16.840.1.113883.3.989.2.1.1.3"   # First sender of case (1=Regulator, 2=Other)
FIRST_SENDER_MAP   = {"1": "Regulator", "2": "Other"}

# Reporter qualification OID and mapping (1..5)
REPORTER_QUAL_OID  = "2.16.840.1.113883.3.989.2.1.1.6"
REPORTER_MAP = {
    "1": "Physician",
    "2": "Pharmacist",
    "3": "Other health professional",
    "4": "Lawyer",
    "5": "Consumer or other non-health professional",
}

# Reporter SOURCE anchor OID (to locate reporter branches)
REPORT_SOURCE_OID  = "2.16.840.1.113883.3.989.2.1.1.22"  # displayName="sourceReport"

# Patient: Age observation OID
AGE_OID            = "2.16.840.1.113883.3.989.2.1.1.19"  # 'age' observation system

# Patient: Record Number OID
PATIENT_RECORD_OID = "2.16.840.1.113883.3.989.2.1.3.7"

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

# ---- Generic patient observation resolver (displayName OR OID) ----
def get_pq_value_by_code(
    root: ET.Element,
    display_name: Optional[str] = None,
    code_system_oid: Optional[str] = None
) -> Tuple[str, str]:
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
    """
    Return an identifier's extension for the given OID anywhere in the doc.
    If the id node has nullFlavor='MSK', return 'Masked'.
    Otherwise prefer @extension (if present); else ''.
    """
    for el in root.iter():
        if local_name(el.tag) != 'id':
            continue
        if el.attrib.get('root') == oid:
            if el.attrib.get('nullFlavor') == 'MSK':
                return "Masked"
            ext = (el.attrib.get('extension') or '').strip()
            return ext
    return ""

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
    # FRD (earliest low)
    lows = []
    for el in root.iter():
        if local_name(el.tag) == 'low':
            v = el.attrib.get('value')
            if v: lows.append(v)
    if lows:
        pairs = [(parse_date_obj(v), v) for v in lows if parse_date_obj(v)]
        if pairs:
            pairs.sort(key=lambda t: t[0])
            out["FRD_raw"] = pairs[0][1]; out["FRD"] = format_date(pairs[0][1])
    return out

# ---------------- Patient extraction (patched) ----------------
def extract_patient(root: ET.Element) -> Dict[str, str]:
    # Gender
    gender_elem = find_first(root, './/hl7:administrativeGenderCode')
    gender_code = gender_elem.attrib.get('code', '') if gender_elem is not None else ''
    gender = clean_value(map_gender(gender_code))

    # Age: support displayName OR OID (AGE_OID)
    age_val, age_unit_raw = get_pq_value_by_code(root, display_name="age", code_system_oid=AGE_OID)
    unit_map = {'a': 'year', 'b': 'month'}
    age_unit_label = unit_map.get((age_unit_raw or '').lower(), age_unit_raw or '')
    age = ""
    if clean_value(age_val):
        age = clean_value(age_val)
        if clean_value(age_unit_label):
            age = f"{age} {age_unit_label}"

    # Age Group (kept as before)
    age_group_map = {"0":"Foetus","1":"Neonate","2":"Infant","3":"Child","4":"Adolescent","5":"Adult","6":"Elderly"}
    ag_elem = find_first(root, './/hl7:code[@displayName="ageGroup"]/../hl7:value')
    age_group = ""
    if ag_elem is not None:
        c = ag_elem.attrib.get('code','')
        nf = ag_elem.attrib.get('nullFlavor','')
        age_group = age_group_map.get(c, "[Masked/Unknown]" if (c in ["MSK","UNK","ASKU","NI"] or nf in ["MSK","UNK","ASKU","NI"]) else "")

    # Weight: prefer displayName; fallback by typical units if displayName omitted
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

    # Height: prefer displayName; fallback by typical units if displayName omitted
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
                if g.text and g.text.strip(): parts.append(g.text.strip()[0].upper())
            fam = nm.find('hl7:family', NS)
            if fam is not None and fam.text and fam.text.strip(): parts.append(fam.text.strip()[0].upper())
            initials = "".join(parts) or clean_value(get_text(nm))

    # Patient Record Number (mask-aware)
    patient_record_no = find_mask_aware_id_by_root(root, PATIENT_RECORD_OID)

    return {
        "Gender": clean_value(gender),
        "Age": clean_value(age),
        "Age Group": clean_value(age_group),
        "Height": clean_value(height),
        "Weight": clean_value(weight),
        "Initials": clean_value(initials),
        "Patient Record Number": clean_value(patient_record_no),
    }

# ---------------- Products extraction (ALL + robust grouping) ----------------
def extract_suspect_ids(root: ET.Element) -> Set[str]:
    out = set()
    for c in findall(root, './/hl7:causalityAssessment'):
        v = find_first(c, './/hl7:value')
        if v is not None and v.attrib.get('code') == '1':
            sid = find_first(c, './/hl7:subject2/hl7:productUseReference/hl7:id')
            if sid is not None:
                out.add(sid.attrib.get('root',''))
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

def _resolve_drug_name(admin: ET.Element) -> str:
    """
    Try multiple, realistic paths to get a human-readable product name.
    Returns '' if a name cannot be found anywhere.
    """
    # 1) kindOfProduct/name (text or @displayName or <originalText>)
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

    # 2) manufacturedProduct/name
    alt = find_first(admin, './/hl7:manufacturedProduct/hl7:name')
    if alt is not None and (alt.text or '').strip():
        return alt.text.strip()

    # 3) manufacturedMaterial/name
    mm = find_first(admin, './/hl7:manufacturedMaterial/hl7:name')
    if mm is not None and (mm.text or '').strip():
        return mm.text.strip()

    # 4) code@displayName under manufacturedMaterial
    mm_code = find_first(admin, './/hl7:manufacturedMaterial/hl7:code')
    if mm_code is not None:
        disp = (mm_code.attrib.get('displayName') or '').strip()
        if disp:
            return disp

    # 5) asManufacturedProduct/.../name
    amp = find_first(admin, './/hl7:asManufacturedProduct//hl7:name')
    if amp is not None and (amp.text or '').strip():
        return amp.text.strip()

    return ""

def extract_all_products(root: ET.Element) -> List[Dict[str, Any]]:
    """
    Extract ALL substanceAdministration records.
    Classify each as Suspect / Interacting / Treatment / Concomitant.
    Preserve multiple regimens (one record per administration).
    Ensure all regimens for the same product fall under ONE group (by name if present; else by ID).
    """
    suspects   = extract_suspect_ids(root)
    interact   = extract_interacting_ids(root)
    treatments = extract_treatment_ids(root)

    prods: List[Dict[str, Any]] = []
    running_unnamed_idx = 0  # only used if both name and pid are missing

    for admin in findall(root, './/hl7:substanceAdministration'):
        # Product ID (first <id>)
        id_elem = find_first(admin, './/hl7:id')
        pid = (id_elem.attrib.get('root', '') if id_elem is not None else '').strip()

        # Robust name resolution
        raw_name = _resolve_drug_name(admin).strip()

        # Canonical key to group the drug:
        #  - Prefer normalized name
        #  - Else pid
        #  - Else a stable synthetic fallback
        if raw_name:
            name_key = normalize_text(raw_name)
        elif pid:
            name_key = f"pid::{pid.lower()}"
        else:
            running_unnamed_idx += 1
            name_key = f"drug#{running_unnamed_idx:03d}"

        # Dosage text & quantity
        txt = get_text(find_first(admin, './/hl7:text'))
        dq = find_first(admin, './/hl7:doseQuantity')
        dose_v = dq.attrib.get('value','') if dq is not None else ''
        dose_u = dq.attrib.get('unit','') if dq is not None else ''

        # Dates
        low  = find_first(admin, './/hl7:effectiveTime/hl7:low')
        high = find_first(admin, './/hl7:effectiveTime/hl7:high')
        sd_raw = (low.attrib.get('value') or '').strip() if low is not None else ''
        ed_raw = (high.attrib.get('value') or '').strip() if high is not None else ''

        # Route
        rtxt = get_text(find_first(admin, './/hl7:routeCode/hl7:originalText'))
        if not rtxt:
            rc = find_first(admin, './/hl7:routeCode')
            rtxt = (rc.attrib.get('displayName') or '') if rc is not None else ''

        # Formulation, Lot, MAH
        form = get_text(find_first(admin, './/hl7:formCode/hl7:originalText'))
        lot  = get_text(find_first(admin, './/hl7:lotNumberText'))
        mah  = ""
        for p in [
            './/hl7:playingOrganization/hl7:name',
            './/hl7:manufacturerOrganization/hl7:name',
            './/hl7:asManufacturedProduct/hl7:manufacturerOrganization/hl7:name',
        ]:
            node = find_first(admin, p)
            if node is not None and get_text(node):
                mah = get_text(node); break

        # Classification tags
        tags = []
        if pid and pid in suspects:   tags.append("Suspect")
        if pid and pid in interact:   tags.append("Interacting")
        if pid and pid in treatments: tags.append("Treatment")
        if not tags:                  tags.append("Concomitant")

        prods.append({
            "Drug": clean_value(raw_name) if raw_name else (pid or ""),
            "Type": ", ".join(sorted(set(tags))),
            "Dosage Text": clean_value(txt),
            "Dose Value": clean_value(dose_v),
            "Dose Unit": clean_value(dose_u),
            "Start Date (raw)": sd_raw, "Start Date": format_date(sd_raw),
            "Stop Date (raw)": ed_raw, "Stop Date": format_date(ed_raw),
            "Route": clean_value(rtxt),
            "Formulation": clean_value(form),
            "Lot No": clean_value(lot),
            "MAH": clean_value(mah),
            "_name_key": name_key,    # unified grouping key
            "_pid": pid or "",
        })

    # Number regimens within each drug group in encounter order
    counts: Dict[str, int] = {}
    for rec in prods:
        k = rec["_name_key"]
        counts[k] = counts.get(k, 0) + 1
        rec["_regimen_no"] = counts[k]

    return prods

# ---------------- Reporter extraction (STRICT: only branches with sourceReport; list ALL; pair sequentially) ----------------
def build_parent_map(root: ET.Element) -> Dict[ET.Element, ET.Element]:
    return {c: p for p in root.iter() for c in list(p)}

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
    # de-dup by identity & preserve order
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
        "Reporter Name (Full)": "",
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
        rt  = (id_el.attrib.get('root') or '').strip()
        if ext and rt: ids.append(f"{ext} ({rt})")
        elif ext: ids.append(ext)
        elif rt: ids.append(rt)
    if ids:
        result["Reporter IDs"] = "; ".join(dict.fromkeys(ids))

    # Qualification
    qual = ""
    for code_el in node.iter():
        if local_name(code_el.tag) == 'code' and code_el.attrib.get('codeSystem') == REPORTER_QUAL_OID:
            c = (code_el.attrib.get('code') or '').strip()
            qual = REPORTER_MAP.get(c, c); break
    result["Reporter Qualification"] = qual

    # Name + Title (mask-aware)
    name_el = node.find('.//hl7:assignedPerson/hl7:name', NS) or node.find('.//hl7:name', NS)

    title_vals, given_vals, family_val = [], [], ""
    if name_el is not None:
        for pfx in name_el.findall('hl7:prefix', NS):
            v = read_text_or_mask(pfx)
            if v: title_vals.append(v)
        for g in name_el.findall('hl7:given', NS):
            v = read_text_or_mask(g)
            if v: given_vals.append(v)
        fam_el = name_el.find('hl7:family', NS)
        family_val = read_text_or_mask(fam_el)

    parts = [p for p in given_vals if p] + ([family_val] if family_val else [])
    full_name = " ".join(parts).strip()
    if not full_name and name_el is not None and any(ch.attrib.get('nullFlavor') == 'MSK' for ch in name_el):
        full_name = "Masked"

    result["Reporter Title"]         = "; ".join(title_vals) if title_vals else ""
    result["Reporter Name (Full)"]   = full_name
    result["Reporter Given Name(s)"] = "; ".join(given_vals) if given_vals else ""
    result["Reporter Family Name"]   = family_val

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
            if val: streets.append(val)
        city   = read_text_or_mask(addr.find('hl7:city', NS))
        state  = read_text_or_mask(addr.find('hl7:state', NS))
        postal = read_text_or_mask(addr.find('hl7:postalCode', NS))
        country= read_text_or_mask(addr.find('hl7:country', NS))
    if not country:
        loc = node.find('.//hl7:asLocatedEntity/hl7:location/hl7:code', NS)
        if loc is not None and loc.attrib.get('code'):
            country = loc.attrib.get('code').strip()
    result["Reporter Street"]         = ", ".join(streets)
    result["Reporter City/Town"]      = city
    result["Reporter State/Province"] = state
    result["Reporter Postal Code"]    = postal
    result["Reporter Country"]        = country

    # Telecoms
    phones, emails, faxes = [], [], []
    for tel in node.findall('.//hl7:telecom', NS):
        raw = (tel.attrib.get('value') or '').strip()
        use = (tel.attrib.get('use') or '').upper()
        if not raw: continue
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
            elif len(re.sub(r'\D','',raw)) >= 7:
                phones.append(raw)
            else:
                phones.append(raw)
    if phones: result["Reporter Phone(s)"] = "; ".join(dict.fromkeys(phones))
    if emails: result["Reporter Email(s)"] = "; ".join(dict.fromkeys(emails))
    if faxes:  result["Reporter Fax(es)"]  = "; ".join(dict.fromkeys(faxes))
    return result

def extract_reporters_from_sourceReport(root: ET.Element) -> List[Dict[str, str]]:
    containers = find_all_source_report_containers(root)
    reporters: List[Dict[str, str]] = []
    for node in containers:
        rep = extract_reporter_from_container(node)
        if any(clean_value(v) for v in rep.values()):
            reporters.append(rep)  # sequential; no matching
    return reporters

# ---------------- Events extraction (robust + debug) ----------------
def extract_events(root: ET.Element, debug: bool = False) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    debug_rows: List[str] = []
    try:
        rxns = findall(root, './/hl7:observation')
        if not rxns:
            rxns = [el for el in root.iter() if local_name(el.tag) == 'observation']
        for idx, rxn in enumerate(rxns, start=1):
            try:
                code_el = rxn.find('hl7:code', NS)
                is_reaction = False
                if code_el is not None:
                    disp = (code_el.attrib.get('displayName') or '').strip()
                    is_reaction = (disp.lower() == 'reaction')
                val_el = rxn.find('hl7:value', NS)
                if not is_reaction and val_el is None:
                    continue
                llt_code = (val_el.attrib.get('code') or '').strip() if val_el is not None else ''
                llt_term = (val_el.attrib.get('displayName') or '').strip() if val_el is not None else ''
                if not (llt_code or llt_term):
                    if val_el is not None:
                        ot = val_el.find('hl7:originalText', NS)
                        llt_term = get_text(ot)
                    if not (llt_code or llt_term):
                        continue
                ser_map = {
                    "resultsInDeath": "Death",
                    "isLifeThreatening": "LT",
                    "requiresInpatientHospitalization": "Hospital",
                    "resultsInPersistentOrSignificantDisability": "Disability",
                    "congenitalAnomalyBirthDefect": "Congenital",
                    "otherMedicallyImportantCondition": "IME"
                }
                flags: List[str] = []
                for k, lbl in ser_map.items():
                    crit = rxn.find(f'.//hl7:code[@displayName="{k}"]/../hl7:value', NS)
                    if crit is not None and (crit.attrib.get('value') or '').strip().lower() == 'true':
                        flags.append(lbl)
                outcome_map = {
                    "1": "Recovered/Resolved",
                    "2": "Recovering/Resolving",
                    "3": "Not recovered/Ongoing",
                    "4": "Recovered with sequelae",
                    "5": "Fatal",
                    "0": "Unknown"
                }
                outcome_el = rxn.find('.//hl7:code[@displayName="outcome"]/../hl7:value', NS)
                outcome_code = (outcome_el.attrib.get('code') or '').strip() if outcome_el is not None else ''
                outcome = outcome_map.get(outcome_code, "Unknown" if outcome_code else "")

                low = rxn.find('.//hl7:effectiveTime/hl7:low', NS)
                high = rxn.find('.//hl7:effectiveTime/hl7:high', NS)
                start_raw = (low.attrib.get('value') or '').strip() if low is not None else ''
                end_raw = (high.attrib.get('value') or '').strip() if high is not None else ''
                start_disp = format_date(start_raw); end_disp = format_date(end_raw)

                out.append({
                    "LLT Code": clean_value(llt_code),
                    "LLT Term": clean_value(llt_term),
                    "Seriousness": "Non-serious" if not flags else ", ".join(sorted(set(flags))),
                    "Outcome": clean_value(outcome),
                    "Event Start (raw)": start_raw, "Event Start": start_disp,
                    "Event End (raw)": end_raw, "Event End": end_disp,
                    "_key": clean_value(llt_code) or normalize_text(llt_term),
                })
            except Exception as e_evt:
                if debug: debug_rows.append(f"[event {idx}] {type(e_evt).__name__}: {e_evt}")
        if debug and debug_rows:
            st.warning("Event parsing warnings:\n- " + "\n- ".join(debug_rows))
        return out
    except Exception as e:
        if debug:
            st.exception(e)
        return out

# ---------------- Narrative extraction ----------------
def extract_narrative(root: ET.Element) -> str:
    narrative_elem = root.find('.//hl7:code[@code="PAT_ADV_EVNT"]/../hl7:text', NS)
    return clean_value(narrative_elem.text if narrative_elem is not None else '')

# ---------------- Model builder ----------------
def extract_model(xml_bytes: bytes, debug_events: bool = False) -> Dict[str, Any]:
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
    model["Products"] = extract_all_products(root)   # ALL drugs with types + regimens (grouped by robust key)
    model["Events"] = extract_events(root, debug=debug_events)
    model["Narrative"] = extract_narrative(root)
    return model

# --------------- Table builders (hide fully blank rows) ----------------
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
        "Reporter Qualification",
        "Reporter IDs",
        "Reporter Title",
        "Reporter Name (Full)",
        "Reporter Given Name(s)",
        "Reporter Family Name",
        "Reporter Organization",
        "Reporter Street",
        "Reporter City/Town",
        "Reporter State/Province",
        "Reporter Postal Code",
        "Reporter Country",
        "Reporter Phone(s)",
        "Reporter Email(s)",
        "Reporter Fax(es)",
    ]
    rows = [(f, src_rep.get(f,""), prc_rep.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

def make_patient_table(src_pat: Dict[str,str], prc_pat: Dict[str,str]) -> pd.DataFrame:
    fields = ["Gender","Age","Age Group","Height","Weight","Initials","Patient Record Number"]
    rows = [(f, src_pat.get(f,""), prc_pat.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

# ------ Drug UI helpers: grouping + per-drug de-dup of identical regimens ------
def group_products_by_name(products: List[Dict[str,Any]]) -> Dict[str, List[Dict[str,Any]]]:
    """
    Groups by the canonical '_name_key' produced in extract_all_products.
    Never returns a blank key; order preserved.
    """
    groups: Dict[str, List[Dict[str,Any]]] = {}
    for rec in products:
        k = rec.get("_name_key") or "drug#000"
        groups.setdefault(k, []).append(rec)
    return groups

def _dedupe_regimens(regs: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """
    Remove exact duplicate regimen rows within a drug group (keeps encounter order).
    """
    seen = set()
    out  = []
    keep_fields = ("Type","Dosage Text","Dose Value","Dose Unit","Start Date","Stop Date","Route","Formulation","Lot No","MAH")
    for r in regs:
        sig = tuple((r.get(f,"") or "") for f in keep_fields)
        if sig in seen:
            continue
        seen.add(sig)
        out.append(r)
    return out

def make_regimen_pair_table(src_rec: Dict[str,Any], prc_rec: Dict[str,Any]) -> pd.DataFrame:
    fields = [
        "Type",
        "Dosage Text",
        "Dose Value",
        "Dose Unit",
        "Start Date",
        "Stop Date",
        "Route",
        "Formulation",
        "Lot No",
        "MAH",
    ]
    rows = []
    for field in fields:
        s_val = src_rec.get(field, "")
        p_val = prc_rec.get(field, "")
        if "Date" in field:
            s_val = s_val or format_date(src_rec.get(field + " (raw)", ""))
            p_val = p_val or format_date(prc_rec.get(field + " (raw)", ""))
        rows.append((field, s_val, p_val))
    return compare_table(rows, treat_as_dates=True)

# --------------- UI: Upload & Parse ----------------
st.markdown("### 📤 Upload the two XML files you want to compare (no ID pairing; exact files compared)")
c1, c2 = st.columns(2)
with c1:
    src_file = st.file_uploader("Source XML", type=["xml"], key="src_xml")
with c2:
    prc_file = st.file_uploader("Processed XML", type=["xml"], key="prc_xml")

if not (src_file and prc_file):
    st.info("Please upload **both** Source and Processed XML files to view the tabular comparison.")
    st.stop()

src_bytes = src_file.read()
prc_bytes = prc_file.read()

with st.spinner("Parsing Source..."):
    src = extract_model(src_bytes, debug_events=DEBUG_EVENTS)
with st.spinner("Parsing Processed..."):
    prc = extract_model(prc_bytes, debug_events=DEBUG_EVENTS)

if src.get("_error") or prc.get("_error"):
    st.error(f"Source error: {src.get('_error','-')}\nProcessed error: {prc.get('_error','-')}")
    st.stop()

# ---------------- SECTION: Admin/Header ----------------
st.subheader("Admin / Header")
admin_df = make_admin_table(src, prc)
if not admin_df.empty:
    st.table(admin_df)
else:
    st.markdown('<div class="box smallnote">No header/admin values present in either file.</div>', unsafe_allow_html=True)

# ---------------- SECTION: Reporters (paired sequentially) ----------------
st.subheader("Reporters (paired sequentially)")
src_reps = src.get("Reporters", []) or []
prc_reps = prc.get("Reporters", []) or []

n_boxes = max(len(src_reps), len(prc_reps))
if n_boxes == 0:
    st.markdown('<div class="box smallnote">No reporters (sourceReport) present in either file.</div>', unsafe_allow_html=True)
else:
    for i in range(n_boxes):
        srep = src_reps[i] if i < len(src_reps) else {}
        prep = prc_reps[i] if i < len(prc_reps) else {}
        st.markdown(f'<div class="box"><h5>Reporter {i+1}</h5>', unsafe_allow_html=True)
        r_df = make_reporter_pair_table(srep, prep)
        if r_df.empty:
            st.markdown('<div class="smallnote">No values to display for this reporter in either file.</div>', unsafe_allow_html=True)
        else:
            st.table(r_df)
        st.markdown('</div>', unsafe_allow_html=True)

# ---------------- SECTION: Patient Details ----------------
st.subheader("Patient Details")
pat_df = make_patient_table(src.get("Patient",{}), prc.get("Patient",{}))
if not pat_df.empty:
    st.table(pat_df)
else:
    st.markdown('<div class="box smallnote">No patient values present in either file.</div>', unsafe_allow_html=True)

# ---------------- SECTION: Drug Details (ALL products, grouped by drug name, regimen-wise) ----------------
st.subheader("Drug Details (all products) — grouped by drug name; regimen-wise")

src_prods = src.get("Products", [])
prc_prods = prc.get("Products", [])

# Group and de-duplicate identical regimens per drug
src_groups_raw = group_products_by_name(src_prods)
prc_groups_raw = group_products_by_name(prc_prods)
src_groups = {k: _dedupe_regimens(v) for k, v in src_groups_raw.items()}
prc_groups = {k: _dedupe_regimens(v) for k, v in prc_groups_raw.items()}

all_name_keys = sorted(set(src_groups) | set(prc_groups))

if not all_name_keys:
    st.markdown('<div class="box smallnote">No products found in either file.</div>', unsafe_allow_html=True)
else:
    for name_key in all_name_keys:
        s_list = src_groups.get(name_key, [])
        p_list = prc_groups.get(name_key, [])
        # Title from any non-empty name (fallback to PID if truly nameless)
        title = ""
        for lst in (s_list, p_list):
            for rec in lst:
                if rec.get("Drug"):
                    title = rec["Drug"]; break
            if title: break
        if not title:
            # Try PID hint
            hint = ""
            for lst in (s_list, p_list):
                for rec in lst:
                    if rec.get("_pid"):
                        hint = rec["_pid"]; break
                if hint: break
            title = hint or "(Unnamed drug)"

        st.markdown(f'<div class="box"><h5>Drug: {title}</h5>', unsafe_allow_html=True)

        n_reg = max(len(s_list), len(p_list))
        if n_reg == 0:
            st.markdown('<div class="smallnote">No regimens present in either file for this drug.</div>', unsafe_allow_html=True)
        else:
            for idx in range(n_reg):
                srec = s_list[idx] if idx < len(s_list) else {}
                prec = p_list[idx] if idx < len(p_list) else {}
                st.markdown(f"**Regimen #{idx+1}**")
                d_df = make_regimen_pair_table(srec, prec)
                if d_df.empty:
                    st.markdown('<div class="smallnote">No values to display for this regimen in either file.</div>', unsafe_allow_html=True)
                else:
                    st.table(d_df)
                st.markdown("---")
        st.markdown('</div>', unsafe_allow_html=True)

# ---------------- SECTION: Event Details (matched by LLT Code then term) ----------------
st.subheader("Event Details — matched by LLT code (fallback: normalized term)")
src_evts = src.get("Events", [])
prc_evts = prc.get("Events", [])
def _idx_events(lst: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    return {e.get("_key",""): e for e in lst if e.get("_key","")}
src_evt_idx = _idx_events(src_evts)
prc_evt_idx = _idx_events(prc_evts)
all_evt_keys = sorted(set(src_evt_idx) | set(prc_evt_idx))

def make_event_box_for_ui(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
    st.markdown(f'<div class="box"><h5>Event: {title}</h5>', unsafe_allow_html=True)
    fields = [
        ("LLT Code","text"),
        ("LLT Term","text"),
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
    if e_df.empty:
        st.markdown('<div class="smallnote">No values to display for this event in either file.</div>', unsafe_allow_html=True)
    else:
        st.table(e_df)
    st.markdown('</div>', unsafe_allow_html=True)

if not all_evt_keys:
    st.markdown('<div class="box smallnote">No events found in either file.</div>', unsafe_allow_html=True)
else:
    # show union; keep source-first, then processed-only
    shown = set()
    for key in all_evt_keys:
        se = src_evt_idx.get(key, {})
        pe = prc_evt_idx.get(key, {})
        title = se.get("LLT Term") or pe.get("LLT Term") or (se.get("LLT Code") or pe.get("LLT Code") or "(Unnamed event)")
        make_event_box_for_ui(se, pe, title)
        shown.add(key)

# ---------------- SECTION: Narrative ----------------
st.subheader("Narrative")
show_full = st.checkbox("Show full narrative (may be long)", value=True)
max_len = None if show_full else 1000
src_narr_full = src.get("Narrative","")
prc_narr_full = prc.get("Narrative","")
src_narr = src_narr_full[:max_len] if max_len else src_narr_full
prc_narr = prc_narr_full[:max_len] if max_len else prc_narr_full
if not has_value(src_narr_full) and not has_value(prc_narr_full):
    st.markdown('<div class="box smallnote">No narrative present in either file.</div>', unsafe_allow_html=True)
else:
    st.markdown('<div class="box"><h5>Source</h5>', unsafe_allow_html=True); st.code(src_narr or "—"); st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('<div class="box"><h5>Processed</h5>', unsafe_allow_html=True); st.code((prc_narr or "—") + (" 🔴" if (src_narr_full != prc_narr_full) else "")); st.markdown('</div>', unsafe_allow_html=True)

# ---------------- Export (robust Excel engine handling) ----------------
st.markdown("---")
st.markdown("### ⬇️ Download Comparison (Excel)")

def rows_from_table(df: pd.DataFrame, section: str, group: str = "") -> List[Dict[str,str]]:
    if df is None or df.empty: return []
    return [{"Section": section, "Group": group, "Field": r["Field"], "Source": r["Source"], "Processed": r["Processed"]} for _, r in df.iterrows()]

admin_rows = rows_from_table(admin_df, "Admin/Header")
patient_rows = rows_from_table(pat_df, "Patient")

# Reporter rows (paired)
reporter_rows: List[Dict[str,str]] = []
for i in range(max(len(src_reps), len(prc_reps))):
    srep = src_reps[i] if i < len(src_reps) else {}
    prep = prc_reps[i] if i < len(prc_reps) else {}
    title = f"Reporter {i+1}"
    r_df = make_reporter_pair_table(srep, prep)
    reporter_rows += rows_from_table(r_df, "Reporter", group=title)

# Drugs sheet (flatten all products & regimens)
drug_rows = []
for name_key in all_name_keys:
    s_list = src_groups.get(name_key, [])
    p_list = prc_groups.get(name_key, [])
    # Title
    title = ""
    for lst in (s_list, p_list):
        for rec in lst:
            if rec.get("Drug"):
                title = rec["Drug"]; break
        if title: break
    if not title:
        hint = ""
        for lst in (s_list, p_list):
            for rec in lst:
                if rec.get("_pid"):
                    hint = rec["_pid"]; break
            if hint: break
        title = hint or "(Unnamed drug)"

    n_reg = max(len(s_list), len(p_list))
    for idx in range(n_reg):
        srec = s_list[idx] if idx < len(s_list) else {}
        prec = p_list[idx] if idx < len(p_list) else {}
        group_name = f"{title} — Regimen #{idx+1}"
        for field in ["Type","Dosage Text","Dose Value","Dose Unit","Start Date","Stop Date","Route","Formulation","Lot No","MAH"]:
            s_val = srec.get(field, "") or (format_date(srec.get(field + " (raw)","")) if "Date" in field else "")
            p_val = prec.get(field, "") or (format_date(prec.get(field + " (raw)","")) if "Date" in field else "")
            if has_value(s_val) or has_value(p_val):
                drug_rows.append({"Section":"Drug", "Group": group_name, "Field": field, "Source": s_val or "—", "Processed": p_val or "—"})

# Events sheet (flatten; include processed-only too)
event_rows = []
for key, se in src_evt_idx.items():
    pe = prc_evt_idx.get(key, {})
    title = se.get("LLT Term") or pe.get("LLT Term") or (se.get("LLT Code") or pe.get("LLT Code") or "(Unnamed event)")
    for field in ["LLT Code","LLT Term","Seriousness","Outcome","Event Start","Event End"]:
        s_val = se.get(field, "") or (format_date(se.get(field + " (raw)","")) if "Event" in field else "")
        p_val = pe.get(field, "") or (format_date(pe.get(field + " (raw)","")) if "Event" in field else "")
        if has_value(s_val) or has_value(p_val):
            event_rows.append({"Section":"Event", "Group": title, "Field": field, "Source": s_val or "—", "Processed": p_val or "—"})
for key, pe in prc_evt_idx.items():
    if key in src_evt_idx:
        continue
    title = pe.get("LLT Term") or pe.get("LLT Code") or "(Unnamed event)"
    for field in ["LLT Code","LLT Term","Seriousness","Outcome","Event Start","Event End"]:
        s_val = ""
        p_val = pe.get(field, "") or (format_date(pe.get(field + " (raw)","")) if "Event" in field else "")
        if has_value(p_val):
            event_rows.append({"Section":"Event", "Group": title, "Field": field, "Source": "—", "Processed": p_val})

def build_excel_bytes() -> Optional[bytes]:
    sheets: Dict[str, pd.DataFrame] = {}
    sheets["Admin_Patient"] = pd.DataFrame(admin_rows + patient_rows)
    if reporter_rows: sheets["Reporters"] = pd.DataFrame(reporter_rows)
    if drug_rows:     sheets["Drugs"] = pd.DataFrame(drug_rows)
    if event_rows:    sheets["Events"] = pd.DataFrame(event_rows)
    if has_value(src.get("Narrative","")) or has_value(prc.get("Narrative","")):
        sheets["Narrative"] = pd.DataFrame([{
            "Source Narrative": src.get("Narrative","") or "—",
            "Processed Narrative": prc.get("Narrative","") or "—"
        }])
    excel_buffer = io.BytesIO()
    for engine in ("openpyxl", "xlsxwriter"):
        try:
            with pd.ExcelWriter(excel_buffer, engine=engine) as writer:
                for name, df in sheets.items():
                    df.to_excel(writer, index=False, sheet_name=name)
            return excel_buffer.getvalue()
        except ModuleNotFoundError:
            continue
        except Exception as e:
            st.warning(f"Excel export using engine='{engine}' failed: {e}")
            continue
    return None

excel_bytes = build_excel_bytes()
if excel_bytes:
    st.download_button("Download qc_twofile_compare_tabular.xlsx", excel_bytes, "qc_twofile_compare_tabular.xlsx")
else:
    st.error(
        "Excel export failed because neither 'openpyxl' nor 'xlsxwriter' is available. "
        "Add one of these to your environment (requirements.txt) and redeploy.\n\n"
        "Example:\n  openpyxl>=3.1.0\n  # or\n  XlsxWriter>=3.1.0"
    )
