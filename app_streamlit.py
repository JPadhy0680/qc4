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

# "Report Source" (your snippet)
REPORT_SOURCE_OID = "2.16.840.1.113883.3.989.2.1.1.22"

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
    return clean_value(elem.text) if (elem is not None and elem.text) else ""

def read_text_or_mask(elem: Optional[ET.Element]) -> str:
    """Return 'Masked' when nullFlavor='MSK'; else trimmed text; else ''."""
    if elem is None:
        return ""
    nf = elem.attrib.get('nullFlavor', '')
    if nf == 'MSK':
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
        if local_name(el.tag) == 'code' and el.attrib.get('codeSystem') == FIRST_SENDER_OID:
            raw = (el.attrib.get('code') or "").strip()
            return FIRST_SENDER_MAP.get(raw, raw or "")
    return ""

def extract_report_source(root: ET.Element) -> str:
    """From codeSystem=…1.1.22, prefer displayName; else code."""
    for el in root.iter():
        if local_name(el.tag) == 'code' and el.attrib.get('codeSystem') == REPORT_SOURCE_OID:
            return el.attrib.get('displayName') or el.attrib.get('code') or ""
    return ""

def extract_td_frd_lrd(root: ET.Element) -> Dict[str, str]:
    out = {"TD_raw":"", "TD":"", "FRD_raw":"", "FRD":"", "LRD_raw":"", "LRD":""}
    for p in TD_PATHS:
        e = find_first(root, p)
        if e is not None:
            val = e.attrib.get('value') or get_text(e)
            if val:
                out["TD_raw"] = val; out["TD"] = format_date(val)
                break
    for el in root.iter():
        if local_name(el.tag) == 'availabilityTime':
            v = el.attrib.get('value')
            if v:
                out["LRD_raw"] = v; out["LRD"] = format_date(v); break
    # FRD: earliest <low>
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

def extract_patient(root: ET.Element) -> Dict[str, str]:
    gender_elem = find_first(root, './/hl7:administrativeGenderCode')
    gender_code = gender_elem.attrib.get('code', '') if gender_elem is not None else ''
    gender = clean_value(map_gender(gender_code))

    age_elem = find_first(root, './/hl7:code[@displayName="age"]/../hl7:value')
    age_val = age_elem.attrib.get('value','') if age_elem is not None else ''
    age_unit_raw = age_elem.attrib.get('unit','') if age_elem is not None else ''
    unit = {'a':'year', 'b':'month'}.get(str(age_unit_raw).lower(), age_unit_raw)
    age = f"{clean_value(age_val)}{(' ' + clean_value(unit)) if clean_value(age_val) and clean_value(unit) else ''}".strip()

    age_group_map = {"0":"Foetus","1":"Neonate","2":"Infant","3":"Child","4":"Adolescent","5":"Adult","6":"Elderly"}
    ag_elem = find_first(root, './/hl7:code[@displayName="ageGroup"]/../hl7:value')
    age_group = ""
    if ag_elem is not None:
        c = ag_elem.attrib.get('code','')
        nf = ag_elem.attrib.get('nullFlavor','')
        age_group = age_group_map.get(c, "[Masked/Unknown]" if (c in ["MSK","UNK","ASKU","NI"] or nf in ["MSK","UNK","ASKU","NI"]) else "")

    weight_elem = find_first(root, './/hl7:code[@displayName="bodyWeight"]/../hl7:value')
    weight = ""
    if weight_elem is not None:
        wv = clean_value(weight_elem.attrib.get('value',''))
        wu = clean_value(weight_elem.attrib.get('unit',''))
        weight = f"{wv}{(' ' + wu) if wv and wu else ''}"

    height_elem = find_first(root, './/hl7:code[@displayName="height"]/../hl7:value')
    height = ""
    if height_elem is not None:
        hv = clean_value(height_elem.attrib.get('value',''))
        hu = clean_value(height_elem.attrib.get('unit',''))
        height = f"{hv}{(' ' + hu) if hv and hu else ''}"

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
            initials = "".join(parts) or get_text(nm)
    return {
        "Gender": clean_value(gender), "Age": clean_value(age), "Age Group": clean_value(age_group),
        "Height": clean_value(height), "Weight": clean_value(weight), "Initials": clean_value(initials),
    }

def extract_suspect_ids(root: ET.Element) -> Set[str]:
    out = set()
    for c in findall(root, './/hl7:causalityAssessment'):
        v = find_first(c, './/hl7:value')
        if v is not None and v.attrib.get('code') == '1':
            sid = find_first(c, './/hl7:subject2/hl7:productUseReference/hl7:id')
            if sid is not None:
                out.add(sid.attrib.get('root',''))
    return out

def extract_products(root: ET.Element) -> List[Dict[str, str]]:
    suspects = extract_suspect_ids(root)
    products = []
    for drug in findall(root, './/hl7:substanceAdministration'):
        id_elem = find_first(drug, './/hl7:id')
        drug_id = id_elem.attrib.get('root','') if id_elem is not None else ''
        if drug_id in suspects:
            nm = find_first(drug, './/hl7:kindOfProduct/hl7:name')
            raw_name = ""
            if nm is not None:
                raw_name = (nm.text or "").strip() or clean_value(nm.attrib.get('displayName', ''))
                if not raw_name:
                    ot = nm.find('hl7:originalText', NS)
                    raw_name = get_text(ot)
            if not raw_name:
                alt = find_first(drug, './/hl7:manufacturedProduct/hl7:name')
                raw_name = get_text(alt)

            txt = get_text(find_first(drug, './/hl7:text'))
            dq = find_first(drug, './/hl7:doseQuantity')
            dose_v = dq.attrib.get('value','') if dq is not None else ''
            dose_u = dq.attrib.get('unit','') if dq is not None else ''

            low = find_first(drug, './/hl7:low'); high = find_first(drug, './/hl7:high')
            sd_raw = low.attrib.get('value','') if low is not None else ''
            ed_raw = high.attrib.get('value','') if high is not None else ''

            form = get_text(find_first(drug, './/hl7:formCode/hl7:originalText'))
            lot = get_text(find_first(drug, './/hl7:lotNumberText'))
            mah = ""
            for p in [
                './/hl7:playingOrganization/hl7:name',
                './/hl7:manufacturerOrganization/hl7:name',
                './/hl7:asManufacturedProduct/hl7:manufacturerOrganization/hl7:name',
            ]:
                node = find_first(drug, p)
                if node is not None and get_text(node):
                    mah = get_text(node); break

            products.append({
                "Drug": clean_value(raw_name),
                "Dosage Text": clean_value(txt),
                "Dose Value": clean_value(dose_v),
                "Dose Unit": clean_value(dose_u),
                "Start Date (raw)": sd_raw, "Start Date": format_date(sd_raw),
                "Stop Date (raw)": ed_raw, "Stop Date": format_date(ed_raw),
                "Formulation": clean_value(form),
                "Lot No": clean_value(lot),
                "MAH": clean_value(mah),
                "_key": normalize_text(raw_name) if raw_name else "",
            })
    return products

# -------- Reporter extraction (QC: robust + mask-aware) --------

def reporter_candidates(root: ET.Element) -> List[ET.Element]:
    """
    Likely reporter containers (avoid bare asQualifiedEntity).
    """
    cands: List[ET.Element] = []
    # Primary: author/assignedEntity (your snippet)
    cands += list(root.findall('.//hl7:author/hl7:assignedEntity', NS))
    # Also consider assignedAuthor and informant/assignedEntity
    cands += list(root.findall('.//hl7:author/hl7:assignedAuthor', NS))
    cands += list(root.findall('.//hl7:informant/hl7:assignedEntity', NS))
    # Generic fallbacks
    cands += list(root.findall('.//hl7:assignedAuthor', NS))
    cands += list(root.findall('.//hl7:assignedEntity', NS))
    # participant(AUT/INF)
    for p in root.findall('.//hl7:participant', NS):
        if (p.attrib.get('typeCode') or '').upper() in {'AUT','INF'}:
            cands.append(p)
    # De-duplicate while preserving order
    seen = set(); uniq = []
    for el in cands:
        key = id(el)
        if key not in seen:
            seen.add(key); uniq.append(el)
    return uniq

def extract_reporter_full(root: ET.Element) -> Dict[str, str]:
    result = {
        "Reporter Qualification": "",
        "Reporter IDs": "",
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
        "Report Source": "",  # from codeSystem …1.1.22 if present
    }

    cands = reporter_candidates(root)
    # Pull report source from anywhere
    result["Report Source"] = extract_report_source(root)

    if not cands:
        return result

    # Helper collectors bound to a container node
    def collect_ids(node: ET.Element) -> List[str]:
        ids = []
        for id_el in node.findall('.//hl7:id', NS):
            ext = (id_el.attrib.get('extension') or '').strip()
            rt  = (id_el.attrib.get('root') or '').strip()
            if ext and rt: ids.append(f"{ext} ({rt})")
            elif ext: ids.append(ext)
            elif rt: ids.append(rt)
        return list(dict.fromkeys(ids))

    def collect_qualification(node: ET.Element) -> str:
        # Map any code 1..5 under the container
        for code_el in node.iter():
            if local_name(code_el.tag) == 'code':
                c = (code_el.attrib.get('code') or '').strip()
                if c in REPORTER_MAP:
                    return REPORTER_MAP[c]
        return ""

    def collect_name(node: ET.Element) -> Tuple[str, List[str], str]:
        # Prefer assignedPerson/name
        name_el = node.find('.//hl7:assignedPerson/hl7:name', NS)
        if name_el is None:
            name_el = node.find('.//hl7:name', NS)
        given, family = [], ""
        if name_el is not None:
            # Mask-aware children
            for g in name_el.findall('hl7:given', NS):
                val = read_text_or_mask(g)
                if val: given.append(val)
            fam_el = name_el.find('hl7:family', NS)
            family = read_text_or_mask(fam_el)
        parts = [p for p in given if p] + ([family] if family else [])
        full = " ".join(parts).strip()
        # If entirely masked children and no text, still keep "Masked"
        if not full and name_el is not None:
            # Presence of any MSK child -> 'Masked'
            msks = any((ch.attrib.get('nullFlavor','') == 'MSK') for ch in name_el)
            full = "Masked" if msks else full
        return full, given, family

    def collect_org(node: ET.Element) -> str:
        org_paths = [
            './/hl7:assignedEntity/hl7:representedOrganization/hl7:name',
            './/hl7:representedOrganization/hl7:name',
            './/hl7:scopingOrganization/hl7:name',
        ]
        for p in org_paths:
            el = node.find(p, NS)
            if el is not None:
                txt = read_text_or_mask(el)
                if txt: return txt
        return ""

    def collect_addr_and_country(node: ET.Element) -> Tuple[str,str,str,str,str]:
        addr = node.find('.//hl7:addr', NS)
        street_lines = []
        city = state = postal = country = ""
        if addr is not None:
            for sl in addr.findall('hl7:streetAddressLine', NS):
                val = read_text_or_mask(sl)
                if val: street_lines.append(val)
            city   = read_text_or_mask(addr.find('hl7:city', NS))
            state  = read_text_or_mask(addr.find('hl7:state', NS))
            postal = read_text_or_mask(addr.find('hl7:postalCode', NS))
            country= read_text_or_mask(addr.find('hl7:country', NS))
        # Country fallback via asLocatedEntity/location/code@code (your snippet)
        if not country:
            loc = node.find('.//hl7:asLocatedEntity/hl7:location/hl7:code', NS)
            if loc is not None and loc.attrib.get('code'):
                country = loc.attrib.get('code').strip()
        return ", ".join(street_lines), city, state, postal, country

    def collect_telecom(node: ET.Element) -> Tuple[List[str], List[str], List[str]]:
        phones, emails, faxes = [], [], []
        for tel in node.findall('.//hl7:telecom', NS):
            val = (tel.attrib.get('value') or '').strip()
            use = (tel.attrib.get('use') or '').upper()
            if not val: continue
            vlow = val.lower()
            if vlow.startswith('mailto:'):
                emails.append(val.split(':',1)[1])
            elif 'FAX' in use or vlow.startswith('fax:'):
                faxes.append(val.split(':',1)[-1] if ':' in val else val)
            elif vlow.startswith('tel:') or vlow.startswith('tel;'):
                phones.append(val.split(':',1)[1] if ':' in val else val)
            else:
                if '@' in val:
                    emails.append(val.replace('mailto:', ''))
                elif len(re.sub(r'\D','',val)) >= 7:
                    phones.append(val)
                else:
                    phones.append(val)
        uniq = lambda L: list(dict.fromkeys([x for x in L if x]))
        return uniq(phones), uniq(emails), uniq(faxes)

    # Use the first container that yields any data
    for node in cands:
        ids = collect_ids(node)
        qual = collect_qualification(node)
        full, given, family = collect_name(node)
        org = collect_org(node)
        street, city, state, postal, country = collect_addr_and_country(node)
        phones, emails, faxes = collect_telecom(node)

        if any([ids, qual, full, org, street, city, state, postal, country, phones, emails, faxes]):
            result.update({
                "Reporter Qualification": qual,
                "Reporter IDs": "; ".join(ids),
                "Reporter Name (Full)": full,
                "Reporter Given Name(s)": "; ".join(given) if given else "",
                "Reporter Family Name": family,
                "Reporter Organization": org,
                "Reporter Street": street,
                "Reporter City/Town": city,
                "Reporter State/Province": state,
                "Reporter Postal Code": postal,
                "Reporter Country": country,
                "Reporter Phone(s)": "; ".join(phones) if phones else "",
                "Reporter Email(s)": "; ".join(emails) if emails else "",
                "Reporter Fax(es)": "; ".join(faxes) if faxes else "",
            })
            break

    return result

def extract_narrative(root: ET.Element) -> str:
    narrative_elem = root.find('.//hl7:code[@code="PAT_ADV_EVNT"]/../hl7:text', NS)
    return clean_value(narrative_elem.text if narrative_elem is not None else '')

def extract_model(xml_bytes: bytes) -> Dict[str, Any]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        return {"_error": f"XML parse error: {e}"}
    model: Dict[str, Any] = {}
    # Admin
    model["Sender ID"] = extract_sender_id(root)
    model["WWID"] = extract_wwid(root)
    model["First Sender Type"] = extract_first_sender_type(root)
    model.update(extract_td_frd_lrd(root))
    # Reporter (QC: robust)
    model["Reporter"] = extract_reporter_full(root)
    # Patient / Products / Events / Narrative
    model["Patient"] = extract_patient(root)
    model["Products"] = extract_products(root)
    model["Events"] = extract_events(root)
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

def make_reporter_table(src: Dict[str,str], prc: Dict[str,str]) -> pd.DataFrame:
    fields = [
        "Report Source",               # NEW
        "Reporter Qualification",
        "Reporter IDs",
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
    rows = [(f, src.get(f,""), prc.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

def make_patient_table(src: Dict[str,str], prc: Dict[str,str]) -> pd.DataFrame:
    fields = ["Gender","Age","Age Group","Height","Weight","Initials"]
    rows = [(f, src.get(f,""), prc.get(f,"")) for f in fields]
    return compare_table(rows, treat_as_dates=False)

def dict_by_key(items: List[Dict[str,Any]]) -> Dict[str, Dict[str,Any]]:
    return {it.get("_key",""): it for it in items if it.get("_key","")}

def make_product_box(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
    st.markdown(f'<div class="box"><h5>Drug: {title}</h5>', unsafe_allow_html=True)
    fields = [
        ("Dosage Text","text"),
        ("Dose Value","text"),
        ("Dose Unit","text"),
        ("Start Date","date"),
        ("Stop Date","date"),
        ("Formulation","text"),
        ("Lot No","text"),
        ("MAH","text"),
    ]
    rows = []
    for field, kind in fields:
        s_val = src_rec.get(field,"")
        p_val = prc_rec.get(field,"")
        if kind == "date":
            s_val = s_val or format_date(src_rec.get(field + " (raw)",""))
            p_val = p_val or format_date(prc_rec.get(field + " (raw)",""))
        rows.append((field, s_val, p_val))
    df = compare_table(rows, treat_as_dates=True)
    if df.empty:
        st.markdown('<div class="smallnote">No values to display for this drug in either file.</div>', unsafe_allow_html=True)
    else:
        st.table(df)
    st.markdown('</div>', unsafe_allow_html=True)

def make_event_box(src_rec: Dict[str,Any], prc_rec: Dict[str,Any], title: str):
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
    df = compare_table(rows, treat_as_dates=True)
    if df.empty:
        st.markdown('<div class="smallnote">No values to display for this event in either file.</div>', unsafe_allow_html=True)
    else:
        st.table(df)
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------- UI: Upload & Parse ----------------
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
    src = extract_model(src_bytes)
with st.spinner("Parsing Processed..."):
    prc = extract_model(prc_bytes)

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

# ---------------- SECTION: Reporter (QC – full details) ----------------
st.subheader("Reporter")
rep_df = make_reporter_table(src.get("Reporter",{}), prc.get("Reporter",{}))
if not rep_df.empty:
    st.table(rep_df)
else:
    st.markdown('<div class="box smallnote">No reporter details present in either file.</div>', unsafe_allow_html=True)

# ---------------- SECTION: Patient Details ----------------
st.subheader("Patient Details")
pat_df = make_patient_table(src.get("Patient",{}), prc.get("Patient",{}))
if not pat_df.empty:
    st.table(pat_df)
else:
    st.markdown('<div class="box smallnote">No patient values present in either file.</div>', unsafe_allow_html=True)

# ---------------- SECTION: Drug Details (matched by drug name) ----------------
st.subheader("Drug Details (suspects) — matched by drug name")
src_prods = src.get("Products", [])
prc_prods = prc.get("Products", [])
src_idx = dict_by_key(src_prods)
prc_idx = dict_by_key(prc_prods)
all_keys = sorted(set(src_idx) | set(prc_idx))
if not all_keys:
    st.markdown('<div class="box smallnote">No suspect products found in either file.</div>', unsafe_allow_html=True)
else:
    for key in all_keys:
        srec = src_idx.get(key, {"Drug": ""})
        prec = prc_idx.get(key, {"Drug": ""})
        title = srec.get("Drug") or prec.get("Drug") or "(Unnamed drug)"
        make_product_box(srec, prec, title)

# ---------------- SECTION: Event Details (matched by LLT Code then term) ----------------
st.subheader("Event Details — matched by LLT code (fallback: normalized term)")
src_evts = src.get("Events", [])
prc_evts = prc.get("Events", [])
def idx_events(lst: List[Dict[str,Any]]) -> Dict[str,Dict[str,Any]]:
    return {e.get("_key",""): e for e in lst if e.get("_key","")}
src_evt_idx = idx_events(src_evts)
prc_evt_idx = idx_events(prc_evts)
all_evt_keys = sorted(set(src_evt_idx) | set(prc_evt_idx))
if not all_evt_keys:
    st.markdown('<div class="box smallnote">No events found in either file.</div>', unsafe_allow_html=True)
else:
    for key in all_evt_keys:
        se = src_evt_idx.get(key, {"LLT Term": ""})
        pe = prc_evt_idx.get(key, {"LLT Term": ""})
        title = se.get("LLT Term") or pe.get("LLT Term") or (se.get("LLT Code") or pe.get("LLT Code") or "(Unnamed event)")
        make_event_box(se, pe, title)

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
    st.markdown('<div class="box"><h5>Source</h5>', unsafe_allow_html=True)
    st.code(src_narr or "—")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="box"><h5>Processed</h5>', unsafe_allow_html=True)
    st.code((prc_narr or "—") + (" 🔴" if (src_narr_full != prc_narr_full) else ""))
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------- Export (robust Excel engine handling) ----------------
st.markdown("---")
st.markdown("### ⬇️ Download Comparison (Excel)")

def rows_from_table(df: pd.DataFrame, section: str) -> List[Dict[str,str]]:
    if df is None or df.empty: return []
    return [{"Section": section, "Field": r["Field"], "Source": r["Source"], "Processed": r["Processed"]} for _, r in df.iterrows()]

admin_rows = rows_from_table(admin_df, "Admin/Header")
reporter_rows = rows_from_table(rep_df, "Reporter")
pat_rows = rows_from_table(pat_df, "Patient")

# Drugs sheet
prod_rows = []
for key in all_keys:
    srec = src_idx.get(key, {})
    prec = prc_idx.get(key, {})
    title = srec.get("Drug") or prec.get("Drug") or "(Unnamed drug)"
    for field in ["Dosage Text","Dose Value","Dose Unit","Start Date","Stop Date","Formulation","Lot No","MAH"]:
        s_val = srec.get(field, "") or (format_date(srec.get(field + " (raw)","")) if "Date" in field else "")
        p_val = prec.get(field, "") or (format_date(prec.get(field + " (raw)","")) if "Date" in field else "")
        if has_value(s_val) or has_value(p_val):
            prod_rows.append({"Section":"Drug", "Group": title, "Field": field, "Source": s_val or "—", "Processed": p_val or "—"})

# Events sheet
evt_rows = []
for key in all_evt_keys:
    se = src_evt_idx.get(key, {})
    pe = prc_evt_idx.get(key, {})
    title = se.get("LLT Term") or pe.get("LLT Term") or (se.get("LLT Code") or pe.get("LLT Code") or "(Unnamed event)")
    for field in ["LLT Code","LLT Term","Seriousness","Outcome","Event Start","Event End"]:
        s_val = se.get(field, "") or (format_date(se.get(field + " (raw)","")) if "Event" in field else "")
        p_val = pe.get(field, "") or (format_date(pe.get(field + " (raw)","")) if "Event" in field else "")
        if has_value(s_val) or has_value(p_val):
            evt_rows.append({"Section":"Event", "Group": title, "Field": field, "Source": s_val or "—", "Processed": p_val or "—"})

def build_excel_bytes() -> Optional[bytes]:
    sheets: Dict[str, pd.DataFrame] = {}
    sheets["Admin_Reporter_Patient"] = pd.DataFrame(admin_rows + reporter_rows + pat_rows)
    if prod_rows: sheets["Drugs"] = pd.DataFrame(prod_rows)
    if evt_rows: sheets["Events"] = pd.DataFrame(evt_rows)
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
