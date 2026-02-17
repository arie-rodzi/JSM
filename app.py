import re
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Malaysia Standards Dashboard (MS)"
DB_PATH = Path("ms_dashboard.sqlite3")

# Master file sheets
DEFAULT_SHEET_MASTER = "MS Collection"
DEFAULT_SHEET_NSC = "List of NSC"


# -----------------------------
# Helpers
# -----------------------------
def parse_nsc(nsc_text: str):
    """Extract NSC_CODE (NSC 05), NSC_NO (5), NSC_NAME (text after '-')"""
    if pd.isna(nsc_text):
        return None, None, None
    s = str(nsc_text).strip()
    m = re.search(r"\bNSC\s*(\d{1,2})\b", s, flags=re.IGNORECASE)
    nsc_no = int(m.group(1)) if m else None
    nsc_code = f"NSC {nsc_no:02d}" if nsc_no is not None else None
    name = None
    if "-" in s:
        name = s.split("-", 1)[1].strip()
    return nsc_code, nsc_no, name


def canonical_ms_code(x) -> str:
    """
    Robust MS key extraction from messy strings (both master & mandatory files).
    Example -> 'MS 1040:1986', 'MS 1742-2:2004', 'MS IEC 61851-21-1:2021'
    """
    if pd.isna(x):
        return ""
    raw = str(x).upper().strip()

    # remove bracketed notes
    raw = re.sub(r"\(.*?\)", "", raw)
    raw = raw.replace(",", " ")
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(r"\s*-\s*", "-", raw)
    raw = re.sub(r"\s*:\s*", ":", raw)

    # Capture starting from MS up to a year (optional)
    # Allows prefixes like "MS IEC", "MS ISO/IEC", etc.
    pat = r"\bMS\b\s*(?:[A-Z/\.]{2,20}\s+)*[0-9A-Z][0-9A-Z./-]*(?:-[0-9A-Z./-]+)*(:\d{4})?"
    m = re.search(pat, raw)
    if not m:
        return ""

    code = m.group(0).strip()
    code = re.sub(r"\bMS\b\s*", "MS ", code).strip()
    code = re.sub(r"\s+", " ", code)

    # If year missing in captured code but appears later in raw, append it
    if not re.search(r":\d{4}\b", code):
        y = re.search(r":(\d{4})\b", raw)
        if y:
            code = code + (y.group(1) if code.endswith(":") else f":{y.group(1)}")

    return code


def safe_get(df: pd.DataFrame, col: str):
    return df[col] if col in df.columns else None


def read_mandatory_sheet(excel_file) -> pd.DataFrame:
    """
    Auto-detect the sheet in mandatory file that contains 'No. of MS' column.
    Also strips all column names.
    """
    try:
        xls = pd.ExcelFile(excel_file)
    except Exception:
        return pd.DataFrame()

    target = None
    for sh in xls.sheet_names:
        try:
            df0 = pd.read_excel(excel_file, sheet_name=sh, nrows=5)
        except Exception:
            continue
        cols = [str(c).strip() for c in df0.columns]
        if any(c.lower() == "no. of ms" for c in cols):
            target = sh
            break

    if target is None:
        return pd.DataFrame()

    mand = pd.read_excel(excel_file, sheet_name=target)
    mand.columns = [str(c).strip() for c in mand.columns]  # IMPORTANT
    return mand


def connect_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def ensure_tables(conn: sqlite3.Connection, force_recreate: bool = False):
    """
    IMPORTANT: if force_recreate=True, drops tables so schema updates won't crash.
    """
    cur = conn.cursor()

    if force_recreate:
        cur.execute("DROP TABLE IF EXISTS ms_master")
        cur.execute("DROP TABLE IF EXISTS nsc_directory")
        conn.commit()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ms_master (
            row_id INTEGER PRIMARY KEY,
            type_of_ms TEXT,
            ms_number TEXT,
            ms_title TEXT,
            pages TEXT,
            price_rm TEXT,
            ms_status TEXT,
            nsc_raw TEXT,
            nsc_code TEXT,
            nsc_no INTEGER,
            nsc_name TEXT,
            ms_new_old_number TEXT,

            compliance_type TEXT,
            agency TEXT,
            refer TEXT,
            act TEXT,
            regulation TEXT,
            directive_guideline TEXT,
            date_of_enforcement TEXT,
            remarks_new_version TEXT,

            ms_key TEXT,
            ms_old_key TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nsc_directory (
            nsc_code TEXT PRIMARY KEY,
            nsc_full TEXT,
            project_manager TEXT,
            email TEXT,
            office_number TEXT,
            cfs TEXT
        )
        """
    )
    conn.commit()


def load_excel_to_db(master_excel_file, conn: sqlite3.Connection, mandatory_excel_file=None):
    """
    Ingest master Excel + optional mandatory Excel into SQLite, replacing existing data.
    """
    master = pd.read_excel(master_excel_file, sheet_name=DEFAULT_SHEET_MASTER)
    nsc_dir = pd.read_excel(master_excel_file, sheet_name=DEFAULT_SHEET_NSC)

    master = master.copy()
    master["row_id"] = range(1, len(master) + 1)

    # Ensure columns exist (safe get)
    master["TYPE OF MS"] = master.get("TYPE OF MS")
    master["MS NUMBER"] = master.get("MS NUMBER")
    master["MS TITLE"] = master.get("MS TITLE")
    master["PAGES"] = master.get("PAGES")
    master["PRICE (RM)"] = master.get("PRICE (RM)")
    master["MS STATUS"] = master.get("MS STATUS")
    master["NATIONAL STANDARDS COMMITTEE (NSC)"] = master.get("NATIONAL STANDARDS COMMITTEE (NSC)")
    master["MS NEW/OLD NUMBER"] = master.get("MS NEW/OLD NUMBER")

    # Parse NSC
    parsed = master["NATIONAL STANDARDS COMMITTEE (NSC)"].apply(parse_nsc)
    master["NSC_CODE"] = parsed.apply(lambda x: x[0])
    master["NSC_NO"] = parsed.apply(lambda x: x[1])
    master["NSC_NAME"] = parsed.apply(lambda x: x[2])

    # Build matching keys
    master["ms_key"] = master["MS NUMBER"].apply(canonical_ms_code)
    master["ms_old_key"] = master["MS NEW/OLD NUMBER"].apply(canonical_ms_code)

    # Defaults
    master["compliance_type"] = "Voluntary"
    master["agency"] = None
    master["refer"] = None
    master["act"] = None
    master["regulation"] = None
    master["directive_guideline"] = None
    master["date_of_enforcement"] = None
    master["remarks_new_version"] = None

    # --- Merge mandatory info (if provided)
    if mandatory_excel_file is not None:
        mand = read_mandatory_sheet(mandatory_excel_file)

        if mand.empty or ("No. of MS" not in mand.columns):
            st.warning("Mandatory file uploaded but cannot find column 'No. of MS' in any sheet.")
        else:
            mand = mand.copy()
            mand["ms_key"] = mand["No. of MS"].apply(canonical_ms_code)
            mand = mand[mand["ms_key"] != ""].copy()

            reg_col = (
                "Regulation \n"
                if "Regulation \n" in mand.columns
                else ("Regulation" if "Regulation" in mand.columns else None)
            )

            mand_out = pd.DataFrame(
                {
                    "ms_key": mand["ms_key"],
                    "refer": safe_get(mand, "Refer"),
                    "agency": safe_get(mand, "Agency"),
                    "act": safe_get(mand, "Act"),
                    "regulation": mand[reg_col] if reg_col else None,
                    "directive_guideline": safe_get(mand, "Directive / Circulars / Guideline"),
                    "date_of_enforcement": safe_get(mand, "Date of Enforcement"),
                    "remarks_new_version": safe_get(mand, "Remarks / New Version"),
                }
            ).drop_duplicates(subset=["ms_key"], keep="first")

            # 1) Match by current MS number
            merged1 = master.merge(mand_out, on="ms_key", how="left", suffixes=("", "_m"))

            # 2) Fallback match using old MS number (MS NEW/OLD NUMBER)
            mand_out2 = mand_out.rename(columns={"ms_key": "ms_old_key"})
            merged2 = merged1.merge(mand_out2, on="ms_old_key", how="left", suffixes=("", "_old"))

            def coalesce(a, b):
                return a.where(a.notna(), b)

            merged2["agency_final"] = coalesce(merged2["agency"], merged2.get("agency_old"))
            merged2["refer_final"] = coalesce(merged2["refer"], merged2.get("refer_old"))
            merged2["act_final"] = coalesce(merged2["act"], merged2.get("act_old"))
            merged2["regulation_final"] = coalesce(merged2["regulation"], merged2.get("regulation_old"))
            merged2["directive_guideline_final"] = coalesce(
                merged2["directive_guideline"], merged2.get("directive_guideline_old")
            )
            merged2["date_of_enforcement_final"] = coalesce(
                merged2["date_of_enforcement"], merged2.get("date_of_enforcement_old")
            )
            merged2["remarks_new_version_final"] = coalesce(
                merged2["remarks_new_version"], merged2.get("remarks_new_version_old")
            )

            matched = (
                merged2["agency_final"].notna()
                | merged2["act_final"].notna()
                | merged2["regulation_final"].notna()
                | merged2["refer_final"].notna()
            )
            merged2.loc[matched, "compliance_type"] = "Mandatory"

            master["compliance_type"] = merged2["compliance_type"]
            master["agency"] = merged2["agency_final"]
            master["refer"] = merged2["refer_final"]
            master["act"] = merged2["act_final"]
            master["regulation"] = merged2["regulation_final"]
            master["directive_guideline"] = merged2["directive_guideline_final"]
            master["date_of_enforcement"] = merged2["date_of_enforcement_final"]
            master["remarks_new_version"] = merged2["remarks_new_version_final"]

    master_out = pd.DataFrame(
        {
            "row_id": master["row_id"],
            "type_of_ms": master["TYPE OF MS"],
            "ms_number": master["MS NUMBER"],
            "ms_title": master["MS TITLE"],
            "pages": master["PAGES"],
            "price_rm": master["PRICE (RM)"],
            "ms_status": master["MS STATUS"],
            "nsc_raw": master["NATIONAL STANDARDS COMMITTEE (NSC)"],
            "nsc_code": master["NSC_CODE"],
            "nsc_no": master["NSC_NO"],
            "nsc_name": master["NSC_NAME"],
            "ms_new_old_number": master["MS NEW/OLD NUMBER"],
            "compliance_type": master["compliance_type"],
            "agency": master["agency"],
            "refer": master["refer"],
            "act": master["act"],
            "regulation": master["regulation"],
            "directive_guideline": master["directive_guideline"],
            "date_of_enforcement": master["date_of_enforcement"],
            "remarks_new_version": master["remarks_new_version"],
            "ms_key": master["ms_key"],
            "ms_old_key": master["ms_old_key"],
        }
    )
    master_out.to_sql("ms_master", conn, if_exists="append", index=False)

    # Insert NSC directory
    if "NSC" in nsc_dir.columns:
        nsc_dir = nsc_dir.copy()
        nsc_dir["nsc_code"] = nsc_dir["NSC"].apply(lambda s: parse_nsc(s)[0] if pd.notna(s) else None)
        nsc_dir = nsc_dir.dropna(subset=["nsc_code"])

        nsc_dir_out = pd.DataFrame(
            {
                "nsc_code": nsc_dir["nsc_code"],
                "nsc_full": nsc_dir["NSC"],
                "project_manager": nsc_dir.get("Project Manager"),
                "email": nsc_dir.get("E-mail"),
                "office_number": nsc_dir.get("Office Number"),
                "cfs": nsc_dir.get("CFS"),
            }
        )
        nsc_dir_out.to_sql("nsc_directory", conn, if_exists="append", index=False)

    conn.commit()


@st.cache_data(show_spinner=False)
def db_has_data() -> bool:
    if not DB_PATH.exists():
        return False
    conn = connect_db()
    try:
        ensure_tables(conn, force_recreate=False)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ms_master")
        n = cur.fetchone()[0]
        return n > 0
    finally:
        conn.close()


def read_master(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM ms_master", conn)


def read_nsc_dir(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM nsc_directory", conn)


def update_status(conn: sqlite3.Connection, updates: pd.DataFrame):
    """updates: DataFrame with columns row_id, ms_status"""
    cur = conn.cursor()
    for _, r in updates.iterrows():
        cur.execute(
            "UPDATE ms_master SET ms_status=? WHERE row_id=?",
            (r["ms_status"], int(r["row_id"])),
        )
    conn.commit()


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.header("Data Source")

    st.caption("1) Upload **Master List** Excel (must contain sheets: MS Collection, List of NSC).")
    uploaded_master = st.file_uploader("Upload Master List (.xlsx)", type=["xlsx"], key="master")

    st.caption("2) Upload **Mandatory/Agency** Excel (auto-detect sheet with column 'No. of MS').")
    uploaded_mand = st.file_uploader("Upload Mandatory List (.xlsx)", type=["xlsx"], key="mand")

    col_a, col_b = st.columns(2)
    with col_a:
        do_refresh = st.button("Refresh DB", use_container_width=True, disabled=(uploaded_master is None))
    with col_b:
        reset_cache = st.button("Reset App Cache", use_container_width=True)

    st.divider()
    st.caption("Troubleshooting (schema changes)")
    hard_reset = st.button("Hard Reset DB (Drop tables)", use_container_width=True)

    if reset_cache:
        st.cache_data.clear()
        st.toast("Cache cleared.", icon="✅")


conn = connect_db()

# If user clicks hard reset
if hard_reset:
    ensure_tables(conn, force_recreate=True)
    st.cache_data.clear()
    st.success("Database schema reset. Now upload Excel(s) and click Refresh DB.")

# Normal ensure (no drop)
ensure_tables(conn, force_recreate=False)

# Refresh DB flow (DROP+RECREATE to avoid schema mismatch)
if do_refresh and uploaded_master is not None:
    with st.spinner("Importing Excel into local database..."):
        ensure_tables(conn, force_recreate=True)  # IMPORTANT: avoids sqlite missing-column errors
        load_excel_to_db(uploaded_master, conn, mandatory_excel_file=uploaded_mand)
        st.cache_data.clear()
    st.success("Database refreshed from uploaded Excel(s).")

if not db_has_data():
    st.warning("No data in the dashboard database yet. Upload Master List and click **Refresh DB**.")
    st.stop()

master = read_master(conn)
nsc_dir = read_nsc_dir(conn)

# -----------------------------
# Filters
# -----------------------------
st.subheader("Filters")
c1, c2, c3, c4, c5, c6 = st.columns([2, 2, 2, 2, 3, 3])

with c1:
    nsc_codes = sorted([c for c in master["nsc_code"].dropna().unique().tolist()])
    nsc_sel = st.multiselect("Filter by NSC", options=nsc_codes, default=[])

with c2:
    statuses = sorted([s for s in master["ms_status"].fillna("Unknown").unique().tolist()])
    default_status = ["Original"] if "Original" in statuses else []
    status_sel = st.multiselect("Filter by MS Status", options=statuses, default=default_status)

with c3:
    type_options = sorted([t for t in master["type_of_ms"].fillna("Unknown").unique().tolist()])
    type_sel = st.multiselect("Filter by TYPE OF MS", options=type_options, default=[])

with c4:
    comp_opts = sorted([t for t in master["compliance_type"].fillna("Voluntary").unique().tolist()])
    comp_sel = st.multiselect("Mandatory / Voluntary", options=comp_opts, default=[])

with c5:
    agency_opts = sorted([a for a in master["agency"].fillna("Unknown").unique().tolist()])
    agency_opts = [a for a in agency_opts if a != "Unknown"] + (["Unknown"] if "Unknown" in agency_opts else [])
    agency_sel = st.multiselect("Filter by Agency", options=agency_opts, default=[])

with c6:
    search = st.text_input("Search (MS Number / Title contains)", value="")

# Apply filters
filtered = master.copy()
filtered["ms_status"] = filtered["ms_status"].fillna("Unknown")
filtered["type_of_ms"] = filtered["type_of_ms"].fillna("Unknown")
filtered["compliance_type"] = filtered["compliance_type"].fillna("Voluntary")
filtered["agency"] = filtered["agency"].fillna("Unknown")

if nsc_sel:
    filtered = filtered[filtered["nsc_code"].isin(nsc_sel)]
if status_sel:
    filtered = filtered[filtered["ms_status"].isin(status_sel)]
if type_sel:
    filtered = filtered[filtered["type_of_ms"].isin(type_sel)]
if comp_sel:
    filtered = filtered[filtered["compliance_type"].isin(comp_sel)]
if agency_sel:
    filtered = filtered[filtered["agency"].isin(agency_sel)]

if search.strip():
    s = search.strip().lower()
    filtered = filtered[
        filtered["ms_number"].fillna("").str.lower().str.contains(s)
        | filtered["ms_title"].fillna("").str.lower().str.contains(s)
    ]

# -----------------------------
# KPIs
# -----------------------------
st.subheader("Key Metrics")
k1, k2, k3, k4, k5, k6, k7 = st.columns(7)

total_all = int(master.shape[0])
total_filtered = int(filtered.shape[0])
unique_ms_all = int(master["ms_number"].nunique())
unique_ms_filtered = int(filtered["ms_number"].nunique())
unique_type_filtered = int(filtered["type_of_ms"].nunique())
mandatory_filtered = int((filtered["compliance_type"] == "Mandatory").sum())
unique_agency_filtered = int(filtered["agency"].nunique())

k1.metric("Rows (All)", f"{total_all:,}")
k2.metric("Rows (Filtered)", f"{total_filtered:,}")
k3.metric("Unique MS (All)", f"{unique_ms_all:,}")
k4.metric("Unique MS (Filtered)", f"{unique_ms_filtered:,}")
k5.metric("Unique TYPE (Filtered)", f"{unique_type_filtered:,}")
k6.metric("Mandatory (Filtered)", f"{mandatory_filtered:,}")
k7.metric("Agencies (Filtered)", f"{unique_agency_filtered:,}")

# -----------------------------
# Charts
# -----------------------------
st.subheader("Distributions (Filtered)")
cc1, cc2, cc3, cc4 = st.columns(4)

with cc1:
    st.caption("Count by Status")
    status_counts = filtered["ms_status"].value_counts().reset_index()
    status_counts.columns = ["ms_status", "count"]
    st.bar_chart(status_counts.set_index("ms_status"))

with cc2:
    st.caption("Count by NSC")
    nsc_counts = filtered["nsc_code"].fillna("Unknown").value_counts().reset_index()
    nsc_counts.columns = ["nsc_code", "count"]
    st.bar_chart(nsc_counts.set_index("nsc_code"))

with cc3:
    st.caption("Count by TYPE OF MS")
    type_counts = filtered["type_of_ms"].fillna("Unknown").value_counts().reset_index()
    type_counts.columns = ["type_of_ms", "count"]
    st.bar_chart(type_counts.set_index("type_of_ms"))

with cc4:
    st.caption("Mandatory vs Voluntary")
    comp_counts = filtered["compliance_type"].fillna("Voluntary").value_counts().reset_index()
    comp_counts.columns = ["compliance_type", "count"]
    st.bar_chart(comp_counts.set_index("compliance_type"))

# -----------------------------
# NSC Directory
# -----------------------------
with st.expander("NSC Directory (Project Manager / Contacts)", expanded=False):
    if nsc_dir.empty:
        st.info("NSC directory not found in the uploaded master file.")
    else:
        show_dir = nsc_dir.copy()
        if nsc_sel:
            show_dir = show_dir[show_dir["nsc_code"].isin(nsc_sel)]
        st.dataframe(show_dir, use_container_width=True, hide_index=True)

# -----------------------------
# Data Table + Optional Editing
# -----------------------------
st.subheader("Data Table")
edit_mode = st.toggle("Enable edit mode (Update MS Status)", value=False)

display_cols = [
    "row_id",
    "ms_number",
    "ms_title",
    "type_of_ms",
    "ms_status",
    "compliance_type",
    "agency",
    "act",
    "regulation",
    "directive_guideline",
    "date_of_enforcement",
    "remarks_new_version",
    "nsc_code",
    "nsc_name",
    "pages",
    "price_rm",
    "ms_new_old_number",
]

table = filtered[display_cols].copy()

if not edit_mode:
    st.dataframe(table, use_container_width=True, hide_index=True)
else:
    st.info("Edit **MS Status** and click **Save changes**. Changes are stored in local SQLite.")
    edited = st.data_editor(
        table,
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in table.columns if c not in ["ms_status"]],
        key="editor",
    )

    if st.button("Save changes", type="primary"):
        merged = table[["row_id", "ms_status"]].merge(
            edited[["row_id", "ms_status"]],
            on="row_id",
            suffixes=("_old", "_new"),
        )
        diff = merged[merged["ms_status_old"] != merged["ms_status_new"]]
        if diff.empty:
            st.warning("No changes detected.")
        else:
            updates = diff.rename(columns={"ms_status_new": "ms_status"})[["row_id", "ms_status"]]
            update_status(conn, updates)
            st.cache_data.clear()
            st.success(f"Saved {len(updates)} update(s) to the database.")

# -----------------------------
# Export
# -----------------------------
st.subheader("Export")
csv = filtered[display_cols].to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download filtered data (CSV)",
    data=csv,
    file_name="ms_filtered.csv",
    mime="text/csv",
)

conn.close()
