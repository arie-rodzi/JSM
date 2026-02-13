import re
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

APP_TITLE = "Malaysia Standards Dashboard (MS)"
DB_PATH = Path("ms_dashboard.sqlite3")

DEFAULT_SHEET_MASTER = "MS Collection"
DEFAULT_SHEET_NSC = "List of NSC"


# -----------------------------
# Helpers
# -----------------------------
def parse_nsc(nsc_text: str):
    """
    Extract:
      - nsc_code: 'NSC 05'
      - nsc_no  : 5
      - nsc_name: description after '-'
    """
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


def connect_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
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
            ms_new_old_number TEXT
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


def load_excel_to_db(excel_file, conn: sqlite3.Connection):
    """
    Ingest excel sheets into SQLite, replacing existing data.
    """
    master = pd.read_excel(excel_file, sheet_name=DEFAULT_SHEET_MASTER)
    nsc_dir = pd.read_excel(excel_file, sheet_name=DEFAULT_SHEET_NSC)

    master = master.copy()
    master["row_id"] = range(1, len(master) + 1)

    # Ensure columns exist (safe get)
    master["TYPE OF MS"] = master.get("TYPE OF MS")
    master["MS NUMBER"] = master.get("MS NUMBER")
    master["MS TITLE"] = master.get("MS TITLE")
    master["PAGES"] = master.get("PAGES")
    master["PRICE (RM)"] = master.get("PRICE (RM)")
    master["MS STATUS"] = master.get("MS STATUS")
    master["NATIONAL STANDARDS COMMITTEE (NSC)"] = master.get(
        "NATIONAL STANDARDS COMMITTEE (NSC)"
    )
    master["MS NEW/OLD NUMBER"] = master.get("MS NEW/OLD NUMBER")

    parsed = master["NATIONAL STANDARDS COMMITTEE (NSC)"].apply(parse_nsc)
    master["NSC_CODE"] = parsed.apply(lambda x: x[0])
    master["NSC_NO"] = parsed.apply(lambda x: x[1])
    master["NSC_NAME"] = parsed.apply(lambda x: x[2])

    # Replace tables
    cur = conn.cursor()
    cur.execute("DELETE FROM ms_master")
    cur.execute("DELETE FROM nsc_directory")
    conn.commit()

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
        }
    )
    master_out.to_sql("ms_master", conn, if_exists="append", index=False)

    # Insert NSC directory (if present)
    if "NSC" in nsc_dir.columns:
        nsc_dir = nsc_dir.copy()
        nsc_dir["nsc_code"] = nsc_dir["NSC"].apply(
            lambda s: parse_nsc(s)[0] if pd.notna(s) else None
        )
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
        ensure_tables(conn)
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
    """
    updates: DataFrame with columns row_id, ms_status
    """
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
    st.caption("Upload the latest master list Excel to refresh the dashboard database.")
    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

    col_a, col_b = st.columns(2)
    with col_a:
        do_refresh = st.button(
            "Refresh DB", use_container_width=True, disabled=(uploaded is None)
        )
    with col_b:
        reset = st.button("Reset App Cache", use_container_width=True)

    if reset:
        st.cache_data.clear()
        st.toast("Cache cleared.", icon="✅")

conn = connect_db()
ensure_tables(conn)

if do_refresh and uploaded is not None:
    with st.spinner("Importing Excel into local database..."):
        load_excel_to_db(uploaded, conn)
        st.cache_data.clear()
    st.success("Database refreshed from uploaded Excel.")

if not db_has_data():
    st.warning("No data in the dashboard database yet. Upload the Excel file and click **Refresh DB**.")
    st.stop()

master = read_master(conn)
nsc_dir = read_nsc_dir(conn)

# -----------------------------
# Filters (NSC + Status + TYPE OF MS)
# -----------------------------
st.subheader("Filters")
c1, c2, c3, c4 = st.columns([2, 2, 2, 3])

with c1:
    nsc_codes = sorted([c for c in master["nsc_code"].dropna().unique().tolist()])
    nsc_sel = st.multiselect("Filter by NSC", options=nsc_codes, default=[])

with c2:
    statuses = sorted([s for s in master["ms_status"].fillna("Unknown").unique().tolist()])
    # keep your previous default behavior (try Original)
    default_status = ["Original"] if "Original" in statuses else []
    status_sel = st.multiselect("Filter by MS Status", options=statuses, default=default_status)

with c3:
    type_options = sorted([t for t in master["type_of_ms"].fillna("Unknown").unique().tolist()])
    type_sel = st.multiselect("Filter by TYPE OF MS", options=type_options, default=[])

with c4:
    search = st.text_input("Search (MS Number / Title contains)", value="")

# Apply filters
filtered = master.copy()
filtered["ms_status"] = filtered["ms_status"].fillna("Unknown")
filtered["type_of_ms"] = filtered["type_of_ms"].fillna("Unknown")

if nsc_sel:
    filtered = filtered[filtered["nsc_code"].isin(nsc_sel)]

if status_sel:
    filtered = filtered[filtered["ms_status"].isin(status_sel)]

if type_sel:
    filtered = filtered[filtered["type_of_ms"].isin(type_sel)]

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
k1, k2, k3, k4, k5 = st.columns(5)

total_all = int(master.shape[0])
total_filtered = int(filtered.shape[0])
unique_ms_all = int(master["ms_number"].nunique())
unique_ms_filtered = int(filtered["ms_number"].nunique())
unique_type_filtered = int(filtered["type_of_ms"].nunique())

k1.metric("Rows (All)", f"{total_all:,}")
k2.metric("Rows (Filtered)", f"{total_filtered:,}")
k3.metric("Unique MS Number (All)", f"{unique_ms_all:,}")
k4.metric("Unique MS Number (Filtered)", f"{unique_ms_filtered:,}")
k5.metric("Unique TYPE OF MS (Filtered)", f"{unique_type_filtered:,}")

# -----------------------------
# Charts
# -----------------------------
st.subheader("Distributions (Filtered)")
cc1, cc2, cc3 = st.columns(3)

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

# -----------------------------
# NSC Directory (optional)
# -----------------------------
with st.expander("NSC Directory (Project Manager / Contacts)", expanded=False):
    if nsc_dir.empty:
        st.info("NSC directory not found in the uploaded file.")
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
    st.info("Edit **MS Status** and click **Save changes**. Changes are stored in the local SQLite database.")
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
