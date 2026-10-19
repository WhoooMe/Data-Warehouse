"""
Dashboard Analitik Rumah Sakit  |  Streamlit + Plotly + MySQL (star schema `hospital_dwh`)

Prasyarat : pip install streamlit plotly pandas sqlalchemy pymysql
Jalankan  : streamlit run dashboard.py

Koneksi (opsional, lewat environment variable):
    MYSQL_HOST (localhost), MYSQL_PORT (3306), MYSQL_USER (root),
    MYSQL_PASSWORD (kosong), MYSQL_DB (hospital_dwh)

Pemetaan nama kolom: query di bawah memakai nama kolom ASLI pada DDL hospital_dwh.
    deskripsi di brief            ->  kolom sebenarnya di DWH
    fact_encounters.encounter_id  ->  encounter_sk (kunci surrogate) / encounter_bk (ID asal)
    fact_encounters.date_id       ->  start_date_sk   (FK ke dim_date.date_sk)
    dim_date.date / month / year  ->  full_date / month_num / year_num
    dim_patient.birthdate         ->  birth_date
    dim_payer.name                ->  payer_name
    dim_procedure.description     ->  procedure_description
    fact_procedures.encounter_id  ->  encounter_sk
    dim_patient.lat / lon         ->  lat / lon  (kolom BARU: jalankan ulang etl_new.py versi terbaru)
"""
import os
from decimal import Decimal

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ============================================================== KONFIGURASI
st.set_page_config(page_title="Hospital Analytics", page_icon="🏥", layout="wide",
                   initial_sidebar_state="expanded")

DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DB", "hospital_dwh")

CURRENCY = "$"  # data sumber berformat Synthea (USD). Ganti jika memakai mata uang lain.
MONTHS = ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]
PALETTE = ["#4F46E5", "#06B6D4", "#F59E0B", "#10B981", "#EF4444", "#8B5CF6", "#EC4899", "#14B8A6"]
GENDER_LABEL = {"M": "Laki-laki", "F": "Perempuan", "U": "Tidak diketahui"}
GENDER_COLOR = {"Laki-laki": "#4F46E5", "Perempuan": "#EC4899", "Tidak diketahui": "#9CA3AF"}
AGE_BINS = [-1, 4, 12, 17, 39, 59, 200]
AGE_LABELS = ["Balita (0-4)", "Anak (5-12)", "Remaja (13-17)",
              "Dewasa Muda (18-39)", "Dewasa (40-59)", "Lansia (60+)"]

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.6rem; padding-bottom: 2rem;}
    [data-testid="stMetricValue"] {font-size: 1.9rem; font-weight: 700;}
    [data-testid="stMetricLabel"] p {font-size: .8rem; opacity: .7; text-transform: uppercase;
                                      letter-spacing: .05em;}
    button[data-baseweb="tab"] p {font-size: 1rem; font-weight: 600;}
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================== KONEKSI & QUERY
@st.cache_resource(show_spinner=False)
def get_engine():
    url = URL.create("mysql+pymysql", username=DB_USER, password=DB_PASS, host=DB_HOST,
                     port=DB_PORT, database=DB_NAME, query={"charset": "utf8mb4"})
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)


@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Jalankan query (parameter di-bind, bukan di-format ke string) dan hasilnya di-cache 10 menit."""
    with get_engine().connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params or {})
    for col in df.columns:  # SUM() di MySQL mengembalikan Decimal -> ubah ke float agar bisa diplot
        nonnull = df[col].dropna()
        if len(nonnull) and isinstance(nonnull.iloc[0], Decimal):
            df[col] = df[col].astype(float)
    return df


def show(fig, height=380):
    """Tampilkan grafik Plotly dengan gaya seragam (kompatibel Streamlit lama maupun baru)."""
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=24, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title_text=""),
        font=dict(size=13),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,.2)")
    try:
        st.plotly_chart(fig, width="stretch")
    except TypeError:  # Streamlit versi lama
        st.plotly_chart(fig, use_container_width=True)


def money(v: float) -> str:
    a = abs(v)
    if a >= 1e9:
        return f"{CURRENCY}{v / 1e9:,.2f}B"
    if a >= 1e6:
        return f"{CURRENCY}{v / 1e6:,.2f}M"
    if a >= 1e3:
        return f"{CURRENCY}{v / 1e3:,.1f}K"
    return f"{CURRENCY}{v:,.0f}"


def shorten(s: str, n: int = 55) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


# ============================================================== SQL (satu per visualisasi)
SQL_BOUNDS = """
SELECT MIN(d.year_num) AS y_min, MAX(d.year_num) AS y_max
FROM   fact_encounters f
JOIN   dim_date d ON d.date_sk = f.start_date_sk
"""

SQL_CLASSES = """
SELECT encounter_class_sk, encounter_class_name
FROM   dim_encounter_class
WHERE  encounter_class_sk <> -1
ORDER  BY encounter_class_name
"""

# --- KPI
SQL_KPI = """
SELECT COUNT(*)                                AS total_encounters,
       COUNT(DISTINCT f.patient_sk)            AS unique_patients,
       COALESCE(SUM(f.total_claim_cost), 0)    AS total_claim,
       COALESCE(SUM(f.payer_coverage), 0)      AS total_coverage,
       COALESCE(SUM(f.out_of_pocket_cost), 0)  AS total_oop
FROM   fact_encounters f
JOIN   dim_date d ON d.date_sk = f.start_date_sk
WHERE  {where}
"""

# --- Tab 1: Financial Performance
SQL_PAYER_FINANCE = """
SELECT p.payer_name,
       COUNT(*)                   AS encounters,
       SUM(f.total_claim_cost)    AS total_claim,
       SUM(f.payer_coverage)      AS total_coverage,
       SUM(f.out_of_pocket_cost)  AS out_of_pocket
FROM   fact_encounters f
JOIN   dim_payer p ON p.payer_sk = f.payer_sk
JOIN   dim_date  d ON d.date_sk  = f.start_date_sk
WHERE  {where}
GROUP  BY p.payer_sk, p.payer_name
ORDER  BY total_claim DESC
"""

SQL_TOP_PAYERS = """
SELECT p.payer_name, COUNT(*) AS encounters
FROM   fact_encounters f
JOIN   dim_payer p ON p.payer_sk = f.payer_sk
JOIN   dim_date  d ON d.date_sk  = f.start_date_sk
WHERE  {where}
GROUP  BY p.payer_sk, p.payer_name
ORDER  BY encounters DESC
LIMIT  5
"""

# --- Tab 2: Clinical Insights
SQL_TREND = """
SELECT d.year_num, d.month_num, COUNT(*) AS encounters
FROM   fact_encounters f
JOIN   dim_date d ON d.date_sk = f.start_date_sk
WHERE  {where}
GROUP  BY d.year_num, d.month_num
ORDER  BY d.year_num, d.month_num
"""

SQL_TOP_PROCEDURES = """
SELECT pr.procedure_description, SUM(fp.procedure_count) AS total_procedures
FROM   fact_procedures fp
JOIN   fact_encounters f  ON f.encounter_sk  = fp.encounter_sk
JOIN   dim_procedure   pr ON pr.procedure_sk = fp.procedure_sk
JOIN   dim_date        d  ON d.date_sk       = f.start_date_sk
WHERE  {where}
GROUP  BY pr.procedure_sk, pr.procedure_description
ORDER  BY total_procedures DESC
LIMIT  10
"""

# --- Tab 3: Patient Demographics (umur dihitung per pasien unik)
SQL_AGE_AT_LAST_VISIT = """
SELECT p.patient_sk, p.gender,
       TIMESTAMPDIFF(YEAR, p.birth_date, MAX(f.start_datetime)) AS age
FROM   fact_encounters f
JOIN   dim_patient p ON p.patient_sk = f.patient_sk
JOIN   dim_date    d ON d.date_sk    = f.start_date_sk
WHERE  {where} AND p.patient_sk <> -1 AND p.birth_date IS NOT NULL
GROUP  BY p.patient_sk, p.gender, p.birth_date
"""

SQL_AGE_TODAY = """
SELECT p.patient_sk, p.gender, TIMESTAMPDIFF(YEAR, p.birth_date, CURDATE()) AS age
FROM   fact_encounters f
JOIN   dim_patient p ON p.patient_sk = f.patient_sk
JOIN   dim_date    d ON d.date_sk    = f.start_date_sk
WHERE  {where} AND p.patient_sk <> -1 AND p.birth_date IS NOT NULL
GROUP  BY p.patient_sk, p.gender, p.birth_date
"""

SQL_HAS_COORDS = "SHOW COLUMNS FROM dim_patient LIKE 'lat'"

SQL_PATIENT_MAP = """
SELECT p.patient_sk, p.lat, p.lon, p.city, COUNT(*) AS encounters
FROM   fact_encounters f
JOIN   dim_patient p ON p.patient_sk = f.patient_sk
JOIN   dim_date    d ON d.date_sk    = f.start_date_sk
WHERE  {where} AND p.patient_sk <> -1 AND p.lat IS NOT NULL AND p.lon IS NOT NULL
GROUP  BY p.patient_sk, p.lat, p.lon, p.city
"""

SQL_TOP_CITIES = """
SELECT p.city, COUNT(DISTINCT p.patient_sk) AS patients
FROM   fact_encounters f
JOIN   dim_patient p ON p.patient_sk = f.patient_sk
JOIN   dim_date    d ON d.date_sk    = f.start_date_sk
WHERE  {where} AND p.patient_sk <> -1
GROUP  BY p.city
ORDER  BY patients DESC
LIMIT  10
"""


def build_where(y1, y2, m1, m2, class_sks, all_class_sks):
    """Susun klausa WHERE + parameter terikat dari filter sidebar."""
    where = ["d.year_num BETWEEN :y1 AND :y2", "d.month_num BETWEEN :m1 AND :m2"]
    params = {"y1": y1, "y2": y2, "m1": m1, "m2": m2}
    if class_sks and set(class_sks) != set(all_class_sks):
        ph = []
        for i, sk in enumerate(class_sks):
            params[f"c{i}"] = int(sk)
            ph.append(f":c{i}")
        where.append(f"f.encounter_class_sk IN ({', '.join(ph)})")
    return " AND ".join(where), params


# ============================================================== CEK KONEKSI
try:
    bounds = run_query(SQL_BOUNDS)
    classes_df = run_query(SQL_CLASSES)
except Exception as exc:  # noqa: BLE001
    st.error("Tidak bisa terhubung / membaca database `%s` di %s:%s." % (DB_NAME, DB_HOST, DB_PORT))
    st.markdown(
        "- Pastikan MySQL (Laragon) sedang berjalan.\n"
        "- Pastikan ETL sudah dijalankan: `python etl_new.py --once`.\n"
        "- Jika root memakai password, set `MYSQL_PASSWORD` sebelum `streamlit run`."
    )
    st.code(str(exc))
    st.stop()

if pd.isna(bounds.loc[0, "y_min"]):
    st.warning("Tabel `fact_encounters` masih kosong. Jalankan ETL terlebih dahulu.")
    st.stop()

Y_MIN, Y_MAX = int(bounds.loc[0, "y_min"]), int(bounds.loc[0, "y_max"])
ALL_CLASS_NAMES = classes_df["encounter_class_name"].tolist()
CLASS_SK = dict(zip(classes_df["encounter_class_name"], classes_df["encounter_class_sk"]))

# ============================================================== SIDEBAR
with st.sidebar:
    st.title("🏥 Hospital Analytics")
    st.caption("Filter berlaku untuk seluruh dashboard")

    if Y_MIN < Y_MAX:
        y1, y2 = st.slider("Rentang tahun", Y_MIN, Y_MAX, (Y_MIN, Y_MAX))
    else:
        y1 = y2 = Y_MIN
        st.info(f"Data hanya tersedia untuk tahun {Y_MIN}")

    m1_name, m2_name = st.select_slider("Rentang bulan (di setiap tahun)", options=MONTHS,
                                        value=(MONTHS[0], MONTHS[-1]))
    m1, m2 = MONTHS.index(m1_name) + 1, MONTHS.index(m2_name) + 1

    chosen_classes = st.multiselect("Kelas kunjungan", ALL_CLASS_NAMES, default=ALL_CLASS_NAMES)

    st.divider()
    if st.button("🔄 Muat ulang data"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Sumber: `{DB_NAME}` @ {DB_HOST}:{DB_PORT}  \nCache query: 10 menit")

if m1 > m2:
    st.warning("Bulan awal harus sebelum bulan akhir.")
    st.stop()
if not chosen_classes:
    st.warning("Pilih minimal satu kelas kunjungan di sidebar.")
    st.stop()

WHERE, PARAMS = build_where(y1, y2, m1, m2, [CLASS_SK[c] for c in chosen_classes],
                            list(CLASS_SK.values()))

# ============================================================== HEADER + KPI
st.title("Dashboard Analitik Rumah Sakit")
st.caption(f"Periode **{y1}–{y2}** · Bulan **{m1_name}–{m2_name}** · "
           f"{len(chosen_classes)} dari {len(ALL_CLASS_NAMES)} kelas kunjungan")

kpi = run_query(SQL_KPI.format(where=WHERE), PARAMS).iloc[0]
if int(kpi["total_encounters"]) == 0:
    st.warning("Tidak ada data pada filter yang dipilih. Longgarkan filter di sidebar.")
    st.stop()

total_enc = int(kpi["total_encounters"])
total_claim, total_cov, total_oop = (float(kpi["total_claim"]), float(kpi["total_coverage"]),
                                     float(kpi["total_oop"]))
cov_pct = total_cov / total_claim * 100 if total_claim else 0

c1, c2, c3, c4 = st.columns(4)
with c1, st.container(border=True):
    st.metric("Total Kunjungan", f"{total_enc:,}")
    st.caption(f"{int(kpi['unique_patients']):,} pasien unik")
with c2, st.container(border=True):
    st.metric("Total Klaim Biaya", money(total_claim))
    st.caption(f"Rata-rata {money(total_claim / total_enc)} per kunjungan")
with c3, st.container(border=True):
    st.metric("Ditanggung Asuransi", money(total_cov))
    st.caption(f"{cov_pct:.1f}% dari total klaim")
with c4, st.container(border=True):
    st.metric("Biaya Pasien (Out-of-Pocket)", money(total_oop))
    st.caption(f"{100 - cov_pct:.1f}% dari total klaim")

st.write("")
tab_fin, tab_clin, tab_demo = st.tabs(["💰 Financial Performance", "🩺 Clinical Insights",
                                       "👥 Patient Demographics"])

# ============================================================== TAB 1: FINANCIAL
with tab_fin:
    payer = run_query(SQL_PAYER_FINANCE.format(where=WHERE), PARAMS)
    top5 = run_query(SQL_TOP_PAYERS.format(where=WHERE), PARAMS)

    left, right = st.columns([3, 2])
    with left, st.container(border=True):
        st.markdown("**Total Klaim vs Ditanggung Asuransi, per Asuransi**")
        long = payer.melt(id_vars="payer_name", value_vars=["total_claim", "total_coverage"],
                          var_name="Jenis", value_name="Nilai")
        long["Jenis"] = long["Jenis"].map({"total_claim": "Total klaim",
                                           "total_coverage": "Ditanggung asuransi"})
        fig = px.bar(long, x="payer_name", y="Nilai", color="Jenis", barmode="group",
                     color_discrete_sequence=[PALETTE[0], PALETTE[3]],
                     labels={"payer_name": "", "Nilai": f"Nilai ({CURRENCY})"})
        fig.update_xaxes(tickangle=-30)
        fig.update_traces(hovertemplate="%{x}<br>%{fullData.name}: " + CURRENCY + "%{y:,.0f}<extra></extra>")
        show(fig, 420)

    with right, st.container(border=True):
        st.markdown("**Top 5 Asuransi berdasarkan Jumlah Kunjungan**")
        fig = px.pie(top5, names="payer_name", values="encounters", hole=0.58,
                     color_discrete_sequence=PALETTE)
        fig.update_traces(textinfo="percent", textposition="outside",
                          hovertemplate="%{label}<br>%{value:,} kunjungan (%{percent})<extra></extra>")
        fig.update_layout(legend=dict(orientation="h", y=-0.1))
        show(fig, 420)
        share = top5["encounters"].sum() / total_enc * 100
        st.caption(f"Lima asuransi teratas mencakup **{share:.1f}%** dari seluruh kunjungan.")

    with st.expander("Lihat tabel rincian per asuransi"):
        tbl = payer.rename(columns={"payer_name": "Asuransi", "encounters": "Kunjungan",
                                    "total_claim": "Total klaim", "total_coverage": "Ditanggung asuransi",
                                    "out_of_pocket": "Out-of-pocket"})
        try:
            st.dataframe(tbl, hide_index=True, width="stretch")
        except TypeError:
            st.dataframe(tbl, hide_index=True, use_container_width=True)

# ============================================================== TAB 2: CLINICAL
with tab_clin:
    trend = run_query(SQL_TREND.format(where=WHERE), PARAMS)
    procs = run_query(SQL_TOP_PROCEDURES.format(where=WHERE), PARAMS)

    with st.container(border=True):
        st.markdown("**Tren Kunjungan Pasien per Bulan**")
        trend["period"] = pd.to_datetime(dict(year=trend["year_num"], month=trend["month_num"], day=1))
        trend["moving_avg"] = trend["encounters"].rolling(3, min_periods=1).mean()
        fig = px.line(trend, x="period", y="encounters", markers=True,
                      color_discrete_sequence=[PALETTE[0]],
                      labels={"period": "", "encounters": "Jumlah kunjungan"})
        fig.add_scatter(x=trend["period"], y=trend["moving_avg"], mode="lines", name="Rata-rata 3 bulan",
                        line=dict(color=PALETTE[2], dash="dash", width=2))
        fig.update_traces(selector=dict(mode="lines+markers"), name="Kunjungan", showlegend=True)
        show(fig, 380)
        st.caption("Bulan terakhir pada data bisa belum lengkap sehingga tampak turun.")

    with st.container(border=True):
        st.markdown("**Top 10 Tindakan Medis Paling Sering Dilakukan**")
        if procs.empty:
            st.info("Tidak ada tindakan medis pada filter ini.")
        else:
            procs["label"] = procs["procedure_description"].map(shorten)
            fig = px.bar(procs.sort_values("total_procedures"), x="total_procedures", y="label",
                         orientation="h", text="total_procedures",
                         color_discrete_sequence=[PALETTE[1]],
                         labels={"total_procedures": "Jumlah tindakan", "label": ""},
                         hover_data={"procedure_description": True, "label": False})
            fig.update_traces(texttemplate="%{text:,}", textposition="outside", cliponaxis=False)
            show(fig, 460)

# ============================================================== TAB 3: DEMOGRAPHICS
with tab_demo:
    left, right = st.columns([1, 1])

    with left, st.container(border=True):
        st.markdown("**Distribusi Kelompok Umur per Gender**")
        basis = st.radio("Umur dihitung pada", ["Kunjungan terakhir dalam periode", "Hari ini"],
                         horizontal=True)
        sql_age = SQL_AGE_AT_LAST_VISIT if basis.startswith("Kunjungan") else SQL_AGE_TODAY
        age = run_query(sql_age.format(where=WHERE), PARAMS)
        age["Kelompok umur"] = pd.cut(age["age"], bins=AGE_BINS, labels=AGE_LABELS)
        age["Gender"] = age["gender"].map(GENDER_LABEL).fillna("Tidak diketahui")
        dist = (age.groupby(["Kelompok umur", "Gender"], observed=False).size()
                .reset_index(name="Jumlah pasien"))
        fig = px.bar(dist, x="Kelompok umur", y="Jumlah pasien", color="Gender", barmode="group",
                     category_orders={"Kelompok umur": AGE_LABELS}, color_discrete_map=GENDER_COLOR,
                     labels={"Kelompok umur": ""})
        show(fig, 420)
        st.caption(f"{len(age):,} pasien unik · umur rata-rata {age['age'].mean():.1f} tahun")

    with right, st.container(border=True):
        st.markdown("**Peta Sebaran Lokasi Pasien**")
        has_coords = not run_query(SQL_HAS_COORDS).empty
        if has_coords:
            pts = run_query(SQL_PATIENT_MAP.format(where=WHERE), PARAMS)
            if pts.empty:
                st.info("Tidak ada koordinat pasien pada filter ini.")
            else:
                try:
                    st.map(pts, latitude="lat", longitude="lon", size=60, height=420)
                except TypeError:  # Streamlit versi lama (tanpa parameter height)
                    st.map(pts, latitude="lat", longitude="lon", size=60)
                st.caption(f"{len(pts):,} pasien dipetakan.")
        else:
            st.warning("Kolom `lat`/`lon` belum ada di `dim_patient`. Jalankan ulang ETL versi terbaru "
                       "(`python etl_new.py --once`) agar peta tersedia. Sementara ditampilkan per kota.")

    with st.container(border=True):
        st.markdown("**10 Kota Asal Pasien Terbanyak**")
        cities = run_query(SQL_TOP_CITIES.format(where=WHERE), PARAMS)
        fig = px.bar(cities.sort_values("patients"), x="patients", y="city", orientation="h",
                     text="patients", color_discrete_sequence=[PALETTE[3]],
                     labels={"patients": "Jumlah pasien unik", "city": ""})
        fig.update_traces(textposition="outside", cliponaxis=False)
        show(fig, 380)
