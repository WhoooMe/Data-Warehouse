"""
ETL Hospital Patient Records -> Data Warehouse (MySQL, star schema)

Prasyarat : pip install pandas sqlalchemy pymysql schedule
Jalankan  :
    python etl_new.py                        # mode terjadwal: standby, ETL jalan tiap hari 01:00
    python etl_new.py oltp_dirty             # folder sumber eksplisit (mode terjadwal)
    python etl_new.py --run-now              # jalankan ETL sekali sekarang, lalu tetap standby
    python etl_new.py --once                 # jalankan ETL sekali lalu keluar (tanpa scheduler)
    python etl_new.py --once --dry-run       # TANPA MySQL: hasil disimpan ke dwh_preview/*.csv
    python etl_new.py --at 02:30             # ganti jam jadwal harian

Koneksi MySQL (ubah di bawah atau pakai environment variable):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
Jadwal (opsional): ETL_RUN_TIME (format HH:MM, default 01:00)

CATATAN: jadwal memakai jam LOKAL server. Pastikan timezone server sudah sesuai
(mis. Asia/Makassar untuk WITA / Bali).

Tiga teknik cleansing wajib + bonus (semua dicatat di etl_cleansing_summary & reject_log):
    1. NULL            -> isi 'Unknown' / Unknown member (SK = -1) / tolak bila kolom wajib kosong
    2. DUPLIKASI       -> baris identik, ID ganda, pasien yang sama ber-ID beda
    3. BAD BUSINESS LOGIC -> tolak ke reject_log (STOP<START, biaya negatif, kunjungan setelah wafat,
                             orphan) atau perbaiki (coverage > total klaim)
    +  STANDARDISASI   -> hapus angka pada nama, Title Case, BOM, trim spasi, parsing tanggal ISO8601
"""
import argparse
import json
import logging
import os
import time
import traceback
from pathlib import Path

import pandas as pd
import schedule

log = logging.getLogger("etl_scheduler")

# ============================== KONFIGURASI ==============================
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")      # <- isi password MySQL-mu
DB_NAME = "hospital_dwh"

REJECT_AFTER_DEATH = True      # True: kunjungan setelah wafat ditolak; False: tetap dimuat
READMISSION_DAYS = 30
ETL_RUN_TIME = os.getenv("ETL_RUN_TIME", "01:00")     # jam eksekusi harian (waktu lokal server)
DDL_FILE = Path(__file__).with_name("ddl_hospital_dwh.sql")
# ==========================================================================

summary, rejects = [], []
masuk, dup = {}, {}      # untuk rekonsiliasi: baris masuk & duplikat dibuang per tabel
SRC = Path("oltp_dirty")


# ------------------------------------------------------------------ helper umum
def note(teknik, rule, tabel, n, aksi):
    if int(n) > 0:
        summary.append(dict(technique=teknik, rule_name=rule, source_table=tabel,
                            rows_affected=int(n), action=aksi))


def catat_dup(tabel, n):
    dup[tabel] = dup.get(tabel, 0) + int(n)


def read_csv(nama):
    """EXTRACT: baca CSV (utf-8-sig menangani BOM), trim spasi, string kosong -> NULL."""
    df = pd.read_csv(SRC / f"{nama}.csv", encoding="utf-8-sig", dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].map(lambda v: (v.strip() or None) if isinstance(v, str) else v)
    masuk[nama] = len(df)
    print(f"[extract] {nama}.csv: {len(df):,} baris")
    return df


def to_dt(s):
    """Parse tanggal ISO8601 (UTC) menjadi datetime tanpa timezone."""
    try:
        out = pd.to_datetime(s, errors="coerce", utc=True, format="mixed")
    except (TypeError, ValueError):
        out = pd.to_datetime(s, errors="coerce", utc=True)
    return out.dt.tz_localize(None)


def tolak(df, mask, tabel, teknik, rule, alasan):
    """Pindahkan baris yang melanggar aturan ke reject_log, kembalikan sisanya."""
    mask = mask.fillna(False).astype(bool)
    bad = df[mask]
    if len(bad):
        raw = []
        for r in bad.drop(columns=["_KEY"]).to_dict("records"):
            raw.append(json.dumps({k: (None if pd.isna(v) else str(v)) for k, v in r.items()},
                                  ensure_ascii=False))
        rejects.append(pd.DataFrame({
            "source_table": tabel, "source_key": bad["_KEY"].astype(str).to_numpy(),
            "rule_name": rule, "reason": alasan, "raw_data": raw}))
        note(teknik, rule, tabel, len(bad), "ditolak ke reject_log")
    return df[~mask].copy()


def sebagai_date(s):
    return pd.Series([None if pd.isna(x) else x.date() for x in s], index=s.index, dtype=object)


# ------------------------------------------------------------------ TRANSFORM: dimensi
def transform_patients():
    df = read_csv("patients")
    df["_KEY"] = df["ID"]

    # DUPLIKASI 1: ID pasien ganda
    m = df.duplicated("ID", keep="first")
    note("Duplikasi", "ID pasien ganda", "patients", m.sum(), "baris duplikat dibuang")
    catat_dup("patients", m.sum())
    df = df[~m].copy()

    # DUPLIKASI 2: orang yang sama terdaftar dengan ID berbeda (FIRST+LAST+BIRTHDATE)
    kunci = df["FIRST"].fillna("") + "|" + df["LAST"].fillna("") + "|" + df["BIRTHDATE"].fillna("")
    id_pertama = df.groupby(kunci)["ID"].transform("first")
    dup_orang = df["ID"] != id_pertama
    remap = dict(zip(df.loc[dup_orang, "ID"], id_pertama[dup_orang]))
    note("Duplikasi", "Pasien sama (nama+tgl lahir) ber-ID berbeda", "patients", dup_orang.sum(),
         "dibuang; ID lama dipetakan ke ID yang dipertahankan")
    catat_dup("patients", dup_orang.sum())
    df = df[~dup_orang].copy()

    # NULL: isi 'Unknown'
    for col in ["ZIP", "MARITAL", "CITY", "STATE", "RACE", "ETHNICITY", "GENDER"]:
        note("Null", f"patients.{col} kosong", "patients", df[col].isna().sum(), "diisi 'Unknown'")
        df[col] = df[col].fillna("Unknown")

    # STANDARDISASI: nama bersih dari angka, Title Case
    ada_angka = df["FIRST"].str.contains(r"\d", na=False) | df["LAST"].str.contains(r"\d", na=False)
    note("Standardisasi", "Nama mengandung angka", "patients", ada_angka.sum(),
         "angka dihapus (regex), Title Case")
    for c in ["FIRST", "LAST"]:
        df[c] = df[c].fillna("").str.replace(r"\d+", "", regex=True).str.strip().str.title()
    df["FULL_NAME"] = (df["FIRST"] + " " + df["LAST"]).str.strip().replace("", "Unknown")

    g = df["GENDER"].str.upper().str[:1]
    salah = ~g.isin(["M", "F"])
    note("Standardisasi", "GENDER di luar M/F", "patients", salah.sum(), "diisi 'U'")
    df["GENDER"] = g.where(~salah, "U")

    df["BIRTH"] = to_dt(df["BIRTHDATE"])
    df["DEATH"] = to_dt(df["DEATHDATE"])

    # KOORDINAT (untuk peta dashboard): angka valid, lat -90..90, lon -180..180, selain itu NULL
    for c in ["LAT", "LON"]:
        df[c] = pd.to_numeric(df[c], errors="coerce") if c in df.columns else float("nan")
    luar = ~df["LAT"].between(-90, 90) | ~df["LON"].between(-180, 180) | df["LAT"].isna() | df["LON"].isna()
    note("Null", "LAT/LON kosong atau di luar rentang", "patients", luar.sum(), "koordinat diisi NULL (pasien tetap dimuat)")
    df.loc[luar, ["LAT", "LON"]] = float("nan")
    df["LAT"] = df["LAT"].round(6)
    df["LON"] = df["LON"].round(6)

    # BAD BUSINESS LOGIC pada pasien
    df = tolak(df, df["BIRTH"].isna(), "patients", "Null", "BIRTHDATE kosong/tidak valid",
               "kolom wajib")
    df = tolak(df, df["BIRTH"] > pd.Timestamp.now(), "patients", "Bad business logic",
               "BIRTHDATE di masa depan", "tanggal lahir tidak logis")
    df = tolak(df, df["DEATH"] < df["BIRTH"], "patients", "Bad business logic",
               "DEATHDATE lebih awal dari BIRTHDATE", "tanggal wafat tidak logis")

    df = df.sort_values("ID").reset_index(drop=True)
    dim = pd.DataFrame({
        "patient_sk": range(1, len(df) + 1), "patient_bk": df["ID"],
        "first_name": df["FIRST"], "last_name": df["LAST"], "full_name": df["FULL_NAME"],
        "gender": df["GENDER"], "birth_date": df["BIRTH"], "death_date": df["DEATH"],
        "is_alive": df["DEATH"].isna().astype(int), "race": df["RACE"].str.title(),
        "ethnicity": df["ETHNICITY"].str.title(), "marital_status": df["MARITAL"],
        "city": df["CITY"], "state": df["STATE"], "zip": df["ZIP"],
        "lat": df["LAT"], "lon": df["LON"]})
    unknown = pd.DataFrame([dict(patient_sk=-1, patient_bk="UNKNOWN", first_name="Unknown",
                                 last_name="Unknown", full_name="Unknown", gender="U",
                                 birth_date=pd.NaT, death_date=pd.NaT, is_alive=None, race="Unknown",
                                 ethnicity="Unknown", marital_status="Unknown", city="Unknown",
                                 state="Unknown", zip="Unknown", lat=None, lon=None)])
    out = pd.concat([unknown, dim], ignore_index=True)
    out["is_alive"] = out["is_alive"].astype("Int64")  # 1 hidup, 0 wafat, NULL = tidak diketahui (Unknown member)
    return out, remap


def kategori_payer(nama):
    n = str(nama).lower()
    if "no_insurance" in n or "no insurance" in n:
        return "Self-Pay"
    if any(k in n for k in ["medicare", "medicaid", "dual eligible"]):
        return "Public"
    return "Private"


def transform_payers():
    df = read_csv("payers")
    df["_KEY"] = df["ID"]
    m = df.duplicated("ID", keep="first")
    note("Duplikasi", "ID payer ganda", "payers", m.sum(), "baris duplikat dibuang")
    catat_dup("payers", m.sum())
    df = df[~m].copy()
    for col in ["ADDRESS", "CITY", "STATE_HEADQUARTERED", "ZIP", "PHONE"]:
        note("Null", f"payers.{col} kosong", "payers", df[col].isna().sum(), "diisi 'Unknown'")
        df[col] = df[col].fillna("Unknown")
    df = df.sort_values("ID").reset_index(drop=True)
    dim = pd.DataFrame({
        "payer_sk": range(1, len(df) + 1), "payer_bk": df["ID"], "payer_name": df["NAME"],
        "city": df["CITY"], "state": df["STATE_HEADQUARTERED"], "zip": df["ZIP"],
        "phone": df["PHONE"], "payer_category": df["NAME"].map(kategori_payer)})
    unknown = pd.DataFrame([dict(payer_sk=-1, payer_bk="UNKNOWN", payer_name="Unknown",
                                 city="Unknown", state="Unknown", zip="Unknown", phone="Unknown",
                                 payer_category="Unknown")])
    return pd.concat([unknown, dim], ignore_index=True)


# ------------------------------------------------------------------ TRANSFORM: fakta kunjungan
def transform_encounters(dim_pat, dim_pay, remap):
    df = read_csv("encounters")
    df["_KEY"] = df["ID"]

    # DUPLIKASI
    m = df.drop(columns=["_KEY"]).duplicated(keep="first")
    note("Duplikasi", "Baris kunjungan identik penuh", "encounters", m.sum(), "baris duplikat dibuang")
    catat_dup("encounters", m.sum())
    df = df[~m].copy()
    m = df.duplicated("ID", keep="first")
    note("Duplikasi", "ID kunjungan ganda (isi berbeda)", "encounters", m.sum(), "dibuang, ambil yang pertama")
    catat_dup("encounters", m.sum())
    df = df[~m].copy()

    if remap:
        n = df["PATIENT"].isin(list(remap)).sum()
        note("Duplikasi", "Kunjungan merujuk pasien duplikat", "encounters", n,
             "PATIENT dipetakan ke ID pasien yang dipertahankan")
        df["PATIENT"] = df["PATIENT"].replace(remap)

    # NULL pada kolom wajib -> tolak
    df = tolak(df, df["ID"].isna() | df["PATIENT"].isna() | df["START"].isna() | df["STOP"].isna(),
               "encounters", "Null", "Kolom wajib kosong (ID/PATIENT/START/STOP)", "tidak bisa dimuat")
    df["START_DT"], df["STOP_DT"] = to_dt(df["START"]), to_dt(df["STOP"])
    df = tolak(df, df["START_DT"].isna() | df["STOP_DT"].isna(), "encounters", "Standardisasi",
               "Tanggal gagal diparse", "format tanggal tidak valid")
    for c in ["BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE"]:
        df[c + "_N"] = pd.to_numeric(df[c], errors="coerce")
    df = tolak(df, df[["BASE_ENCOUNTER_COST_N", "TOTAL_CLAIM_COST_N", "PAYER_COVERAGE_N"]].isna().any(axis=1),
               "encounters", "Null", "Biaya kosong / non-numerik", "measure tidak bisa dihitung")

    # BAD BUSINESS LOGIC
    df = tolak(df, df["STOP_DT"] < df["START_DT"], "encounters", "Bad business logic",
               "STOP lebih awal dari START", "durasi negatif")
    neg = (df["BASE_ENCOUNTER_COST_N"] < 0) | (df["TOTAL_CLAIM_COST_N"] < 0) | (df["PAYER_COVERAGE_N"] < 0)
    df = tolak(df, neg, "encounters", "Bad business logic", "Biaya negatif", "nilai biaya tidak valid")

    pat_ids = set(dim_pat["patient_bk"])
    df = tolak(df, ~df["PATIENT"].isin(pat_ids), "encounters", "Bad business logic",
               "PATIENT tidak ada di dimensi pasien (orphan)", "referensi rusak")
    lahir = df["PATIENT"].map(dim_pat.set_index("patient_bk")["birth_date"])
    wafat = df["PATIENT"].map(dim_pat.set_index("patient_bk")["death_date"])
    mulai = df["START_DT"].dt.normalize()
    df = tolak(df, mulai < lahir, "encounters", "Bad business logic",
               "Kunjungan sebelum tanggal lahir pasien", "tanggal tidak logis")
    lahir = df["PATIENT"].map(dim_pat.set_index("patient_bk")["birth_date"])
    wafat = df["PATIENT"].map(dim_pat.set_index("patient_bk")["death_date"])
    mulai = df["START_DT"].dt.normalize()
    sesudah_wafat = mulai > wafat
    if REJECT_AFTER_DEATH:
        df = tolak(df, sesudah_wafat, "encounters", "Bad business logic",
                   "Kunjungan setelah tanggal wafat pasien", "pasien sudah meninggal")
    else:
        note("Bad business logic", "Kunjungan setelah tanggal wafat pasien", "encounters",
             sesudah_wafat.sum(), "dipertahankan (REJECT_AFTER_DEATH=False)")

    # PERBAIKAN: coverage tidak boleh melebihi total klaim
    lebih = df["PAYER_COVERAGE_N"] > df["TOTAL_CLAIM_COST_N"]
    note("Bad business logic", "PAYER_COVERAGE > TOTAL_CLAIM_COST", "encounters", lebih.sum(),
         "diperbaiki: coverage dibatasi = total klaim (out-of-pocket = 0)")
    df.loc[lebih, "PAYER_COVERAGE_N"] = df.loc[lebih, "TOTAL_CLAIM_COST_N"]

    # NULL pada PAYER -> Unknown member
    note("Null", "encounters.PAYER kosong", "encounters", df["PAYER"].isna().sum(),
         "dipetakan ke Unknown member (payer_sk = -1)")
    payer_map = dict(zip(dim_pay["payer_bk"], dim_pay["payer_sk"]))
    tak_ada = df["PAYER"].notna() & ~df["PAYER"].isin(payer_map)
    note("Bad business logic", "PAYER tidak ada di dimensi payer", "encounters", tak_ada.sum(),
         "dipetakan ke Unknown member (payer_sk = -1)")
    df["PAYER_SK"] = df["PAYER"].map(payer_map).fillna(-1).astype(int)

    # Dimensi kelas kunjungan
    df["CLASS"] = df["ENCOUNTERCLASS"].fillna("unknown").str.lower()
    note("Null", "encounters.ENCOUNTERCLASS kosong", "encounters", df["ENCOUNTERCLASS"].isna().sum(),
         "diisi 'unknown'")
    kelas = sorted(df["CLASS"].unique())
    dim_cls = pd.concat([
        pd.DataFrame([dict(encounter_class_sk=-1, encounter_class_name="unknown")]),
        pd.DataFrame({"encounter_class_sk": range(1, len(kelas) + 1), "encounter_class_name": kelas})
    ], ignore_index=True)
    dim_cls = dim_cls.drop_duplicates("encounter_class_name")
    cls_map = dict(zip(dim_cls["encounter_class_name"], dim_cls["encounter_class_sk"]))

    # Measure turunan
    df["OOP"] = (df["TOTAL_CLAIM_COST_N"] - df["PAYER_COVERAGE_N"]).round(2)
    df["DUR"] = ((df["STOP_DT"] - df["START_DT"]).dt.total_seconds() / 60).round().astype("Int64")

    # Readmission 30 hari (hanya inpatient)
    df["READMIT"] = 0
    inp = df[df["CLASS"] == "inpatient"].sort_values(["PATIENT", "START_DT"])
    stop_sebelumnya = inp.groupby("PATIENT")["STOP_DT"].shift(1)
    # selisih waktu PERSIS (bukan .dt.days yang membuang jam): readmisi = 0 s.d. 30 x 24 jam
    selisih = inp["START_DT"] - stop_sebelumnya
    flag = (selisih >= pd.Timedelta(0)) & (selisih <= pd.Timedelta(days=READMISSION_DAYS))
    df.loc[inp.index[flag.to_numpy()], "READMIT"] = 1

    df = df.sort_values(["START_DT", "ID"]).reset_index(drop=True)
    pat_sk = dict(zip(dim_pat["patient_bk"], dim_pat["patient_sk"]))
    fact = pd.DataFrame({
        "encounter_sk": range(1, len(df) + 1), "encounter_bk": df["ID"],
        "start_date_sk": df["START_DT"].dt.strftime("%Y%m%d").astype(int),
        "patient_sk": df["PATIENT"].map(pat_sk).astype(int), "payer_sk": df["PAYER_SK"],
        "encounter_class_sk": df["CLASS"].map(cls_map).astype(int),
        "start_datetime": df["START_DT"], "stop_datetime": df["STOP_DT"],
        "base_encounter_cost": df["BASE_ENCOUNTER_COST_N"].round(2),
        "total_claim_cost": df["TOTAL_CLAIM_COST_N"].round(2),
        "payer_coverage": df["PAYER_COVERAGE_N"].round(2), "out_of_pocket_cost": df["OOP"],
        "encounter_duration_minutes": df["DUR"], "is_readmission_30d": df["READMIT"].astype(int)})
    return fact, dim_cls


# ------------------------------------------------------------------ TRANSFORM: fakta tindakan
def transform_procedures(dim_pat, remap, fact_enc):
    df = read_csv("procedures")
    df["_KEY"] = df["ENCOUNTER"].fillna("?") + "|" + df["CODE"].fillna("?") + "|" + df["START"].fillna("?")

    # DUPLIKASI
    m = df.drop(columns=["_KEY"]).duplicated(keep="first")
    note("Duplikasi", "Baris tindakan identik penuh", "procedures", m.sum(), "baris duplikat dibuang")
    catat_dup("procedures", m.sum())
    df = df[~m].copy()
    if remap:
        df["PATIENT"] = df["PATIENT"].replace(remap)

    # NULL wajib
    df = tolak(df, df["ENCOUNTER"].isna() | df["PATIENT"].isna() | df["CODE"].isna() | df["START"].isna(),
               "procedures", "Null", "Kolom wajib kosong (ENCOUNTER/PATIENT/CODE/START)", "tidak bisa dimuat")
    df["START_DT"], df["STOP_DT"] = to_dt(df["START"]), to_dt(df["STOP"])
    df = tolak(df, df["START_DT"].isna(), "procedures", "Standardisasi", "START gagal diparse",
               "format tanggal tidak valid")
    df["BASE_N"] = pd.to_numeric(df["BASE_COST"], errors="coerce")
    df = tolak(df, df["BASE_N"].isna(), "procedures", "Null", "BASE_COST kosong / non-numerik",
               "measure tidak bisa dihitung")

    # BAD BUSINESS LOGIC
    df = tolak(df, df["STOP_DT"] < df["START_DT"], "procedures", "Bad business logic",
               "STOP lebih awal dari START", "durasi negatif")
    df = tolak(df, df["BASE_N"] < 0, "procedures", "Bad business logic", "BASE_COST negatif",
               "biaya tidak valid")
    enc_sk = dict(zip(fact_enc["encounter_bk"], fact_enc["encounter_sk"]))
    df = tolak(df, ~df["ENCOUNTER"].isin(enc_sk), "procedures", "Bad business logic",
               "Kunjungan induk tidak ada / sudah ditolak", "orphan terhadap fact_encounters")
    pat_sk = dict(zip(dim_pat["patient_bk"], dim_pat["patient_sk"]))
    df = tolak(df, ~df["PATIENT"].isin(pat_sk), "procedures", "Bad business logic",
               "PATIENT tidak ada di dimensi pasien (orphan)", "referensi rusak")

    # Dimensi tindakan (kode -> deskripsi)
    banyak = (df.groupby("CODE")["DESCRIPTION"].nunique() > 1).sum()
    note("Standardisasi", "Satu CODE punya >1 deskripsi", "procedures", banyak,
         "dipakai deskripsi pertama pada dim_procedure")
    desk = df.groupby("CODE")["DESCRIPTION"].agg(lambda s: s.dropna().iloc[0] if s.notna().any() else "Unknown")
    kode = sorted(desk.index)
    dim_proc = pd.DataFrame({"procedure_sk": range(1, len(kode) + 1), "procedure_code": kode,
                             "procedure_description": [desk[k] for k in kode]})
    proc_sk = dict(zip(dim_proc["procedure_code"], dim_proc["procedure_sk"]))

    enc_payer = dict(zip(fact_enc["encounter_bk"], fact_enc["payer_sk"]))
    df = df.sort_values(["START_DT", "ENCOUNTER", "CODE"]).reset_index(drop=True)
    fact = pd.DataFrame({
        "procedure_fact_sk": range(1, len(df) + 1),
        "encounter_sk": df["ENCOUNTER"].map(enc_sk).astype(int),
        "procedure_date_sk": df["START_DT"].dt.strftime("%Y%m%d").astype(int),
        "patient_sk": df["PATIENT"].map(pat_sk).astype(int),
        "payer_sk": df["ENCOUNTER"].map(enc_payer).astype(int),
        "procedure_sk": df["CODE"].map(proc_sk).astype(int),
        "start_datetime": df["START_DT"], "stop_datetime": df["STOP_DT"],
        "base_cost": df["BASE_N"].round(2),
        "procedure_duration_minutes": ((df["STOP_DT"] - df["START_DT"]).dt.total_seconds() / 60)
        .round().astype("Int64"),
        "procedure_count": 1})
    return fact, dim_proc


def rekonsiliasi(rej, dim_pat, dim_pay, fact_enc, fact_pro):
    """Bukti tidak ada baris hilang diam-diam: masuk = dimuat + ditolak + duplikat dibuang."""
    dimuat = {"patients": len(dim_pat) - 1, "payers": len(dim_pay) - 1,   # -1 = Unknown member
              "encounters": len(fact_enc), "procedures": len(fact_pro)}
    ditolak = rej.groupby("source_table").size().to_dict() if len(rej) else {}
    baris, gagal = [], []
    for t in ["patients", "payers", "encounters", "procedures"]:
        m, d, r, l = masuk[t], dup.get(t, 0), int(ditolak.get(t, 0)), dimuat[t]
        selisih = m - (d + r + l)
        baris.append(dict(tabel=t, baris_sumber=m, dimuat=l, ditolak=r, duplikat_dibuang=d,
                          selisih=selisih, status="SESUAI" if selisih == 0 else "TIDAK SESUAI"))
        summary.append(dict(technique="Rekonsiliasi", source_table=t, rows_affected=m,
                            rule_name=f"sumber {m} = dimuat {l} + ditolak {r} + duplikat {d}",
                            action="SESUAI" if selisih == 0 else f"SELISIH {selisih}"))
        if selisih != 0:
            gagal.append(t)
    print("\n" + "=" * 90 + "\nREKONSILIASI BARIS (sumber = dimuat + ditolak + duplikat dibuang)\n" + "=" * 90)
    print(pd.DataFrame(baris).to_string(index=False))
    if gagal:
        raise RuntimeError(f"Rekonsiliasi GAGAL untuk {gagal}: ada baris hilang tanpa tercatat. Load dibatalkan.")


def build_dim_date(y0, y1):
    d = pd.date_range(f"{y0}-01-01", f"{y1}-12-31", freq="D")
    return pd.DataFrame({
        "date_sk": [int(x.strftime("%Y%m%d")) for x in d], "full_date": [x.date() for x in d],
        "year_num": d.year, "quarter_num": d.quarter, "quarter_name": [f"Q{q}" for q in d.quarter],
        "month_num": d.month, "month_name": [x.strftime("%B") for x in d], "day_of_month": d.day,
        "day_of_week": d.dayofweek + 1, "day_name": [x.strftime("%A") for x in d],
        "is_weekend": (d.dayofweek >= 5).astype(int)})


# ------------------------------------------------------------------ LOAD
def ddl_statements():
    hasil = []
    for bagian in DDL_FILE.read_text(encoding="utf-8").split(";"):
        baris = [b for b in bagian.splitlines() if not b.strip().startswith("--")]
        stmt = "\n".join(baris).strip()
        if stmt:
            hasil.append(stmt)
    return hasil


def load_mysql(tables):
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL

    def url(db=None):
        return URL.create("mysql+pymysql", username=MYSQL_USER, password=MYSQL_PASSWORD,
                          host=MYSQL_HOST, port=MYSQL_PORT, database=db, query={"charset": "utf8mb4"})

    print("\n[load] membuat ulang skema (DROP + CREATE) ...")
    with create_engine(url()).begin() as conn:
        for stmt in ddl_statements():
            conn.execute(text(stmt))
    eng = create_engine(url(DB_NAME))
    with eng.begin() as conn:
        for nama, df in tables.items():
            df.to_sql(nama, conn, if_exists="append", index=False, chunksize=2000)
            print(f"[load] {nama:24s} {len(df):>8,} baris")
    print("\n[verifikasi] jumlah baris di MySQL:")
    with eng.connect() as conn:
        for nama in tables:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {nama}")).scalar()
            print(f"  {nama:24s} {n:>8,}")


def simpan_preview(tables):
    out = Path("dwh_preview")
    out.mkdir(exist_ok=True)
    for nama, df in tables.items():
        df.to_csv(out / f"{nama}.csv", index=False, encoding="utf-8-sig")
        print(f"[dry-run] {nama:24s} {len(df):>8,} baris -> {out / (nama + '.csv')}")


# ------------------------------------------------------------------ PIPELINE UTAMA
def reset_state():
    """Kosongkan state global agar setiap run mulai bersih.
    Tanpa ini, summary/rejects/masuk/dup menumpuk antar-run dan rekonsiliasi jadi salah."""
    summary.clear()
    rejects.clear()
    masuk.clear()
    dup.clear()


def run_etl(src="oltp_dirty", dry_run=False):
    """Satu siklus ETL penuh: extract -> transform/cleansing -> rekonsiliasi -> load."""
    global SRC
    SRC = Path(src)
    reset_state()
    mulai = time.time()

    # ---- EXTRACT + TRANSFORM
    dim_pat, remap = transform_patients()
    dim_pay = transform_payers()
    fact_enc, dim_cls = transform_encounters(dim_pat, dim_pay, remap)
    fact_pro, dim_proc = transform_procedures(dim_pat, remap, fact_enc)

    tahun = pd.concat([fact_enc["start_datetime"], fact_pro["start_datetime"]])
    dim_date = build_dim_date(tahun.min().year, tahun.max().year)

    dim_pat = dim_pat.copy()
    dim_pat["birth_date"] = sebagai_date(dim_pat["birth_date"])
    dim_pat["death_date"] = sebagai_date(dim_pat["death_date"])

    rej = (pd.concat(rejects, ignore_index=True) if rejects else
           pd.DataFrame(columns=["source_table", "source_key", "rule_name", "reason", "raw_data"]))
    rekonsiliasi(rej, dim_pat, dim_pay, fact_enc, fact_pro)
    ringkas = pd.DataFrame(summary, columns=["technique", "rule_name", "source_table", "rows_affected", "action"])

    tables = {  # urutan load: dimensi -> fakta -> log
        "dim_date": dim_date, "dim_patient": dim_pat, "dim_payer": dim_pay,
        "dim_procedure": dim_proc, "dim_encounter_class": dim_cls,
        "fact_encounters": fact_enc, "fact_procedures": fact_pro,
        "reject_log": rej, "etl_cleansing_summary": ringkas}

    print("\n" + "=" * 90 + "\nRINGKASAN CLEANSING\n" + "=" * 90)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 80)
    print(ringkas.to_string(index=False) if len(ringkas) else "(tidak ada temuan)")
    print(f"\nTotal baris ditolak (reject_log): {len(rej):,}")

    # ---- LOAD
    if dry_run:
        simpan_preview(tables)
    else:
        load_mysql(tables)
    print(f"\nETL selesai dalam {time.time() - mulai:,.1f} detik.")


def job(src, dry_run):
    """Pembungkus untuk scheduler: error pada satu run TIDAK boleh mematikan proses standby."""
    log.info("=== ETL dimulai ===")
    try:
        run_etl(src, dry_run)
        log.info("=== ETL sukses ===")
    except Exception:
        log.error("=== ETL GAGAL (akan dicoba lagi pada jadwal berikutnya) ===\n%s",
                  traceback.format_exc())


# ------------------------------------------------------------------ MAIN
def main():
    ap = argparse.ArgumentParser(description="ETL Hospital -> Data Warehouse (terjadwal harian)")
    ap.add_argument("src", nargs="?", default="oltp_dirty", help="folder CSV sumber")
    ap.add_argument("--dry-run", action="store_true", help="tanpa MySQL, simpan hasil ke dwh_preview/")
    ap.add_argument("--at", default=ETL_RUN_TIME, help="jam eksekusi harian HH:MM (default 01:00)")
    ap.add_argument("--run-now", action="store_true", help="jalankan sekali sekarang, lalu tetap standby")
    ap.add_argument("--once", action="store_true", help="jalankan sekali lalu keluar (tanpa scheduler)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler("etl_scheduler.log", encoding="utf-8")])

    if args.once:
        run_etl(args.src, args.dry_run)
        return

    # Jadwal harian
    schedule.every().day.at(args.at).do(job, args.src, args.dry_run)
    log.info("Scheduler aktif. ETL dijadwalkan tiap hari pukul %s. Eksekusi berikutnya: %s",
             args.at, schedule.next_run())

    if args.run_now:
        job(args.src, args.dry_run)

    # Standby: cek jadwal tiap detik
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Scheduler dihentikan oleh pengguna.")
