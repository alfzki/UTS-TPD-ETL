"""
extract.py — Fungsi Extract: membaca data dari 4 sumber
=========================================================================
- ZeniBelajar  : MySQL via JDBC
- RuangCerdas  : PostgreSQL via JDBC
- KelasJuara   : MongoDB (seeded from CSV source files)
- PintarNusa   : CSV files via Spark CSV reader

Data yang di-extract disimpan sebagai Parquet untuk intermediate storage,
agar setiap step Airflow bisa berjalan independen.
"""
import os
from pyspark.sql import SparkSession
import config as cfg


# ──────────────────────────────────────────────
# EXTRACT FUNCTIONS
# ──────────────────────────────────────────────

def extract_zenibelajar(spark: SparkSession) -> dict:
    """Extract semua tabel ZeniBelajar dari MySQL via JDBC."""
    jdbc = cfg.ZENIBELAJAR_JDBC
    props = {"user": jdbc["user"], "password": jdbc["password"],
             "driver": jdbc["driver"]}
    table_names = [
        "member", "program_belajar", "peserta_program",
        "riwayat_belajar", "nilai_tryout",
        "order_langganan", "feedback_program",
    ]
    tables = {}
    for name in table_names:
        tables[name] = spark.read.jdbc(url=jdbc["url"], table=name,
                                       properties=props)
        print(f"  [ZeniBelajar] {name}: {tables[name].count()} rows")
    return tables


def extract_ruangcerdas(spark: SparkSession) -> dict:
    """Extract semua tabel RuangCerdas dari PostgreSQL via JDBC."""
    jdbc = cfg.RUANGCERDAS_JDBC
    props = {"user": jdbc["user"], "password": jdbc["password"],
             "driver": jdbc["driver"]}
    table_names = [
        "siswa", "kelas", "pendaftaran_kelas", "log_video",
        "hasil_kuis", "transaksi_paket", "ulasan_kelas",
    ]
    tables = {}
    for name in table_names:
        tables[name] = spark.read.jdbc(url=jdbc["url"], table=name,
                                       properties=props)
        print(f"  [RuangCerdas] {name}: {tables[name].count()} rows")
    return tables


def extract_kelasjuara(spark: SparkSession) -> dict:
    """Extract semua tabel KelasJuara dari MongoDB.

    Source data for MongoDB is seeded from `sumber_data_new/kelasjuara/*.csv`.
    """
    table_names = [
        "pengguna", "produk_kelas", "pembelian_kelas",
        "akses_materi", "skor_evaluasi", "invoice_pembayaran",
        "review_kelas",
    ]
    tables = {}
    for name in table_names:
            tables[name] = (
                spark.read.format("mongodb")
                .option("uri", cfg.KELASJUARA_MONGO["uri"])
                .option("database", cfg.KELASJUARA_MONGO["database"])
                .option("collection", name)
                .load()
            )
            print(f"  [KelasJuara/MongoDB] {name}: {tables[name].count()} rows")
    return tables


def extract_pintarnusa(spark: SparkSession) -> dict:
    """Extract semua CSV PintarNusa dari folder db_pintarnusa_csv.

    CSV files:
      siswa_pintarnusa, katalog_program, enrolmen_program,
      sesi_belajar, hasil_assessment, tagihan_program, ulasan_program
    """
    csv_dir = cfg.PINTARNUSA_CSV_DIR
    csv_files = {
        "siswa_pintarnusa": "siswa_pintarnusa",
        "katalog_program": "katalog_program",
        "enrolmen_program": "enrolemen",
        "sesi_belajar": "sesi_belajar",
        "hasil_assessment": "hasil_assessment",
        "tagihan_program": "tagihan_program",
        "ulasan_program": "ulasan_program",
    }
    tables = {}
    for table_name, file_stem in csv_files.items():
        path = os.path.join(csv_dir, f"{file_stem}.csv").replace("\\", "/")
        tables[table_name] = spark.read.csv(path, header=True, inferSchema=True)
        print(f"  [PintarNusa] {table_name}: {tables[table_name].count()} rows")
    return tables


# ──────────────────────────────────────────────
# PARQUET I/O  (intermediate storage antar step)
# ──────────────────────────────────────────────

def save_to_parquet(tables: dict, platform_name: str):
    """Simpan DataFrames ke Parquet agar bisa dibaca step berikutnya."""
    for name, df in tables.items():
        path = os.path.join(cfg.PARQUET_DIR, platform_name, name).replace("\\", "/")
        df.write.mode("overwrite").parquet(path)
        print(f"    -> Saved: {path}")


def load_from_parquet(spark: SparkSession, platform_name: str,
                      table_names: list) -> dict:
    """Baca DataFrames dari Parquet yang sudah di-extract sebelumnya."""
    tables = {}
    for name in table_names:
        path = os.path.join(cfg.PARQUET_DIR, platform_name, name).replace("\\", "/")
        try:
            tables[name] = spark.read.parquet(path)
        except Exception:
            # Fallback jika folder kosong/tidak ada
            tables[name] = spark.sql("SELECT CAST(NULL AS STRING) AS id_dummy WHERE 1=0")
    return tables
