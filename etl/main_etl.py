"""
main_etl.py - Entry point PySpark ETL Pipeline
=========================================================================
Jalankan dengan:
  spark-submit --jars postgresql-42.x.jar,mysql-connector-j-8.x.jar main_etl.py --step <step>

Step yang tersedia:
  extract_zenibelajar   - Extract data ZeniBelajar (CSV)
  extract_ruangcerdas   - Extract data RuangCerdas (PostgreSQL)
  extract_kelasjuara    - Extract data KelasJuara (MySQL)
  extract_pintarnusa    - Extract data PintarNusa (CSV)
  transform_staging     - Transform semua sumber → staging (DataFrame only)
  load_staging          - Load staging DataFrame ke MySQL (append/upsert)
  transform_warehouse   - Transform staging → warehouse (DataFrame only)
  load_warehouse        - Load warehouse DataFrame ke MySQL (append/upsert)
  full                  - Jalankan seluruh pipeline
"""
import argparse
import sys
import os

# Menambah limit rekursi untuk menghindari Stack Overflow di beberapa env Windows/Python 3.14
sys.setrecursionlimit(2000)

from pyspark.sql import SparkSession

# Tambahkan parent dir ke path agar config bisa di-import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config as cfg
from extract import (extract_zenibelajar, extract_ruangcerdas,
                     extract_kelasjuara, extract_pintarnusa,
                     save_to_parquet, load_from_parquet)
import transform_staging as ts
import transform_warehouse as tw
import load_staging as ls
import load_warehouse as lw


def get_spark():
    """Inisialisasi SparkSession.

    Catatan: Saat dijalankan via Airflow (SparkSubmitOperator),
    packages sudah di-set oleh operator. Saat dijalankan manual,
    gunakan: spark-submit --packages <packages> main_etl.py --step <step>
    """
    return (SparkSession.builder
        .master("local[*]")
        .appName("ETL-Bimbel-Integration")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.jars.packages", ",".join(cfg.SPARK_PACKAGES))
        .config("spark.sql.shuffle.partitions", "10")
        # --- FIX for java.io.FileNotFoundException on WSL/Windows DrvFs ---
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        # ------------------------------------------------------------------
        .getOrCreate())


# ──────────────────────────────────────────────
# STEP FUNCTIONS
# ──────────────────────────────────────────────

def step_extract_zenibelajar(spark):
    print("=" * 60)
    print("STEP: Extract ZeniBelajar (CSV)")
    print("=" * 60)
    tables = extract_zenibelajar(spark)
    save_to_parquet(tables, "zenibelajar")
    print("DONE Extract ZeniBelajar selesai\n")


def step_extract_ruangcerdas(spark):
    print("=" * 60)
    print("STEP: Extract RuangCerdas (PostgreSQL)")
    print("=" * 60)
    tables = extract_ruangcerdas(spark)
    save_to_parquet(tables, "ruangcerdas")
    print("DONE Extract RuangCerdas selesai\n")


def step_extract_kelasjuara(spark):
    print("=" * 60)
    print("STEP: Extract KelasJuara (MongoDB)")
    print("=" * 60)
    tables = extract_kelasjuara(spark)
    save_to_parquet(tables, "kelasjuara")
    print("DONE Extract KelasJuara selesai\n")


def step_extract_pintarnusa(spark):
    print("=" * 60)
    print("STEP: Extract PintarNusa (CSV)")
    print("=" * 60)
    tables = extract_pintarnusa(spark)
    save_to_parquet(tables, "pintarnusa")
    print("DONE Extract PintarNusa selesai\n")


def _load_all_parquet(spark):
    """Helper: Load semua extracted Parquet data."""
    zb = load_from_parquet(spark, "zenibelajar",
        ["member","program_belajar","riwayat_belajar","nilai_tryout",
         "order_langganan","feedback_program","peserta_program"])
    rc = load_from_parquet(spark, "ruangcerdas",
        ["siswa","kelas","pendaftaran_kelas","log_video","hasil_kuis",
         "transaksi_paket","ulasan_kelas"])
    kj = load_from_parquet(spark, "kelasjuara",
        ["pengguna","produk_kelas","akses_materi","skor_evaluasi",
         "invoice_pembayaran","review_kelas","pembelian_kelas"])
    pn = load_from_parquet(spark, "pintarnusa",
        ["siswa_pintarnusa","katalog_program","enrolmen_program",
         "sesi_belajar","hasil_assessment","tagihan_program","ulasan_program"])
    return zb, rc, kj, pn


def _build_staging_tables(spark, zb, rc, kj, pn):
    """Helper: Build semua staging DataFrames (transform only, no write)."""
    print("  Building kamus_platform...")
    kamus_platform = ts.build_kamus_platform(spark)

    print("  Building kamus_mata_pelajaran...")
    kamus_mapel = ts.build_kamus_mata_pelajaran(spark, zb, rc, kj, pn)

    print("  Building peta_pengguna_lintas_platform...")
    peta_pengguna = ts.build_peta_pengguna(spark, zb, rc, kj, pn)
    peta_pengguna.cache()

    print("  Building peta_kelas_lintas_platform...")
    peta_kelas = ts.build_peta_kelas(spark, zb, rc, kj, pn)
    peta_kelas.cache()

    print("  Building olah_aktivitas_belajar...")
    olah_aktivitas = ts.build_olah_aktivitas(spark, zb, rc, kj, pn,
                                              peta_pengguna, peta_kelas)

    print("  Building olah_hasil_latihan...")
    olah_latihan = ts.build_olah_latihan(spark, zb, rc, kj, pn,
                                          peta_pengguna, peta_kelas)

    print("  Building olah_pendaftaran_program...")
    olah_pendaftaran = ts.build_olah_pendaftaran(spark, zb, rc, kj, pn, peta_pengguna, peta_kelas)

    print("  Building olah_transaksi...")
    olah_transaksi = ts.build_olah_transaksi(spark, zb, rc, kj, pn, peta_pengguna)

    print("  Building olah_ulasan...")
    olah_ulasan = ts.build_olah_ulasan(spark, zb, rc, kj, pn,
                                        peta_pengguna, peta_kelas)

    return {
        "kamus_platform": kamus_platform,
        "kamus_mata_pelajaran": kamus_mapel,
        "peta_pengguna_lintas_platform": peta_pengguna,
        "peta_kelas_lintas_platform": peta_kelas,
        "olah_aktivitas_belajar": olah_aktivitas,
        "olah_hasil_latihan": olah_latihan,
        "olah_pendaftaran_program": olah_pendaftaran,
        "olah_transaksi": olah_transaksi,
        "olah_ulasan": olah_ulasan,
    }


def step_transform_staging(spark):
    """Transform → staging DataFrames, lalu simpan ke Parquet staging."""
    print("=" * 60)
    print("STEP: Transform -> Staging")
    print("=" * 60)

    zb, rc, kj, pn = _load_all_parquet(spark)
    staging_tables = _build_staging_tables(spark, zb, rc, kj, pn)

    # Simpan staging DataFrames ke Parquet agar step load bisa baca
    for name, df in staging_tables.items():
        path = os.path.join(cfg.PARQUET_DIR, "_staging", name).replace("\\", "/")
        df.write.mode("overwrite").parquet(path)
        print(f"    -> Staged: {name}")

    print("DONE Transform Staging selesai\n")


def step_load_staging(spark):
    """Load staging DataFrames dari Parquet ke MySQL DB."""
    print("=" * 60)
    print("STEP: Load Staging -> MySQL")
    print("=" * 60)

    batch_id = cfg.get_batch_id()

    # Baca staging DataFrames dari Parquet
    staging_names = list(cfg.STAGING_REFERENCE_TABLES) + list(cfg.STAGING_TRANSACTIONAL_TABLES.keys())
    staging_tables = {}
    for name in staging_names:
        path = os.path.join(cfg.PARQUET_DIR, "_staging", name).replace("\\", "/")
        try:
            staging_tables[name] = spark.read.parquet(path)
        except Exception as e:
            print(f"  WARNING: Could not read {name}: {e}")

    ls.load_all_staging(spark, staging_tables, batch_id)
    print("DONE Load Staging selesai\n")


def step_transform_warehouse(spark):
    """Transform staging → warehouse DataFrames, simpan ke Parquet."""
    print("=" * 60)
    print("STEP: Transform -> Warehouse")
    print("=" * 60)

    # Read staging tables dari MySQL (sudah di-load oleh step sebelumnya)
    staging = {
        "kamus_platform": tw.read_staging(spark, "kamus_platform"),
        "kamus_mata_pelajaran": tw.read_staging(spark, "kamus_mata_pelajaran"),
        "peta_pengguna_lintas_platform": tw.read_staging(spark, "peta_pengguna_lintas_platform"),
        "peta_kelas_lintas_platform": tw.read_staging(spark, "peta_kelas_lintas_platform"),
        "olah_aktivitas_belajar": tw.read_staging(spark, "olah_aktivitas_belajar"),
        "olah_hasil_latihan": tw.read_staging(spark, "olah_hasil_latihan"),
        "olah_pendaftaran_program": tw.read_staging(spark, "olah_pendaftaran_program"),
        "olah_transaksi": tw.read_staging(spark, "olah_transaksi"),
        "olah_ulasan": tw.read_staging(spark, "olah_ulasan"),
    }

    # Build dimensions
    print("  Building dimensions...")
    dims = {
        "waktu": tw.build_dim_waktu(spark, staging),
        "platform": tw.build_dim_platform(spark, staging),
        "pengguna": tw.build_dim_pengguna(spark, staging),
        "kelas": tw.build_dim_kelas(spark, staging),
        "mata_pelajaran": tw.build_dim_mata_pelajaran(spark, staging),
        "jenjang": tw.build_dim_jenjang(spark, staging),
        "perangkat": tw.build_dim_perangkat(spark, staging),
        "paket": tw.build_dim_paket(spark, staging),
        "metode_bayar": tw.build_dim_metode_bayar(spark, staging),
    }
    for d in dims.values():
        d.cache()

    # Build facts
    print("  Building facts...")
    facts = {
        "fakta_pendaftaran_program": tw.build_fakta_pendaftaran(spark, staging, dims),
        "fakta_aktivitas_belajar": tw.build_fakta_aktivitas(spark, staging, dims),
        "fakta_hasil_latihan": tw.build_fakta_latihan(spark, staging, dims),
        "fakta_transaksi": tw.build_fakta_transaksi(spark, staging, dims),
        "fakta_ulasan": tw.build_fakta_ulasan(spark, staging, dims),
    }

    # Simpan ke Parquet agar step load bisa baca
    all_tables = {}
    for k, v in dims.items():
        tbl_name = f"dim_{k}" if not k.startswith("dim_") else k
        all_tables[tbl_name] = v
    all_tables.update(facts)

    for name, df in all_tables.items():
        path = os.path.join(cfg.PARQUET_DIR, "_warehouse", name).replace("\\", "/")
        df.write.mode("overwrite").parquet(path)
        print(f"    -> Staged: {name}")

    print("DONE Transform Warehouse selesai\n")


def step_load_warehouse(spark):
    """Load warehouse DataFrames dari Parquet ke MySQL DB."""
    print("=" * 60)
    print("STEP: Load Warehouse -> MySQL")
    print("=" * 60)

    batch_id = cfg.get_batch_id()

    # Baca warehouse DataFrames dari Parquet
    dim_names = ["dim_waktu", "dim_platform", "dim_pengguna", "dim_kelas",
                 "dim_mata_pelajaran", "dim_jenjang", "dim_perangkat",
                 "dim_paket", "dim_metode_bayar"]
    fact_names = ["fakta_pendaftaran_program", "fakta_aktivitas_belajar", "fakta_hasil_latihan",
                  "fakta_transaksi", "fakta_ulasan"]

    dim_tables = {}
    for name in dim_names:
        path = os.path.join(cfg.PARQUET_DIR, "_warehouse", name).replace("\\", "/")
        try:
            dim_tables[name] = spark.read.parquet(path)
        except Exception as e:
            print(f"  WARNING: Could not read {name}: {e}")

    fact_tables = {}
    for name in fact_names:
        path = os.path.join(cfg.PARQUET_DIR, "_warehouse", name).replace("\\", "/")
        try:
            fact_tables[name] = spark.read.parquet(path)
        except Exception as e:
            print(f"  WARNING: Could not read {name}: {e}")

    lw.load_all_warehouse(spark, dim_tables, fact_tables, batch_id)
    print("DONE Load Warehouse selesai\n")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ETL Bimbel Integration Pipeline")
    parser.add_argument("--step", required=True,
        choices=["extract_zenibelajar", "extract_ruangcerdas",
                 "extract_kelasjuara", "extract_pintarnusa",
                 "transform_staging", "load_staging",
                 "transform_warehouse", "load_warehouse",
                 "full"],
        help="Step ETL yang akan dijalankan")
    args = parser.parse_args()

    spark = get_spark()

    try:
        if args.step == "full":
            step_extract_zenibelajar(spark)
            step_extract_ruangcerdas(spark)
            step_extract_kelasjuara(spark)
            step_extract_pintarnusa(spark)
            step_transform_staging(spark)
            step_load_staging(spark)
            step_transform_warehouse(spark)
            step_load_warehouse(spark)
        elif args.step == "extract_zenibelajar":
            step_extract_zenibelajar(spark)
        elif args.step == "extract_ruangcerdas":
            step_extract_ruangcerdas(spark)
        elif args.step == "extract_kelasjuara":
            step_extract_kelasjuara(spark)
        elif args.step == "extract_pintarnusa":
            step_extract_pintarnusa(spark)
        elif args.step == "transform_staging":
            step_transform_staging(spark)
        elif args.step == "load_staging":
            step_load_staging(spark)
        elif args.step == "transform_warehouse":
            step_transform_warehouse(spark)
        elif args.step == "load_warehouse":
            step_load_warehouse(spark)

        print("=" * 60)
        print(f"PIPELINE STEP '{args.step}' BERHASIL")
        print("=" * 60)
    except Exception as e:
        print(f"ERROR pada step '{args.step}': {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
