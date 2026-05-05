"""
load_warehouse.py - Load data ke warehouse DB (terpisah dari transform)
=========================================================================
Strategi:
- Dimensi: overwrite (key hash-based stabil, data kecil, FK tetap valid)
- Fakta: append + deduplikasi berdasarkan composite key
- Setiap record fakta ditambah kolom etl_loaded_at dan etl_batch_id
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import config as cfg
import pymysql


def _add_etl_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """Tambahkan kolom ETL tracking ke DataFrame."""
    return (df
        .withColumn("etl_loaded_at", F.current_timestamp())
        .withColumn("etl_batch_id", F.lit(batch_id)))


def _get_jdbc_props():
    """Properties JDBC untuk warehouse DB."""
    jdbc = cfg.WAREHOUSE_JDBC
    return jdbc, {
        "user": jdbc["user"],
        "password": jdbc["password"],
        "driver": jdbc["driver"],
    }


def _read_existing(spark: SparkSession, table_name: str) -> DataFrame:
    """Baca tabel existing dari warehouse DB. Return None jika belum ada."""
    jdbc, props = _get_jdbc_props()
    try:
        return spark.read.jdbc(url=jdbc["url"], table=table_name, properties=props)
    except Exception:
        return None


# ──────────────────────────────────────────────
# DIMENSION LOADING
# ──────────────────────────────────────────────

DIM_ORDER = [
    "dim_waktu", "dim_platform", "dim_mata_pelajaran", "dim_jenjang",
    "dim_pengguna", "dim_kelas", "dim_perangkat",
    "dim_paket", "dim_metode_bayar",
]

def load_dimensions(dim_tables: dict, batch_id: str):
    """
    Dimensi: overwrite + drop table first untuk menghindari schema mismatch.
    Ini memastikan kolom etl_loaded_at dan etl_batch_id selalu dengan tipe yang benar.
    """
    jdbc, props = _get_jdbc_props()

    # Explicit CREATE TABLE DDL with PRIMARY KEY constraints.
    # Spark's overwrite mode creates tables WITHOUT primary keys, which breaks
    # MySQL FK references in fact tables. We create tables manually then append.
    DIM_CREATE_DDL = {
        "dim_waktu": """
            CREATE TABLE dim_waktu (
                waktu_key BIGINT PRIMARY KEY,
                tanggal DATE NOT NULL,
                bulan INT,
                nama_bulan VARCHAR(50),
                kuartal VARCHAR(2),
                tahun INT,
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50)
            )""",
        "dim_platform": """
            CREATE TABLE dim_platform (
                platform_key BIGINT PRIMARY KEY,
                id_platform VARCHAR(50) NOT NULL,
                nama_platform VARCHAR(100),
                jenis_platform VARCHAR(100),
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50)
            )""",
        "dim_pengguna": """
            CREATE TABLE dim_pengguna (
                pengguna_key BIGINT PRIMARY KEY,
                id_pengguna_sumber VARCHAR(100) NOT NULL,
                platform_key BIGINT,
                nama_pengguna VARCHAR(200),
                email VARCHAR(200),
                provinsi VARCHAR(100),
                tanggal_daftar DATE,
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key)
            )""",
        "dim_kelas": """
            CREATE TABLE dim_kelas (
                kelas_key BIGINT PRIMARY KEY,
                id_kelas_sumber VARCHAR(100) NOT NULL,
                platform_key BIGINT,
                nama_kelas VARCHAR(255),
                mata_pelajaran_key BIGINT,
                jenjang_key BIGINT,
                tingkat_kesulitan_standar VARCHAR(50),
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
                FOREIGN KEY (mata_pelajaran_key) REFERENCES dim_mata_pelajaran (mata_pelajaran_key),
                FOREIGN KEY (jenjang_key) REFERENCES dim_jenjang (jenjang_key)
            )""",
        "dim_mata_pelajaran": """
            CREATE TABLE dim_mata_pelajaran (
                mata_pelajaran_key BIGINT PRIMARY KEY,
                nama_mata_pelajaran VARCHAR(100),
                kategori VARCHAR(50),
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50),
                UNIQUE KEY (nama_mata_pelajaran)
            )""",
        "dim_jenjang": """
            CREATE TABLE dim_jenjang (
                jenjang_key BIGINT PRIMARY KEY,
                nama_jenjang VARCHAR(50),
                tingkat INT,
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50),
                UNIQUE KEY (nama_jenjang)
            )""",
        "dim_perangkat": """
            CREATE TABLE dim_perangkat (
                perangkat_key BIGINT PRIMARY KEY,
                kategori_perangkat VARCHAR(50),
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50)
            )""",
        "dim_paket": """
            CREATE TABLE dim_paket (
                paket_key BIGINT PRIMARY KEY,
                jenis_paket VARCHAR(50) NOT NULL,
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50),
                UNIQUE KEY (jenis_paket)
            )""",
        "dim_metode_bayar": """
            CREATE TABLE dim_metode_bayar (
                metode_bayar_key BIGINT PRIMARY KEY,
                nama_metode VARCHAR(100),
                kategori_metode VARCHAR(50),
                etl_loaded_at TIMESTAMP,
                etl_batch_id VARCHAR(50),
                UNIQUE KEY (nama_metode)
            )""",
    }

    # Parse JDBC URL once: jdbc:mysql://host:port/database
    url_parts = jdbc["url"].replace("jdbc:mysql://", "").split("/")
    db_host_port = url_parts[0]
    db_name = url_parts[1].split("?")[0]  # strip query params
    db_host = db_host_port.split(":")[0]
    db_port = int(db_host_port.split(":")[1]) if ":" in db_host_port else 3306

    # Construction of Spark URL with FK checks disabled
    spark_url = jdbc["url"]
    separator = "&" if "?" in spark_url else "?"
    spark_url_no_fk = f"{spark_url}{separator}sessionVariables=FOREIGN_KEY_CHECKS=0"

    for name in DIM_ORDER:
        if name in dim_tables:
            try:
                conn = pymysql.connect(
                    host=db_host, port=db_port,
                    user=props["user"], password=props["password"],
                    database=db_name
                )
                cursor = conn.cursor()
                # Disable FK checks so we can drop parent tables freely
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute(f"DROP TABLE IF EXISTS {name}")
                # Recreate with explicit DDL including PRIMARY KEY
                if name in DIM_CREATE_DDL:
                    cursor.execute(DIM_CREATE_DDL[name])
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                conn.commit()
                cursor.close()
                conn.close()
                print(f"  [Warehouse] {name}: table dropped and recreated (schema reset)")
            except Exception as e:
                print(f"  [Warehouse] {name}: drop/create failed: {str(e)}")

            df = _add_etl_columns(dim_tables[name], batch_id)

            # Use truncate=true to preserve the schema created by pymysql.
            # Use spark_url_no_fk to allow truncation of tables referenced by FKs.
            df.coalesce(1).write.option("truncate", "true").jdbc(
                url=spark_url_no_fk, table=name,
                mode="overwrite", properties=props)
            print(f"  [Warehouse] {name}: {dim_tables[name].count()} rows written (recreated)")


# ──────────────────────────────────────────────
# FACT LOADING
# ──────────────────────────────────────────────

# Composite keys untuk deduplikasi fakta
FACT_COMPOSITE_KEYS = {
    "fakta_pendaftaran_program": ["waktu_key", "platform_key", "pengguna_key", "kelas_key"],
    "fakta_aktivitas_belajar": ["waktu_key", "platform_key", "pengguna_key",
                                 "kelas_key", "perangkat_key"],
    "fakta_hasil_latihan": ["waktu_key", "platform_key", "pengguna_key",
                             "kelas_key"],
    "fakta_transaksi": ["waktu_key", "platform_key", "pengguna_key",
                         "paket_key", "metode_bayar_key"],
    "fakta_ulasan": ["waktu_key", "platform_key", "pengguna_key", "kelas_key"],
}

def load_facts(spark: SparkSession, fact_tables: dict, batch_id: str):
    """
    Fakta: append hanya record baru (anti-join dengan data existing).
    """
    jdbc, props = _get_jdbc_props()

    # Column type overrides untuk fact tables (digunakan Spark jika create table)
    FACT_COLUMN_TYPES = {
        "fakta_pendaftaran_program": "waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT, id_pendaftaran_sumber VARCHAR(100), status_pendaftaran VARCHAR(50), aktif_flag TINYINT, batal_flag TINYINT, etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100)",
        "fakta_aktivitas_belajar": "waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT, perangkat_key BIGINT, id_aktivitas_sumber VARCHAR(100), durasi_menit INT, etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100)",
        "fakta_hasil_latihan": "waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT, id_hasil_sumber VARCHAR(100), nilai_standar DECIMAL(5,2), status_lulus VARCHAR(50), lulus_flag TINYINT, etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100)",
        "fakta_transaksi": "waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, paket_key BIGINT, metode_bayar_key BIGINT, id_transaksi_sumber VARCHAR(100), jumlah_bayar DECIMAL(15,2), status_bayar VARCHAR(50), berhasil_flag TINYINT, gagal_flag TINYINT, pending_flag TINYINT, etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100)",
        "fakta_ulasan": "waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT, id_ulasan_sumber VARCHAR(100), rating_standar_5 DECIMAL(3,1), komentar_tersedia_flag TINYINT, etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100)",
    }

    # Explicit CREATE TABLE DDL untuk fact tables.
    FACT_CREATE_DDL = {
        "fakta_pendaftaran_program": """
            CREATE TABLE fakta_pendaftaran_program (
                pendaftaran_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT,
                id_pendaftaran_sumber VARCHAR(100),
                status_pendaftaran VARCHAR(50), aktif_flag TINYINT, batal_flag TINYINT,
                etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100),
                FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
                FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
                FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key)
            )""",
        "fakta_aktivitas_belajar": """
            CREATE TABLE fakta_aktivitas_belajar (
                aktivitas_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT, perangkat_key BIGINT,
                id_aktivitas_sumber VARCHAR(100),
                durasi_menit INT,
                etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100),
                FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
                FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
                FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key),
                FOREIGN KEY (perangkat_key) REFERENCES dim_perangkat (perangkat_key)
            )""",
        "fakta_hasil_latihan": """
            CREATE TABLE fakta_hasil_latihan (
                hasil_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT,
                id_hasil_sumber VARCHAR(100),
                nilai_standar DECIMAL(5,2),
                status_lulus VARCHAR(50), lulus_flag TINYINT,
                etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100),
                FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
                FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
                FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key)
            )""",
        "fakta_transaksi": """
            CREATE TABLE fakta_transaksi (
                transaksi_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, paket_key BIGINT, metode_bayar_key BIGINT,
                id_transaksi_sumber VARCHAR(100), jumlah_bayar DECIMAL(15,2),
                status_bayar VARCHAR(50),
                berhasil_flag TINYINT, gagal_flag TINYINT, pending_flag TINYINT,
                etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100),
                FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
                FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
                FOREIGN KEY (paket_key) REFERENCES dim_paket (paket_key),
                FOREIGN KEY (metode_bayar_key) REFERENCES dim_metode_bayar (metode_bayar_key)
            )""",
        "fakta_ulasan": """
            CREATE TABLE fakta_ulasan (
                ulasan_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
                waktu_key BIGINT, platform_key BIGINT, pengguna_key BIGINT, kelas_key BIGINT,
                id_ulasan_sumber VARCHAR(100),
                rating_standar_5 DECIMAL(3,1), komentar_tersedia_flag TINYINT,
                etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(100),
                FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
                FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
                FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
                FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key)
            )""",
    }

    for name, composite_keys in FACT_COMPOSITE_KEYS.items():
        if name not in fact_tables:
            continue

        df = fact_tables[name]
        df_with_etl = _add_etl_columns(df, batch_id)

        write_props = {**props}
        if name in FACT_COLUMN_TYPES:
            write_props["createTableColumnTypes"] = FACT_COLUMN_TYPES[name]

        # Use session variables to disable FK checks during Spark write
        spark_url = jdbc["url"]
        separator = "&" if "?" in spark_url else "?"
        spark_url_no_fk = f"{spark_url}{separator}sessionVariables=FOREIGN_KEY_CHECKS=0"

        # Fakta lain: append + dedup
        existing = _read_existing(spark, name)

        if existing is not None and existing.count() > 0:
            # Build join condition dari composite keys
            # Anti-join: hanya record yang belum ada
            join_cond = None
            for k in composite_keys:
                cond = df_with_etl[k].eqNullSafe(existing[k])
                join_cond = cond if join_cond is None else join_cond & cond

            new_data = df_with_etl.join(existing, join_cond, "left_anti")
            new_count = new_data.count()

            if new_count > 0:
                new_data.coalesce(1).write.option("truncate", "true").jdbc(
                    url=spark_url_no_fk, table=name,
                    mode="append", properties=write_props)
                print(f"  [Warehouse] {name}: {new_count} new rows appended "
                      f"(skipped {df.count() - new_count} existing)")
            else:
                print(f"  [Warehouse] {name}: 0 new rows "
                      f"(all {df.count()} already exist)")
        else:
            # Tabel kosong/belum ada: tulis semua
            # Drop/recreate first to avoid schema mismatch and ensure explicit DDL is used
            try:
                url_parts = jdbc["url"].replace("jdbc:mysql://", "").split("/")
                db_host_port = url_parts[0]
                db_name = url_parts[1].split("?")[0]
                db_host = db_host_port.split(":")[0]
                db_port = int(db_host_port.split(":")[1]) if ":" in db_host_port else 3306
                
                conn = pymysql.connect(host=db_host, port=db_port, user=props["user"], password=props["password"], database=db_name)
                cursor = conn.cursor()
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute(f"DROP TABLE IF EXISTS {name}")
                if name in FACT_CREATE_DDL:
                    cursor.execute(FACT_CREATE_DDL[name])
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                conn.commit()
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"  [Warehouse] {name}: drop/create failed: {str(e)}")

            df_with_etl.coalesce(1).write.option("truncate", "true").jdbc(
                url=spark_url_no_fk, table=name,
                mode="overwrite", properties=write_props)
            print(f"  [Warehouse] {name}: {df.count()} rows written (initial load)")


def load_all_warehouse(spark: SparkSession, dim_tables: dict,
                       fact_tables: dict, batch_id: str):
    """
    Orchestrate loading semua tabel warehouse ke MySQL.

    Args:
        spark: SparkSession
        dim_tables: dict nama_dim -> DataFrame
        fact_tables: dict nama_fakta -> DataFrame
        batch_id: ID batch ETL saat ini
    """
    print(f"\n  Loading warehouse tables (batch: {batch_id})...")

    # 1. Load dimensi dulu (overwrite, key stabil)
    load_dimensions(dim_tables, batch_id)

    # 2. Load fakta (append + dedup)
    load_facts(spark, fact_tables, batch_id)

    print(f"  Loading warehouse selesai (batch: {batch_id})\n")
