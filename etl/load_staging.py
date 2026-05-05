"""
load_staging.py - Load data ke staging DB (terpisah dari transform)
=========================================================================
Strategi:
- Tabel referensi (kamus, peta, performa): overwrite (data kecil, sifatnya snapshot)
- Tabel transaksional (olah_*): append + deduplikasi berdasarkan primary key
- Setiap record ditambah kolom etl_loaded_at dan etl_batch_id
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import config as cfg
import pymysql


# Column type overrides agar Spark membuat kolom sesuai schema staging
STAGING_COLUMN_TYPES = {
    "kamus_platform": "id_platform VARCHAR(50), nama_platform VARCHAR(100), jenis_platform VARCHAR(100), keterangan VARCHAR(1000), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "kamus_mata_pelajaran": "nama_mapel_standar VARCHAR(100), nama_mapel_sumber VARCHAR(100), nama_platform VARCHAR(100), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "peta_pengguna_lintas_platform": "id_pengguna_global VARCHAR(50), nama_platform VARCHAR(100), id_pengguna_platform VARCHAR(50), email_hash VARCHAR(255), email_asli VARCHAR(200), nama_standar VARCHAR(150), jenjang_standar VARCHAR(50), kelas_sekolah_standar VARCHAR(50), provinsi_standar VARCHAR(100), tanggal_daftar DATE, confidence_score DECIMAL(3,2), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "peta_kelas_lintas_platform": "id_kelas_global VARCHAR(50), nama_platform VARCHAR(100), id_kelas_platform VARCHAR(50), id_platform VARCHAR(50), nama_kelas_standar VARCHAR(150), mata_pelajaran_standar VARCHAR(100), jenjang_standar VARCHAR(50), tingkat_kesulitan_standar VARCHAR(50), pengajar VARCHAR(100), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "olah_aktivitas_belajar": "id_aktivitas VARCHAR(100), id_platform VARCHAR(50), id_pengguna_global VARCHAR(50), id_kelas_global VARCHAR(50), tanggal_aktivitas DATE, durasi_belajar_menit DECIMAL(10,2), perangkat_standar VARCHAR(50), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "olah_hasil_latihan": "id_hasil VARCHAR(100), id_platform VARCHAR(50), id_pengguna_global VARCHAR(50), id_kelas_global VARCHAR(50), tanggal_latihan DATE, nilai_standar DECIMAL(5,2), status_lulus VARCHAR(50), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "olah_transaksi": "id_transaksi VARCHAR(100), id_platform VARCHAR(50), id_pengguna_global VARCHAR(50), tanggal_transaksi DATE, nama_paket_standar VARCHAR(100), metode_bayar_standar VARCHAR(50), jumlah_bayar DECIMAL(15,2), status_bayar_standar VARCHAR(50), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "olah_ulasan": "id_ulasan VARCHAR(100), id_platform VARCHAR(50), id_pengguna_global VARCHAR(50), id_kelas_global VARCHAR(50), tanggal_ulasan DATE, rating_standar_5 DECIMAL(3,1), komentar VARCHAR(1000), perangkat_standar VARCHAR(50), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
    "olah_pendaftaran_program": "id_pendaftaran VARCHAR(100), id_platform VARCHAR(50), id_pengguna_global VARCHAR(50), id_kelas_global VARCHAR(50), tanggal_daftar DATE, status_pendaftaran_standar VARCHAR(50), etl_loaded_at TIMESTAMP, etl_batch_id VARCHAR(50)",
}


STAGING_CREATE_DDL = {
    "kamus_platform": """
        CREATE TABLE kamus_platform (
            id_platform VARCHAR(50) PRIMARY KEY,
            nama_platform VARCHAR(100),
            jenis_platform VARCHAR(100),
            keterangan TEXT,
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
    "kamus_mata_pelajaran": """
        CREATE TABLE kamus_mata_pelajaran (
            id_mapel INT AUTO_INCREMENT PRIMARY KEY,
            nama_mapel_standar VARCHAR(100),
            nama_mapel_sumber VARCHAR(100),
            nama_platform VARCHAR(100),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
    "peta_pengguna_lintas_platform": """
        CREATE TABLE peta_pengguna_lintas_platform (
            id_pengguna_global VARCHAR(50),
            nama_platform VARCHAR(100),
            id_pengguna_platform VARCHAR(50),
            email_hash VARCHAR(255),
            email_asli VARCHAR(200),
            nama_standar VARCHAR(150),
            jenjang_standar VARCHAR(50),
            kelas_sekolah_standar VARCHAR(50),
            provinsi_standar VARCHAR(100),
            tanggal_daftar DATE,
            confidence_score DECIMAL(3, 2),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50),
            PRIMARY KEY (id_pengguna_global, nama_platform, id_pengguna_platform)
        )""",
    "peta_kelas_lintas_platform": """
        CREATE TABLE peta_kelas_lintas_platform (
            id_kelas_global VARCHAR(50),
            nama_platform VARCHAR(100),
            id_kelas_platform VARCHAR(50),
            id_platform VARCHAR(50),
            nama_kelas_standar VARCHAR(150),
            mata_pelajaran_standar VARCHAR(100),
            jenjang_standar VARCHAR(50),
            tingkat_kesulitan_standar VARCHAR(50),
            pengajar VARCHAR(100),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50),
            PRIMARY KEY (id_kelas_global, nama_platform, id_kelas_platform)
        )""",
    "olah_aktivitas_belajar": """
        CREATE TABLE olah_aktivitas_belajar (
            id_aktivitas VARCHAR(100) PRIMARY KEY,
            id_platform VARCHAR(50),
            id_pengguna_global VARCHAR(50),
            id_kelas_global VARCHAR(50),
            tanggal_aktivitas DATE,
            durasi_belajar_menit DECIMAL(10, 2),
            perangkat_standar VARCHAR(50),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
    "olah_hasil_latihan": """
        CREATE TABLE olah_hasil_latihan (
            id_hasil VARCHAR(100) PRIMARY KEY,
            id_platform VARCHAR(50),
            id_pengguna_global VARCHAR(50),
            id_kelas_global VARCHAR(50),
            tanggal_latihan DATE,
            nilai_standar DECIMAL(5, 2),
            status_lulus VARCHAR(50),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
    "olah_transaksi": """
        CREATE TABLE olah_transaksi (
            id_transaksi VARCHAR(100) PRIMARY KEY,
            id_platform VARCHAR(50),
            id_pengguna_global VARCHAR(50),
            tanggal_transaksi DATE,
            nama_paket_standar VARCHAR(100),
            metode_bayar_standar VARCHAR(50),
            jumlah_bayar DECIMAL(15, 2),
            status_bayar_standar VARCHAR(50),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
    "olah_ulasan": """
        CREATE TABLE olah_ulasan (
            id_ulasan VARCHAR(100) PRIMARY KEY,
            id_platform VARCHAR(50),
            id_pengguna_global VARCHAR(50),
            id_kelas_global VARCHAR(50),
            tanggal_ulasan DATE,
            rating_standar_5 DECIMAL(3, 1),
            komentar TEXT,
            perangkat_standar VARCHAR(50),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
    "olah_pendaftaran_program": """
        CREATE TABLE olah_pendaftaran_program (
            id_pendaftaran VARCHAR(100) PRIMARY KEY,
            id_platform VARCHAR(50),
            id_pengguna_global VARCHAR(50),
            id_kelas_global VARCHAR(50),
            tanggal_daftar DATE,
            status_pendaftaran_standar VARCHAR(50),
            etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            etl_batch_id VARCHAR(50)
        )""",
}


def _add_etl_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """Tambahkan kolom ETL tracking ke DataFrame."""
    return (df
        .withColumn("etl_loaded_at", F.current_timestamp())
        .withColumn("etl_batch_id", F.lit(batch_id)))


def _get_jdbc_props():
    """Properties JDBC untuk staging DB."""
    jdbc = cfg.STAGING_JDBC
    return jdbc, {
        "user": jdbc["user"],
        "password": jdbc["password"],
        "driver": jdbc["driver"],
        "batchsize": "500",
    }


def _read_existing(spark: SparkSession, table_name: str) -> DataFrame:
    """Baca tabel existing dari staging DB. Return None jika belum ada."""
    jdbc, props = _get_jdbc_props()
    try:
        return spark.read.jdbc(url=jdbc["url"], table=table_name, properties=props)
    except Exception:
        return None


def _ensure_table_schema(table_name: str):
    """Drop and recreate a staging table using the explicit MySQL schema."""
    if table_name not in STAGING_CREATE_DDL:
        return

    jdbc = cfg.STAGING_JDBC
    url = jdbc["url"].replace("jdbc:mysql://", "")
    db_host_port, db_name = url.split("/", 1)
    db_host = db_host_port.split(":")[0]
    db_port = int(db_host_port.split(":")[1]) if ":" in db_host_port else 3306

    conn = pymysql.connect(
        host=db_host,
        port=db_port,
        user=jdbc["user"],
        password=jdbc["password"],
        database=db_name.split("?")[0],
    )
    try:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(STAGING_CREATE_DDL[table_name])
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def _write_overwrite(df: DataFrame, table_name: str, batch_id: str):
    """Tulis dengan mode overwrite (untuk tabel referensi)."""
    jdbc, props = _get_jdbc_props()
    df_with_etl = _add_etl_columns(df, batch_id)

    _ensure_table_schema(table_name)
    write_props = {**props}
    if table_name in STAGING_COLUMN_TYPES:
        write_props["createTableColumnTypes"] = STAGING_COLUMN_TYPES[table_name]
    df_with_etl.coalesce(1).write.jdbc(
        url=jdbc["url"], table=table_name,
        mode="append", properties=write_props)
    count = df.count()
    print(f"  [Staging] {table_name}: {count} rows written (overwrite)")


def _write_append_dedup(spark: SparkSession, df: DataFrame,
                        table_name: str, pk_col: str, batch_id: str):
    """Tulis dengan mode append, skip record yang sudah ada (deduplikasi)."""
    jdbc, props = _get_jdbc_props()
    df_with_etl = _add_etl_columns(df, batch_id)

    write_props = {**props}
    if table_name in STAGING_COLUMN_TYPES:
        write_props["createTableColumnTypes"] = STAGING_COLUMN_TYPES[table_name]

    # Coba baca data existing
    existing = _read_existing(spark, table_name)

    if existing is not None and existing.count() > 0:
        # Anti-join: hanya ambil record baru yang belum ada di DB
        existing_keys = existing.select(F.col(pk_col).alias("_existing_pk"))
        new_data = df_with_etl.join(
            existing_keys,
            df_with_etl[pk_col] == existing_keys["_existing_pk"],
            "left_anti"
        )
        new_count = new_data.count()
        if new_count > 0:
            new_data.coalesce(1).write.jdbc(
                url=jdbc["url"], table=table_name,
                mode="append", properties=write_props)
            print(f"  [Staging] {table_name}: {new_count} new rows appended "
                  f"(skipped {df.count() - new_count} existing)")
        else:
            print(f"  [Staging] {table_name}: 0 new rows (all {df.count()} already exist)")
    else:
        # Tabel kosong/belum ada: buat schema eksplisit lalu tulis semua
        _ensure_table_schema(table_name)
        df_with_etl.coalesce(1).write.jdbc(
            url=jdbc["url"], table=table_name,
            mode="append", properties=write_props)
        print(f"  [Staging] {table_name}: {df.count()} rows written (initial load)")


def load_all_staging(spark: SparkSession, staging_tables: dict, batch_id: str):
    """
    Orchestrate loading semua tabel staging ke MySQL.

    Args:
        spark: SparkSession
        staging_tables: dict nama_tabel -> DataFrame
        batch_id: ID batch ETL saat ini
    """
    print(f"\n  Loading staging tables (batch: {batch_id})...")

    # 1. Tabel referensi: overwrite (data kecil, sifatnya snapshot terkini)
    for name in cfg.STAGING_REFERENCE_TABLES:
        if name in staging_tables:
            _write_overwrite(staging_tables[name], name, batch_id)

    # 2. Tabel transaksional: append + deduplikasi
    for name, pk_col in cfg.STAGING_TRANSACTIONAL_TABLES.items():
        if name in staging_tables:
            _write_append_dedup(spark, staging_tables[name], name, pk_col, batch_id)

    print(f"  Loading staging selesai (batch: {batch_id})\n")
