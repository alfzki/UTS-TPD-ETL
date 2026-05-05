"""
transform_warehouse.py - Transformasi staging → warehouse (star schema)
=========================================================================
Membaca dari staging DB, membangun tabel dimensi dan fakta,
lalu menulis ke warehouse DB.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config as cfg


def read_staging(spark, table_name):
    """Baca satu tabel dari staging DB."""
    jdbc = cfg.STAGING_JDBC
    return spark.read.jdbc(
        url=jdbc["url"], table=table_name,
        properties={"user": jdbc["user"], "password": jdbc["password"],
                     "driver": jdbc["driver"]})


# ──────────────────────────────────────────────
# DIMENSION BUILDERS
# ──────────────────────────────────────────────

def build_dim_waktu(spark, staging):
    """dim_waktu: kumpulkan semua tanggal unik dari tabel olah."""
    dates = None
    date_cols = {
        "olah_aktivitas_belajar": "tanggal_aktivitas",
        "olah_hasil_latihan": "tanggal_latihan",
        "olah_transaksi": "tanggal_transaksi",
        "olah_ulasan": "tanggal_ulasan",
    }
    for tbl, col in date_cols.items():
        d = staging[tbl].select(F.col(col).alias("tanggal")).filter(F.col(col).isNotNull())
        dates = d if dates is None else dates.union(d)

    dates = dates.distinct()
    bulan_names = {1:"Januari",2:"Februari",3:"Maret",4:"April",5:"Mei",
                   6:"Juni",7:"Juli",8:"Agustus",9:"September",
                   10:"Oktober",11:"November",12:"Desember"}
    bulan_map = F.create_map([F.lit(x) for kv in bulan_names.items() for x in kv])

    w = Window.orderBy("tanggal")
    return (dates
        .withColumn("waktu_key", F.abs(F.crc32(F.col("tanggal").cast("string"))))
        .withColumn("bulan", F.month("tanggal"))
        .withColumn("nama_bulan", bulan_map[F.month("tanggal")])
        .withColumn("kuartal", F.concat(F.lit("Q"), F.quarter("tanggal").cast("string")))
        .withColumn("tahun", F.year("tanggal"))
        .select("waktu_key", "tanggal", "bulan", "nama_bulan", "kuartal", "tahun"))


def build_dim_platform(spark, staging):
    """dim_platform dari kamus_platform."""
    kp = staging["kamus_platform"]
    return (kp
        .withColumn("platform_key", F.abs(F.crc32(F.col("id_platform"))))
        .select("platform_key", "id_platform", "nama_platform", "jenis_platform"))


def build_dim_pengguna(spark, staging):
    """dim_pengguna dari peta_pengguna (distinct global users)."""
    pp = staging["peta_pengguna_lintas_platform"]
    # Platform key mapping
    dim_plat = build_dim_platform(spark, staging)
    
    users = (pp.groupBy("id_pengguna_global")
        .agg(
            F.first("nama_platform").alias("nama_platform"),
            F.first("nama_standar").alias("nama_pengguna"),
            F.first("email_asli").alias("email"),
            F.first("provinsi_standar").alias("provinsi"),
            F.first("tanggal_daftar").alias("tanggal_daftar"),
        ))
    
    return (users
        .join(dim_plat.select("platform_key", F.col("nama_platform").alias("plat_name")), 
              F.col("nama_platform") == F.col("plat_name"), "left")
        .withColumn("pengguna_key", F.abs(F.crc32(F.col("id_pengguna_global"))))
        .select("pengguna_key", F.col("id_pengguna_global").alias("id_pengguna_sumber"), 
                "platform_key", "nama_pengguna", "email", "provinsi", "tanggal_daftar"))


def build_dim_kelas(spark, staging):
    """dim_kelas dari peta_kelas."""
    pk = staging["peta_kelas_lintas_platform"]
    
    # Kita butuh keys dimensi lain
    dim_plat = build_dim_platform(spark, staging)
    dim_mapel = build_dim_mata_pelajaran(spark, staging)
    dim_jenjang = build_dim_jenjang(spark, staging)

    return (pk
        .dropDuplicates(["id_kelas_global"])
        .join(dim_plat.select("platform_key", F.col("id_platform").alias("ref_id_platform")), 
              pk.id_platform == F.col("ref_id_platform"), "left")
        .join(dim_mapel.select("mata_pelajaran_key", F.col("nama_mata_pelajaran").alias("mapel_name")), 
              pk.mata_pelajaran_standar == F.col("mapel_name"), "left")
        .join(dim_jenjang.select("jenjang_key", F.col("nama_jenjang").alias("jenjang_name")), 
              pk.jenjang_standar == F.col("jenjang_name"), "left")
        .withColumn("kelas_key", F.abs(F.crc32(F.col("id_kelas_global"))))
        .select("kelas_key", F.col("id_kelas_global").alias("id_kelas_sumber"),
            "platform_key", F.col("nama_kelas_standar").alias("nama_kelas"),
            "mata_pelajaran_key", "jenjang_key", 
            "tingkat_kesulitan_standar"))


def build_dim_perangkat(spark, staging):
    """dim_perangkat dari aktivitas belajar dan ulasan."""
    d1 = staging["olah_aktivitas_belajar"].select(F.col("perangkat_standar").alias("kategori_perangkat"))
    d2 = staging["olah_ulasan"].select(F.col("perangkat_standar").alias("kategori_perangkat"))
    
    df = d1.union(d2).distinct().filter(
        (F.col("kategori_perangkat").isNotNull()) & 
        (F.lower(F.col("kategori_perangkat")) != "tidak diketahui")
    )
    
    return (df
        .withColumn("perangkat_key", F.abs(F.crc32(F.col("kategori_perangkat"))))
        .select("perangkat_key", "kategori_perangkat"))


def build_dim_paket(spark, staging):
    """dim_paket: standardize all package types to 3 categories (gratis, reguler, premium)."""
    paket_map = F.create_map([F.lit(x) for kv in cfg.PACKAGE_MAPPING.items() for x in kv])
    
    # Get unique paket names and standardize them using the mapping
    pakets = (staging["olah_transaksi"]
        .select(F.col("nama_paket_standar").alias("jenis_paket"))
        .distinct()
        .filter(F.col("jenis_paket").isNotNull())
        .withColumn("jenis_paket_mapped", F.coalesce(paket_map[F.col("jenis_paket")], F.col("jenis_paket")))
        .select("jenis_paket_mapped")
        .distinct()
        .filter(F.col("jenis_paket_mapped").isin("gratis", "reguler", "premium")))
    
    return (pakets
        .withColumn("paket_key", F.abs(F.crc32(F.col("jenis_paket_mapped"))))
        .select("paket_key", F.col("jenis_paket_mapped").alias("jenis_paket")))


def build_dim_metode_bayar(spark, staging):
    """dim_metode_bayar dari transaksi."""
    methods = staging["olah_transaksi"].select(
        F.col("metode_bayar_standar").alias("nama_metode")).distinct().filter(
        F.col("nama_metode").isNotNull())
    
    method_cat_map = F.create_map([F.lit(x) for kv in cfg.PAYMENT_METHOD_CATEGORY.items() for x in kv])
    
    return (methods
        .withColumn("metode_bayar_key", F.abs(F.crc32(F.col("nama_metode"))))
        .withColumn("kategori_metode", F.coalesce(method_cat_map[F.col("nama_metode")], F.lit("lainnya")))
        .select("metode_bayar_key", "nama_metode", "kategori_metode"))


def build_dim_mata_pelajaran(spark, staging):
    """dim_mata_pelajaran dari kamus_mata_pelajaran staging."""
    km = staging["kamus_mata_pelajaran"]
    return (km.select(F.col("nama_mapel_standar").alias("nama_mata_pelajaran"))
        .distinct()
        .withColumn("mata_pelajaran_key", F.abs(F.crc32(F.col("nama_mata_pelajaran"))))
        .withColumn("kategori", 
            F.when(F.col("nama_mata_pelajaran").isin("Matematika", "Fisika", "Kimia", "Biologi"), "Sains")
             .when(F.col("nama_mata_pelajaran").isin("Ekonomi", "Sejarah", "Geografi", "Sosiologi"), "Soshum")
             .otherwise("Bahasa/Lainnya"))
        .select("mata_pelajaran_key", "nama_mata_pelajaran", "kategori"))


def build_dim_jenjang(spark, staging):
    """dim_jenjang dikumpulkan dari berbagai tabel staging."""
    j1 = staging["peta_pengguna_lintas_platform"].select(F.col("jenjang_standar").alias("jenjang"))
    j2 = staging["peta_kelas_lintas_platform"].select(F.col("jenjang_standar").alias("jenjang"))
    
    jenjangs = j1.union(j2).distinct().filter(F.col("jenjang").isNotNull())
    
    return (jenjangs
        .withColumn("jenjang_key", F.abs(F.crc32(F.col("jenjang"))))
        .withColumn("tingkat", 
            F.when(F.col("jenjang") == "SD", 1)
             .when(F.col("jenjang") == "SMP", 2)
             .when(F.col("jenjang") == "SMA", 3)
             .otherwise(4))
        .select("jenjang_key", F.col("jenjang").alias("nama_jenjang"), "tingkat"))


# ──────────────────────────────────────────────
# FACT BUILDERS
# ──────────────────────────────────────────────

def build_fakta_pendaftaran(spark, staging, dims):
    """fakta_pendaftaran_program: join olah_pendaftaran + dimensi."""
    op = staging["olah_pendaftaran_program"]
    return (op
        .join(dims["waktu"].select("waktu_key","tanggal"),
              op.tanggal_daftar == F.col("tanggal"), "left")
        .join(dims["platform"].select("platform_key", F.col("id_platform").alias("ref_id_platform")),
              op.id_platform == F.col("ref_id_platform"), "left")
        .join(dims["pengguna"].select("pengguna_key", F.col("id_pengguna_sumber").alias("ref_id_pengguna_global")),
              op.id_pengguna_global == F.col("ref_id_pengguna_global"), "left")
        .join(dims["kelas"].select("kelas_key", F.col("id_kelas_sumber").alias("ref_id_kelas_global")),
              op.id_kelas_global == F.col("ref_id_kelas_global"), "left")
        .withColumn("aktif_flag", F.when(F.col("status_pendaftaran_standar") == "aktif", 1).otherwise(0))
        .withColumn("batal_flag", F.when(F.col("status_pendaftaran_standar") == "batal", 1).otherwise(0))
        .select(
            "waktu_key", "platform_key", "pengguna_key", "kelas_key",
            F.col("id_pendaftaran").alias("id_pendaftaran_sumber"), 
            F.col("status_pendaftaran_standar").alias("status_pendaftaran"), 
            "aktif_flag", "batal_flag"
        ))


def build_fakta_aktivitas(spark, staging, dims):
    """fakta_aktivitas_belajar: join olah + dimensi."""
    oa = staging["olah_aktivitas_belajar"]
    return (oa
        .join(dims["waktu"].select("waktu_key","tanggal"),
              oa.tanggal_aktivitas == F.col("tanggal"), "left")
        .join(dims["platform"].select("platform_key", F.col("id_platform").alias("ref_id_platform")),
              oa.id_platform == F.col("ref_id_platform"), "left")
        .join(dims["pengguna"].select("pengguna_key", F.col("id_pengguna_sumber").alias("ref_id_pengguna_global")),
              oa.id_pengguna_global == F.col("ref_id_pengguna_global"), "left")
        .join(dims["kelas"].select("kelas_key", F.col("id_kelas_sumber").alias("ref_id_kelas_global")),
              oa.id_kelas_global == F.col("ref_id_kelas_global"), "left")
        .join(dims["perangkat"].select("perangkat_key",F.col("kategori_perangkat").alias("jenis_perangkat")),
              oa.perangkat_standar == F.col("jenis_perangkat"), "left")
        .select(
            "waktu_key", "platform_key", "pengguna_key", "kelas_key", "perangkat_key",
            F.col("id_aktivitas").alias("id_aktivitas_sumber"),
            F.col("durasi_belajar_menit").cast("int").alias("durasi_menit")
        ))


def build_fakta_latihan(spark, staging, dims):
    """fakta_hasil_latihan: join olah latihan + dimensi."""
    oh = staging["olah_hasil_latihan"]

    return (oh
        .join(dims["waktu"].select("waktu_key","tanggal"),
              oh.tanggal_latihan == F.col("tanggal"), "left")
        .join(dims["platform"].select("platform_key", F.col("id_platform").alias("ref_id_platform")),
              oh.id_platform == F.col("ref_id_platform"), "left")
        .join(dims["pengguna"].select("pengguna_key", F.col("id_pengguna_sumber").alias("ref_id_pengguna_global")),
              oh.id_pengguna_global == F.col("ref_id_pengguna_global"), "left") 
        .join(dims["kelas"].select("kelas_key", F.col("id_kelas_sumber").alias("ref_id_kelas_global")),
              oh.id_kelas_global == F.col("ref_id_kelas_global"), "left")       
        .withColumn("lulus_flag", F.when(F.col("status_lulus") == "lulus", 1).otherwise(0))
        .select("waktu_key","platform_key","pengguna_key","kelas_key",
                F.col("id_hasil").alias("id_hasil_sumber"),
                "nilai_standar", 
                F.col("status_lulus").alias("status_lulus"), 
                "lulus_flag"))


def build_fakta_transaksi(spark, staging, dims):
    """fakta_transaksi: join olah + dimensi."""
    ot = staging["olah_transaksi"]
    dp = dims["platform"].select("platform_key", F.col("id_platform").alias("ref_id_platform"))
    dpa = dims["paket"].select("paket_key", F.col("jenis_paket").alias("ref_jenis_paket"))
    
    return (ot
        .join(dims["waktu"].select("waktu_key","tanggal"),
              ot.tanggal_transaksi == F.col("tanggal"), "left")
        .join(dp, ot.id_platform == dp.ref_id_platform, "left")
        .join(dpa, ot.nama_paket_standar == dpa.ref_jenis_paket, "left")
        .join(dims["pengguna"].select("pengguna_key", F.col("id_pengguna_sumber").alias("ref_id_pengguna_global")),
              ot.id_pengguna_global == F.col("ref_id_pengguna_global"), "left")
        .join(dims["metode_bayar"].select("metode_bayar_key",F.col("nama_metode").alias("metode_bayar")),
              ot.metode_bayar_standar == F.col("metode_bayar"), "left")
        .withColumn("berhasil_flag", F.when(F.col("status_bayar_standar") == "berhasil", 1).otherwise(0))
        .withColumn("gagal_flag", F.when(F.col("status_bayar_standar") == "gagal", 1).otherwise(0))
        .withColumn("pending_flag", F.when(F.col("status_bayar_standar") == "pending", 1).otherwise(0))
        .select("waktu_key","platform_key","pengguna_key","paket_key",
                "metode_bayar_key", F.col("id_transaksi").alias("id_transaksi_sumber"),
                ot.jumlah_bayar,
                F.col("status_bayar_standar").alias("status_bayar"),
                "berhasil_flag", "gagal_flag", "pending_flag"))


def build_fakta_ulasan(spark, staging, dims):
    """fakta_ulasan: join olah + dimensi."""
    ou = staging["olah_ulasan"]
    return (ou
        .join(dims["waktu"].select("waktu_key","tanggal"),
              ou.tanggal_ulasan == F.col("tanggal"), "left")
        .join(dims["platform"].select("platform_key", F.col("id_platform").alias("ref_id_platform")),
              ou.id_platform == F.col("ref_id_platform"), "left")
        .join(dims["pengguna"].select("pengguna_key", F.col("id_pengguna_sumber").alias("ref_id_pengguna_global")),
              ou.id_pengguna_global == F.col("ref_id_pengguna_global"), "left")
        .join(dims["kelas"].select("kelas_key", F.col("id_kelas_sumber").alias("ref_id_kelas_global")),
              ou.id_kelas_global == F.col("ref_id_kelas_global"), "left")
        .withColumn("komentar_tersedia_flag", F.when(F.col("komentar").isNotNull(), 1).otherwise(0))
            .select("waktu_key","platform_key","pengguna_key","kelas_key",
                F.col("id_ulasan").alias("id_ulasan_sumber"),
                "rating_standar_5",
                "komentar_tersedia_flag"))


# NOTE: write_warehouse() telah dipindahkan ke load_warehouse.py
# untuk pemisahan antara logika Transform dan Load.
