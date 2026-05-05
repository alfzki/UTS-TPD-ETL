"""
transform_staging.py - Transformasi data mentah dari 4 sumber ke format staging
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config as cfg
import sys

sys.setrecursionlimit(2000)


def _first_existing_column(df, candidates):
    """Return the first column name that exists in df.columns, or None."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _get_device_standar_expr(df, device_col_candidates, device_map):
    """
    Safely get device standardization expression.
    Returns a Spark expression that maps device to standar, or 'tidak diketahui' if column not found.
    """
    device_col = _first_existing_column(df, device_col_candidates)
    if device_col:
        return F.coalesce(device_map[F.lower(F.trim(F.col(device_col)))], F.lit("tidak diketahui"))
    else:
        # No device column found, use default
        return F.lit("tidak diketahui")


def _standardize_level_expr(expr):
    """Normalize difficulty/level values using LEVEL_MAPPING when possible."""
    level_map = F.create_map([F.lit(x) for kv in cfg.LEVEL_MAPPING.items() for x in kv])
    normalized = F.lower(F.trim(expr.cast("string")))
    return F.coalesce(level_map[normalized], expr.cast("string"))


def build_kamus_platform(spark):
    """Tabel kamus_platform: daftar platform yang terintegrasi."""
    queries = []
    for k, v in cfg.PLATFORMS.items():
        queries.append(f"SELECT '{v['id']}' as id_platform, '{v['nama']}' as nama_platform, '{v['jenis']}' as jenis_platform, 'Platform {v['nama']}' as keterangan")
    query = " UNION ALL ".join(queries)
    return spark.sql(query)


def build_kamus_mata_pelajaran(spark, zb, rc, kj, pn):
    """Tabel kamus_mata_pelajaran: mapping mata pelajaran lintas platform."""
    subj_map = F.create_map([F.lit(x) for kv in cfg.SUBJECT_MAPPING.items() for x in kv])

    zb_m = zb["program_belajar"].select(
        F.col("mapel").alias("nama_mapel_standar"),
        F.col("mapel").alias("nama_mapel_sumber"),
        F.lit("ZeniBelajar").alias("nama_platform")
    ).distinct()
    
    rc_m = rc["kelas"].select(
        F.col("mata_pelajaran").alias("nama_mapel_standar"),
        F.col("mata_pelajaran").alias("nama_mapel_sumber"),
        F.lit("RuangCerdas").alias("nama_platform")
    ).distinct()
    
    kj_m = kj["produk_kelas"].select(
        F.coalesce(subj_map[F.col("subject_name")], F.col("subject_name")).alias("nama_mapel_standar"),
        F.col("subject_name").alias("nama_mapel_sumber"),
        F.lit("KelasJuara").alias("nama_platform")
    ).distinct()
    
    pn_m = pn["katalog_program"].select(
        F.coalesce(subj_map[F.col("bidang_studi")], F.col("bidang_studi")).alias("nama_mapel_standar"),
        F.col("bidang_studi").alias("nama_mapel_sumber"),
        F.lit("PintarNusa").alias("nama_platform")
    ).distinct()
    
    return zb_m.union(rc_m).union(kj_m).union(pn_m)


def build_peta_pengguna(spark, zb, rc, kj, pn):
    """Tabel peta_pengguna_lintas_platform: mapping pengguna lintas platform."""
    rc_siswa_class_col = "kelas_sekolah" if "kelas_sekolah" in rc["siswa"].columns else "kelas"
    rc_siswa_location_col = "domisili_provinsi" if "domisili_provinsi" in rc["siswa"].columns else ("kota" if "kota" in rc["siswa"].columns else None)
    rc_siswa_reg_col = _first_existing_column(rc["siswa"], ["tanggal_daftar", "tanggal_registrasi"])

    zb_users = zb["member"].select(
        F.lit("ZeniBelajar").alias("nama_platform"),
        F.col("kode_member").alias("id_pengguna_platform"),
        F.md5(F.lower(F.col("alamat_email"))).alias("email_hash"),
        F.col("alamat_email").alias("email_asli"),
        F.col("nama_lengkap").alias("nama_standar"),
        F.col("level_sekolah").alias("jenjang_standar"),
        F.col("tingkat_kelas").cast("string").alias("kelas_sekolah_standar"),
        F.when(
            F.col("domisili").isNull() | (F.trim(F.col("domisili")) == "") | (F.lower(F.trim(F.col("domisili"))) == "nan"),
            F.lit("Tidak Diketahui")
        ).otherwise(F.col("domisili")).alias("provinsi_standar"),
        F.col("waktu_registrasi").cast("date").alias("tanggal_daftar"),
    )
    rc_users = rc["siswa"].select(
        F.lit("RuangCerdas").alias("nama_platform"),
        F.col("id_siswa").cast("string").alias("id_pengguna_platform"),
        F.md5(F.lower(F.col("email"))).alias("email_hash"),
        F.col("email").alias("email_asli"),
        F.col("nama_siswa").alias("nama_standar"),
        F.col("jenjang").alias("jenjang_standar"),
        F.col(rc_siswa_class_col).cast("string").alias("kelas_sekolah_standar"),
        F.when(
            (F.col(rc_siswa_location_col).isNull() if rc_siswa_location_col else F.lit(True)) |
            ((F.trim(F.col(rc_siswa_location_col)) == "") if rc_siswa_location_col else F.lit(True)) |
            ((F.lower(F.trim(F.col(rc_siswa_location_col))) == "nan") if rc_siswa_location_col else F.lit(True)),
            F.lit("Tidak Diketahui")
        ).otherwise(F.col(rc_siswa_location_col) if rc_siswa_location_col else F.lit("Tidak Diketahui")).alias("provinsi_standar"),
        F.col(rc_siswa_reg_col).cast("date").alias("tanggal_daftar"),
    )
    kj_users = kj["pengguna"].select(
        F.lit("KelasJuara").alias("nama_platform"),
        F.col("user_id").alias("id_pengguna_platform"),
        F.md5(F.lower(F.col("email_user"))).alias("email_hash"),
        F.col("email_user").alias("email_asli"),
        F.col("nama").alias("nama_standar"),
        F.col("jenjang_pendidikan").alias("jenjang_standar"),
        F.col("kelas").cast("string").alias("kelas_sekolah_standar"),
        F.when(
            F.col("asal_provinsi").isNull() | (F.trim(F.col("asal_provinsi")) == "") | (F.lower(F.trim(F.col("asal_provinsi"))) == "nan"),
            F.lit("Tidak Diketahui")
        ).otherwise(F.col("asal_provinsi")).alias("provinsi_standar"),
        F.col("created_date").cast("date").alias("tanggal_daftar"),
    )
    # PintarNusa
    pn_users = pn["siswa_pintarnusa"].select(
        F.lit("PintarNusa").alias("nama_platform"),
        F.col("id_siswa").alias("id_pengguna_platform"),
        F.md5(F.lower(F.col("email"))).alias("email_hash"),
        F.col("email").alias("email_asli"),
        F.col("nama_siswa").alias("nama_standar"),
        F.col("tingkat_pendidikan").alias("jenjang_standar"),
        F.col("kelas_tingkat").cast("string").alias("kelas_sekolah_standar"),
        F.when(
            F.col("provinsi").isNull() | (F.trim(F.col("provinsi")) == "") | (F.lower(F.trim(F.col("provinsi"))) == "nan"),
            F.lit("Tidak Diketahui")
        ).otherwise(F.col("provinsi")).alias("provinsi_standar"),
        F.col("tanggal_gabung").cast("date").alias("tanggal_daftar"),
    )
    all_users = zb_users.union(rc_users).union(kj_users).union(pn_users)

    # Generate global ID berdasarkan email_hash — HASH-BASED (stabil antar run)
    global_ids = (all_users.select("email_hash").distinct()
        .withColumn("id_pengguna_global",
            F.concat(F.lit("USR-"),
                     F.upper(F.substring(F.md5(F.col("email_hash")), 1, 8)))))

    return (all_users.join(global_ids, "email_hash")
        .withColumn("confidence_score", F.lit(1.00))
        .select("id_pengguna_global", "nama_platform", "id_pengguna_platform",
                "email_hash", "email_asli", "nama_standar", "jenjang_standar",
                "kelas_sekolah_standar", "provinsi_standar", "tanggal_daftar",
                "confidence_score"))


def build_peta_kelas(spark, zb, rc, kj, pn):
    """Tabel peta_kelas_lintas_platform: mapping kelas lintas platform."""
    level_map = F.create_map([F.lit(x) for kv in cfg.LEVEL_MAPPING.items() for x in kv])
    subj_map = F.create_map([F.lit(x) for kv in cfg.SUBJECT_MAPPING.items() for x in kv])
    
    rc_level_col = _first_existing_column(rc["kelas"], ["tingkat_kesulitan", "tingkat"])
    rc_pengajar_col = _first_existing_column(rc["kelas"], ["pengajar", "instruktur", "guru"])

    zb_kelas = zb["program_belajar"].select(
        F.lit("ZeniBelajar").alias("nama_platform"),
        F.lit("PLT-ZB").alias("id_platform"),
        F.col("kode_program").alias("id_kelas_platform"),
        F.coalesce(subj_map[F.col("mapel")], F.col("mapel")).alias("mata_pelajaran_standar"),
        F.col("target_jenjang").alias("jenjang_standar"),
        level_map[F.col("kategori_level")].alias("tingkat_kesulitan_standar"),
        F.col("mentor").alias("pengajar")
    )
    
    rc_kelas = rc["kelas"].select(
        F.lit("RuangCerdas").alias("nama_platform"),
        F.lit("PLT-RC").alias("id_platform"),
        F.col("id_kelas").cast("string").alias("id_kelas_platform"),
        F.coalesce(subj_map[F.col("mata_pelajaran")], F.col("mata_pelajaran")).alias("mata_pelajaran_standar"),
        F.col("jenjang").alias("jenjang_standar"),
        _standardize_level_expr(F.col(rc_level_col) if rc_level_col else F.lit("menengah")).alias("tingkat_kesulitan_standar"),
        (F.col(rc_pengajar_col) if rc_pengajar_col else F.lit("Tidak Diketahui")).alias("pengajar")
    )
    
    kj_kelas = kj["produk_kelas"].select(
        F.lit("KelasJuara").alias("nama_platform"),
        F.lit("PLT-KJ").alias("id_platform"),
        F.col("produk_id").alias("id_kelas_platform"),
        F.coalesce(subj_map[F.col("subject_name")], F.col("subject_name")).alias("mata_pelajaran_standar"),
        F.when(F.col("school_level") == "Junior High", "SMP")
         .when(F.col("school_level") == "Senior High", "SMA")
         .otherwise(F.col("school_level")).alias("jenjang_standar"),
        level_map[F.col("difficulty")].alias("tingkat_kesulitan_standar"),
        F.col("tutor_name").alias("pengajar")
    )
    
    pn_kelas = pn["katalog_program"].select(
        F.lit("PintarNusa").alias("nama_platform"),
        F.lit("PLT-PN").alias("id_platform"),
        F.col("id_program").alias("id_kelas_platform"),
        F.coalesce(subj_map[F.col("bidang_studi")], F.col("bidang_studi")).alias("mata_pelajaran_standar"),
        F.col("jenjang").alias("jenjang_standar"),
        level_map[F.col("level_kesulitan")].alias("tingkat_kesulitan_standar"),
        F.col("pengajar").alias("pengajar")
    )
    
    all_kelas = zb_kelas.union(rc_kelas).union(kj_kelas).union(pn_kelas)

    return (all_kelas
        .withColumn("nama_kelas_standar", 
             F.concat_ws(" - ", F.col("mata_pelajaran_standar"), 
                         F.concat_ws(" ", F.col("jenjang_standar"), F.col("tingkat_kesulitan_standar"))))
        .withColumn("id_kelas_global",
            F.concat(F.lit("KLS-"),
                     F.upper(F.substring(F.md5(F.col("nama_kelas_standar")), 1, 8))))
        .select("id_kelas_global", "nama_platform", "id_kelas_platform", "id_platform",
            "nama_kelas_standar", "mata_pelajaran_standar", "jenjang_standar",
            "tingkat_kesulitan_standar", "pengajar"))


def build_olah_aktivitas(spark, zb, rc, kj, pn, peta_pengguna, peta_kelas):
    """Tabel olah_aktivitas_belajar: standarisasi aktivitas belajar."""
    pu = peta_pengguna.select("nama_platform", "id_pengguna_platform", "id_pengguna_global")
    pk = peta_kelas.select("nama_platform", "id_kelas_platform", "id_kelas_global")
    
    # Create device mapping
    device_map = F.create_map([F.lit(x) for kv in cfg.DEVICE_MAPPING.items() for x in kv])

    # ZeniBelajar
    zb_riwayat = zb["riwayat_belajar"].withColumn("_plat", F.lit("ZeniBelajar"))
    zb_device_expr = _get_device_standar_expr(zb_riwayat, ["device"], device_map)
    zb_act = (zb_riwayat
        .select(
            F.concat(F.lit("ZB-AK-"), F.col("id_riwayat")).alias("id_aktivitas"),
            F.lit("PLT-ZB").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("kode_member").alias("id_pengguna_platform"),
            F.col("kode_program").alias("id_kelas_platform"),
            F.to_date("waktu_mulai").alias("tanggal_aktivitas"),
            (F.col("lama_belajar_detik") / 60).cast("decimal(10,2)").alias("durasi_belajar_menit"),
            zb_device_expr.alias("perangkat_standar"),
        ))

    # RuangCerdas
    rc_log = rc["log_video"].withColumn("_plat", F.lit("RuangCerdas"))
    rc_tanggal_akses_col = _first_existing_column(rc_log, ["tanggal_akses", "waktu_mulai"])
    rc_durasi_col = _first_existing_column(rc_log, ["durasi_tonton_menit", "durasi_menit"])
    rc_device_expr = _get_device_standar_expr(rc_log, ["perangkat", "device"], device_map)
    rc_act = (rc_log
        .select(
            F.concat(F.lit("RC-AK-"), F.col("id_log")).alias("id_aktivitas"),
            F.lit("PLT-RC").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").cast("string").alias("id_pengguna_platform"),
            F.col("id_kelas").cast("string").alias("id_kelas_platform"),
            F.to_date(F.col(rc_tanggal_akses_col)).alias("tanggal_aktivitas"),
            F.col(rc_durasi_col).cast("decimal(10,2)").alias("durasi_belajar_menit"),
            rc_device_expr.alias("perangkat_standar"),
        ))

    # KelasJuara
    kj_akses = kj["akses_materi"].withColumn("_plat", F.lit("KelasJuara"))
    kj_device_expr = _get_device_standar_expr(kj_akses, ["platform_device"], device_map)
    kj_act = (kj_akses
        .select(
            F.concat(F.lit("KJ-AK-"), F.col("akses_id")).alias("id_aktivitas"),
            F.lit("PLT-KJ").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("user_id").alias("id_pengguna_platform"),
            F.col("produk_id").alias("id_kelas_platform"),
            F.col("tanggal").alias("tanggal_aktivitas"),
            (F.col("watch_time") * 60).cast("decimal(10,2)").alias("durasi_belajar_menit"),
            kj_device_expr.alias("perangkat_standar"),
        ))

    # PintarNusa
    pn_sesi = pn["sesi_belajar"].withColumn("_plat", F.lit("PintarNusa"))
    pn_device_expr = _get_device_standar_expr(pn_sesi, ["perangkat"], device_map)
    pn_act = (pn_sesi
        .select(
            F.concat(F.lit("PN-AK-"), F.col("id_sesi")).alias("id_aktivitas"),
            F.lit("PLT-PN").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").alias("id_pengguna_platform"),
            F.col("id_program").alias("id_kelas_platform"),
            F.col("tanggal_sesi").cast("date").alias("tanggal_aktivitas"),
            F.col("durasi_menit").cast("decimal(10,2)").alias("durasi_belajar_menit"),
            pn_device_expr.alias("perangkat_standar"),
        ))

    combined = zb_act.union(rc_act).union(kj_act).union(pn_act)

    result = (combined
        .join(pu, ["nama_platform", "id_pengguna_platform"], "left")
        .join(pk, ["nama_platform", "id_kelas_platform"], "left")
        .select("id_aktivitas", "id_platform", "id_pengguna_global",
            "id_kelas_global", "tanggal_aktivitas",
            "durasi_belajar_menit", "perangkat_standar")
        .filter(
            (F.col("tanggal_aktivitas") <= F.current_date()) & 
            (F.col("durasi_belajar_menit") >= 0)
        )
        .dropna(subset=["id_aktivitas", "id_pengguna_global", "id_kelas_global"])
        .dropDuplicates(["id_aktivitas"]))
    return result


def build_olah_latihan(spark, zb, rc, kj, pn, peta_pengguna, peta_kelas):
    """Tabel olah_hasil_latihan: standarisasi hasil latihan/evaluasi."""
    pu = peta_pengguna.select("nama_platform", "id_pengguna_platform", "id_pengguna_global")
    pk = peta_kelas.select("nama_platform", "id_kelas_platform", "id_kelas_global")

    # ZeniBelajar
    zb_lat = (zb["nilai_tryout"]
        .withColumn("_plat", F.lit("ZeniBelajar"))
        .select(
            F.concat(F.lit("ZB-HL-"), F.col("id_tryout")).alias("id_hasil"),
            F.lit("PLT-ZB").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("kode_member").alias("id_pengguna_platform"),
            F.col("kode_program").alias("id_kelas_platform"),
            F.to_date("tanggal_submit").alias("tanggal_latihan"),
            (F.col("skor") / 10).cast("decimal(5,2)").alias("nilai_standar"),
            F.when(F.col("skor") / 10 >= 70, "lulus").otherwise("tidak_lulus").alias("status_lulus"),
        ))

    # RuangCerdas
    rc_lat_id_col = _first_existing_column(rc["hasil_kuis"], ["id_hasil", "id_kuis"])
    rc_lat = (rc["hasil_kuis"]
        .withColumn("_plat", F.lit("RuangCerdas"))
        .select(
            F.concat(F.lit("RC-HL-"), F.col(rc_lat_id_col)).alias("id_hasil"),
            F.lit("PLT-RC").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").cast("string").alias("id_pengguna_platform"),
            F.col("id_kelas").cast("string").alias("id_kelas_platform"),
            F.to_date(F.col("tanggal_kuis")).alias("tanggal_latihan"),
            F.col("nilai").cast("decimal(5,2)").alias("nilai_standar"),
            F.when(F.col("nilai") >= 70, "lulus").otherwise("tidak_lulus").alias("status_lulus"),
        ))

    # KelasJuara
    kj_lat = (kj["skor_evaluasi"]
        .withColumn("_plat", F.lit("KelasJuara"))
        .select(
            F.concat(F.lit("KJ-HL-"), F.col("evaluasi_id")).alias("id_hasil"),
            F.lit("PLT-KJ").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("user_id").alias("id_pengguna_platform"),
            F.col("produk_id").alias("id_kelas_platform"),
            F.col("tanggal_evaluasi").alias("tanggal_latihan"),
            F.col("score_percent").cast("decimal(5,2)").alias("nilai_standar"),
            F.when(F.col("status_lulus") == "lulus", "lulus").otherwise("tidak_lulus").alias("status_lulus"),
        ))

    # PintarNusa
    pn_lat = (pn["hasil_assessment"]
        .withColumn("_plat", F.lit("PintarNusa"))
        .select(
            F.concat(F.lit("PN-HL-"), F.col("id_assessment")).alias("id_hasil"),
            F.lit("PLT-PN").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").alias("id_pengguna_platform"),
            F.col("id_program").alias("id_kelas_platform"),
            F.col("tanggal_assessment").cast("date").alias("tanggal_latihan"),
            F.col("nilai_akhir").cast("decimal(5,2)").alias("nilai_standar"),
            F.when(F.col("nilai_akhir") >= 75, "lulus").otherwise("tidak_lulus").alias("status_lulus"),
        ))

    combined = zb_lat.union(rc_lat).union(kj_lat).union(pn_lat)
    return (combined
        .join(pu, ["nama_platform", "id_pengguna_platform"], "left")
        .join(pk, ["nama_platform", "id_kelas_platform"], "left")
        .select("id_hasil", "id_platform", "id_pengguna_global",
                "id_kelas_global", "tanggal_latihan", 
                "nilai_standar", "status_lulus")
        .filter(
            (F.col("tanggal_latihan") <= F.current_date()) & 
            (F.col("nilai_standar").between(0, 100))
        )
        .dropna(subset=["id_hasil", "id_pengguna_global", "id_kelas_global"])
        .dropDuplicates(["id_hasil"]))


def build_olah_transaksi(spark, zb, rc, kj, pn, peta_pengguna):
    """Tabel olah_transaksi: standarisasi transaksi pembayaran."""
    pu = peta_pengguna.select("nama_platform", "id_pengguna_platform", "id_pengguna_global")
    status_map = F.create_map([F.lit(x) for kv in cfg.PAYMENT_STATUS_MAPPING.items() for x in kv])
    method_map = F.create_map([F.lit(x) for kv in cfg.PAYMENT_METHOD_MAPPING.items() for x in kv])
    paket_map = F.create_map([F.lit(x) for kv in cfg.PACKAGE_MAPPING.items() for x in kv])

    # ZeniBelajar
    zb_trx = (zb["order_langganan"]
        .withColumn("_plat", F.lit("ZeniBelajar"))
        .select(
            F.concat(F.lit("ZB-TR-"), F.col("id_order")).alias("id_transaksi"),
            F.lit("PLT-ZB").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("kode_member").alias("id_pengguna_platform"),
            F.to_date("tanggal_order").alias("tanggal_transaksi"),
            F.coalesce(paket_map[F.col("paket")], F.col("paket")).alias("nama_paket_standar"),
            method_map[F.col("channel_pembayaran")].alias("metode_bayar_standar"),
            F.col("total_harga").cast("decimal(15,2)").alias("jumlah_bayar"),
            F.coalesce(status_map[F.col("status_order")], F.lit("Unknown")).alias("status_bayar_standar"),
        ))

    # RuangCerdas
    rc_trx_date_col = _first_existing_column(rc["transaksi_paket"], ["tanggal_bayar", "tanggal_transaksi"])
    rc_trx_method_col = _first_existing_column(rc["transaksi_paket"], ["metode_bayar", "metode_pembayaran"])
    rc_trx_amount_col = _first_existing_column(rc["transaksi_paket"], ["jumlah_bayar", "harga"])
    rc_trx_status_col = _first_existing_column(rc["transaksi_paket"], ["status_bayar", "status_transaksi"])
    rc_trx = (rc["transaksi_paket"]
        .withColumn("_plat", F.lit("RuangCerdas"))
        .select(
            F.concat(F.lit("RC-TR-"), F.col("id_transaksi")).alias("id_transaksi"),
            F.lit("PLT-RC").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").cast("string").alias("id_pengguna_platform"),
            F.to_date(F.col(rc_trx_date_col)).alias("tanggal_transaksi"),
            F.coalesce(paket_map[F.col("nama_paket")], F.col("nama_paket")).alias("nama_paket_standar"),
            method_map[F.col(rc_trx_method_col)].alias("metode_bayar_standar"),
            F.col(rc_trx_amount_col).cast("decimal(15,2)").alias("jumlah_bayar"),
            F.coalesce(status_map[F.col(rc_trx_status_col)], F.lit("Unknown")).alias("status_bayar_standar"),
        ))

    # KelasJuara
    kj_trx = (kj["invoice_pembayaran"]
        .withColumn("_plat", F.lit("KelasJuara"))
        .select(
            F.concat(F.lit("KJ-TR-"), F.col("invoice_id")).alias("id_transaksi"),
            F.lit("PLT-KJ").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("user_id").alias("id_pengguna_platform"),
            F.col("tanggal_invoice").alias("tanggal_transaksi"),
            F.coalesce(paket_map[F.col("nama_produk_paket")], F.col("nama_produk_paket")).alias("nama_paket_standar"),
            method_map[F.col("payment_type")].alias("metode_bayar_standar"),
            F.col("amount").cast("decimal(15,2)").alias("jumlah_bayar"),
            F.coalesce(status_map[F.col("payment_status")], F.lit("Unknown")).alias("status_bayar_standar"),
        ))

    # PintarNusa
    pn_trx = (pn["tagihan_program"]
        .withColumn("_plat", F.lit("PintarNusa"))
        .select(
            F.concat(F.lit("PN-TR-"), F.col("id_tagihan")).alias("id_transaksi"),
            F.lit("PLT-PN").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").alias("id_pengguna_platform"),
            F.col("tanggal_tagihan").cast("date").alias("tanggal_transaksi"),
            F.coalesce(paket_map[F.col("nama_paket")], F.col("nama_paket")).alias("nama_paket_standar"),
            method_map[F.col("metode_bayar")].alias("metode_bayar_standar"),
            F.col("nominal").cast("decimal(15,2)").alias("jumlah_bayar"),
            F.coalesce(status_map[F.col("status_tagihan")], F.lit("Unknown")).alias("status_bayar_standar"),
        ))

    combined = zb_trx.union(rc_trx).union(kj_trx).union(pn_trx)
    
    return (combined
        .join(pu, ["nama_platform", "id_pengguna_platform"], "left")
        .select("id_transaksi", "id_platform", "id_pengguna_global",
            "tanggal_transaksi", "nama_paket_standar",
            "metode_bayar_standar", "jumlah_bayar", 
            "status_bayar_standar")
        .filter(
            (F.col("tanggal_transaksi") <= F.current_date()) & 
            (F.col("jumlah_bayar") >= 0)
        )
        .dropna(subset=["id_transaksi", "id_pengguna_global"])
        .dropDuplicates(["id_transaksi"]))


def build_olah_ulasan(spark, zb, rc, kj, pn, peta_pengguna, peta_kelas):
    """Tabel olah_ulasan: standarisasi ulasan/review."""
    pu = peta_pengguna.select("nama_platform", "id_pengguna_platform", "id_pengguna_global")
    pk = peta_kelas.select("nama_platform", "id_kelas_platform", "id_kelas_global")
    
    # Create device mapping
    device_map = F.create_map([F.lit(x) for kv in cfg.DEVICE_MAPPING.items() for x in kv])

    # ZeniBelajar: Rating 1-10 -> 1-5
    zb_feedback = zb["feedback_program"].withColumn("_plat", F.lit("ZeniBelajar"))
    zb_device_expr = _get_device_standar_expr(zb_feedback, ["device"], device_map)
    zb_rev = (zb_feedback
        .select(
            F.concat(F.lit("ZB-UL-"), F.col("id_feedback")).alias("id_ulasan"),
            F.lit("PLT-ZB").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("kode_member").alias("id_pengguna_platform"),
            F.col("kode_program").alias("id_kelas_platform"),
            F.to_date("tanggal_feedback").alias("tanggal_ulasan"),
            (F.col("skor_rating") / 2).cast("decimal(3,1)").alias("rating_standar_5"),
            F.col("ulasan_text").alias("komentar"),
            zb_device_expr.alias("perangkat_standar"),
        ))

    # RuangCerdas: Rating 1-5
    rc_ulasan = rc["ulasan_kelas"].withColumn("_plat", F.lit("RuangCerdas"))
    rc_device_expr = _get_device_standar_expr(rc_ulasan, ["perangkat", "device"], device_map)
    rc_rev = (rc_ulasan
        .select(
            F.concat(F.lit("RC-UL-"), F.col("id_ulasan")).alias("id_ulasan"),
            F.lit("PLT-RC").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").cast("string").alias("id_pengguna_platform"),
            F.col("id_kelas").cast("string").alias("id_kelas_platform"),
            F.col("tanggal_ulasan"),
            F.col("rating").cast("decimal(3,1)").alias("rating_standar_5"),
            F.col("komentar"),
            rc_device_expr.alias("perangkat_standar"),
        ))

    # KelasJuara: Rating 1-5
    kj_review = kj["review_kelas"].withColumn("_plat", F.lit("KelasJuara"))
    kj_device_expr = _get_device_standar_expr(kj_review, ["platform_device"], device_map)
    kj_rev = (kj_review
        .select(
            F.concat(F.lit("KJ-UL-"), F.col("review_id")).alias("id_ulasan"),
            F.lit("PLT-KJ").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("user_id").alias("id_pengguna_platform"),
            F.col("produk_id").alias("id_kelas_platform"),
            F.col("review_date").alias("tanggal_ulasan"),
            F.col("star_rating").cast("decimal(3,1)").alias("rating_standar_5"),
            F.col("review_text").alias("komentar"),
            kj_device_expr.alias("perangkat_standar"),
        ))

    # PintarNusa: Rating 1-5
    pn_ulasan = pn["ulasan_program"].withColumn("_plat", F.lit("PintarNusa"))
    pn_device_expr = _get_device_standar_expr(pn_ulasan, ["perangkat"], device_map)
    pn_rev = (pn_ulasan
        .select(
            F.concat(F.lit("PN-UL-"), F.col("id_ulasan")).alias("id_ulasan"),
            F.lit("PLT-PN").alias("id_platform"),
            F.col("_plat").alias("nama_platform"),
            F.col("id_siswa").alias("id_pengguna_platform"),
            F.col("id_program").alias("id_kelas_platform"),
            F.col("tanggal_ulasan").cast("date"),
            F.col("rating").cast("decimal(3,1)").alias("rating_standar_5"),
            F.col("isi_ulasan").alias("komentar"),
            pn_device_expr.alias("perangkat_standar"),
        ))

    combined = zb_rev.union(rc_rev).union(kj_rev).union(pn_rev)
    return (combined
        .join(pu, ["nama_platform", "id_pengguna_platform"], "left")
        .join(pk, ["nama_platform", "id_kelas_platform"], "left")
        .select("id_ulasan", "id_platform", "id_pengguna_global",
                "id_kelas_global", "tanggal_ulasan", 
                "rating_standar_5", "komentar", "perangkat_standar")
        .filter(
            (F.col("tanggal_ulasan") <= F.current_date()) & 
            (F.col("rating_standar_5").between(1, 5))
        )
        .dropna(subset=["id_ulasan", "id_pengguna_global", "id_kelas_global"])
        .dropDuplicates(["id_ulasan"]))


def build_olah_pendaftaran(spark, zb, rc, kj, pn, peta_pengguna, peta_kelas):
    """Tabel olah_pendaftaran_program: standarisasi pendaftaran kelas lintas platform."""
    pu = peta_pengguna.select("nama_platform", "id_pengguna_platform", "id_pengguna_global")
    pk = peta_kelas.select("nama_platform", "id_kelas_platform", "id_kelas_global")
    reg_map = F.create_map([F.lit(x) for kv in cfg.REGISTRATION_STATUS_MAPPING.items() for x in kv])

    # ZB
    zb_pendaftaran_id_col = _first_existing_column(zb["peserta_program"], ["id_peserta", "kode_peserta"])
    zb_pendaftaran_date_col = _first_existing_column(zb["peserta_program"], ["tanggal_daftar", "waktu_daftar"])
    zb_pendaftaran_status_col = _first_existing_column(zb["peserta_program"], ["status_peserta", "status_aktif"])

    zb_df = (zb["peserta_program"]
        .withColumn("nama_platform", F.lit("ZeniBelajar"))
        .select(
            F.concat(F.lit("ZB-PD-"), F.col(zb_pendaftaran_id_col)).alias("id_pendaftaran"),
            F.lit("PLT-ZB").alias("id_platform"),
            F.col("nama_platform"),
            F.col("kode_member").alias("id_pengguna_platform"),
            F.col("kode_program").alias("id_kelas_platform"),
            F.to_date(F.col(zb_pendaftaran_date_col)).alias("tanggal_daftar"),
            F.coalesce(reg_map[F.lower(F.trim(F.col(zb_pendaftaran_status_col).cast("string")))], F.lit("aktif")).alias("status_pendaftaran_standar")
        ))

    # KJ
    kj_pendaftaran_id_col = _first_existing_column(kj["pembelian_kelas"], ["pembelian_id"])
    kj_pendaftaran_date_col = _first_existing_column(kj["pembelian_kelas"], ["tanggal_beli", "purchase_date"])
    kj_pendaftaran_status_col = _first_existing_column(kj["pembelian_kelas"], ["status_pembelian", "status"])

    kj_df = (kj["pembelian_kelas"]
        .withColumn("nama_platform", F.lit("KelasJuara"))
        .select(
            F.concat(F.lit("KJ-PD-"), F.col(kj_pendaftaran_id_col)).alias("id_pendaftaran"),
            F.lit("PLT-KJ").alias("id_platform"),
            F.col("nama_platform"),
            F.col("user_id").alias("id_pengguna_platform"),
            F.col("produk_id").alias("id_kelas_platform"),
            F.to_date(F.col(kj_pendaftaran_date_col)).alias("tanggal_daftar"),
            F.coalesce(reg_map[F.lower(F.trim(F.col(kj_pendaftaran_status_col).cast("string")))], F.lit("aktif")).alias("status_pendaftaran_standar")
        ))

    # RC
    rc_pendaftaran_id_col = _first_existing_column(rc["pendaftaran_kelas"], ["id_pendaftaran", "id_daftar"])
    rc_pendaftaran_date_col = _first_existing_column(rc["pendaftaran_kelas"], ["tanggal_daftar", "tanggal_registrasi"])
    rc_pendaftaran_status_col = _first_existing_column(rc["pendaftaran_kelas"], ["status", "status_pendaftaran"])

    rc_df = (rc["pendaftaran_kelas"]
        .withColumn("nama_platform", F.lit("RuangCerdas"))
        .select(
            F.concat(F.lit("RC-PD-"), F.col(rc_pendaftaran_id_col)).alias("id_pendaftaran"),
            F.lit("PLT-RC").alias("id_platform"),
            F.col("nama_platform"),
            F.col("id_siswa").cast("string").alias("id_pengguna_platform"),
            F.col("id_kelas").cast("string").alias("id_kelas_platform"),
            F.to_date(F.col(rc_pendaftaran_date_col)).alias("tanggal_daftar"),
            F.coalesce(reg_map[F.lower(F.trim(F.col(rc_pendaftaran_status_col).cast("string")))], F.lit("aktif")).alias("status_pendaftaran_standar")
        ))

    # PN
    pn_pendaftaran_id_col = _first_existing_column(pn["enrolmen_program"], ["id_enrol", "id_enrolmen"])
    pn_pendaftaran_date_col = _first_existing_column(pn["enrolmen_program"], ["tanggal_enrol", "waktu_enrol"])
    pn_pendaftaran_status_col = _first_existing_column(pn["enrolmen_program"], ["status_aktif", "status"])

    pn_df = (pn["enrolmen_program"]
        .withColumn("nama_platform", F.lit("PintarNusa"))
        .select(
            F.concat(F.lit("PN-PD-"), F.col(pn_pendaftaran_id_col)).alias("id_pendaftaran"),
            F.lit("PLT-PN").alias("id_platform"),
            F.col("nama_platform"),
            F.col("id_siswa").alias("id_pengguna_platform"),
            F.col("id_program").alias("id_kelas_platform"),
            F.to_date(F.col(pn_pendaftaran_date_col)).alias("tanggal_daftar"),
            F.coalesce(reg_map[F.lower(F.trim(F.col(pn_pendaftaran_status_col).cast("string")))], F.lit("aktif")).alias("status_pendaftaran_standar")
        ))

    combined = zb_df.union(kj_df).union(pn_df).union(rc_df)

    return (combined
        .join(pu, ["nama_platform", "id_pengguna_platform"], "left")
        .join(pk, ["nama_platform", "id_kelas_platform"], "left")
        .select("id_pendaftaran", "id_platform", "id_pengguna_global",
            "id_kelas_global", "tanggal_daftar", "status_pendaftaran_standar")
        .dropna(subset=["id_pendaftaran", "id_pengguna_global", "id_kelas_global"])
        .dropDuplicates(["id_pendaftaran"]))




# NOTE: write_staging() telah dipindahkan ke load_staging.py
# untuk pemisahan antara logika Transform dan Load.
