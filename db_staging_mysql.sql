DROP DATABASE IF EXISTS db_staging_integrasi_bimbel;

CREATE DATABASE IF NOT EXISTS db_staging_integrasi_bimbel;

USE db_staging_integrasi_bimbel;

-- ==========================================
-- TABEL KAMUS & MAPPING
-- ==========================================

CREATE TABLE kamus_platform (
    id_platform VARCHAR(50) PRIMARY KEY,
    nama_platform VARCHAR(100),
    jenis_platform VARCHAR(100),
    keterangan TEXT,
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(50)
);

CREATE TABLE kamus_mata_pelajaran (
    id_mapel INT AUTO_INCREMENT PRIMARY KEY,
    nama_mapel_standar VARCHAR(100),
    nama_mapel_sumber VARCHAR(100),
    nama_platform VARCHAR(100),
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(50)
);

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
    PRIMARY KEY (
        id_pengguna_global,
        nama_platform,
        id_pengguna_platform
    )
);

CREATE TABLE peta_kelas_lintas_platform (
    id_kelas_global VARCHAR(50),
    nama_platform VARCHAR(100),
    id_kelas_platform VARCHAR(50),
    id_platform VARCHAR(50),
    nama_kelas_standar VARCHAR(150),
    mata_pelajaran_standar VARCHAR(100),
    jenjang_standar VARCHAR(50),
    tingkat_kesulitan_standar VARCHAR(50),
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(50),
    PRIMARY KEY (
        id_kelas_global,
        nama_platform,
        id_kelas_platform
    )
);

-- ==========================================
-- TABEL OLAH (HASIL STANDARDISASI)
-- ==========================================

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
);

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
);

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
);

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
);

CREATE TABLE olah_pendaftaran_program (
    id_pendaftaran VARCHAR(100) PRIMARY KEY,
    id_platform VARCHAR(50),
    id_pengguna_global VARCHAR(50),
    id_kelas_global VARCHAR(50),
    tanggal_daftar DATE,
    status_pendaftaran_standar VARCHAR(50),
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(50)
);