DROP DATABASE IF EXISTS db_gudang_data_bimbel;

CREATE DATABASE IF NOT EXISTS db_gudang_data_bimbel;

USE db_gudang_data_bimbel;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

CREATE TABLE dim_waktu (
    waktu_key BIGINT PRIMARY KEY,
    tanggal DATE NOT NULL,
    bulan INT,
    nama_bulan VARCHAR(50),
    kuartal VARCHAR(2),
    tahun INT,
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50)
);

CREATE TABLE dim_platform (
    platform_key BIGINT PRIMARY KEY,
    id_platform VARCHAR(50) NOT NULL,
    nama_platform VARCHAR(100),
    jenis_platform VARCHAR(100),
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50)
);

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
);

CREATE TABLE dim_mata_pelajaran (
    mata_pelajaran_key BIGINT PRIMARY KEY,
    nama_mata_pelajaran VARCHAR(100),
    kategori VARCHAR(50),
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50),
    UNIQUE KEY (nama_mata_pelajaran)
);

CREATE TABLE dim_jenjang (
    jenjang_key BIGINT PRIMARY KEY,
    nama_jenjang VARCHAR(50),
    tingkat INT,
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50),
    UNIQUE KEY (nama_jenjang)
);

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
);

CREATE TABLE dim_paket (
    paket_key BIGINT PRIMARY KEY,
    jenis_paket VARCHAR(50) NOT NULL,
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50),
    UNIQUE KEY (jenis_paket)
);

CREATE TABLE dim_metode_bayar (
    metode_bayar_key BIGINT PRIMARY KEY,
    nama_metode VARCHAR(100),
    kategori_metode VARCHAR(50),
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50),
    UNIQUE KEY (nama_metode)
);

CREATE TABLE dim_perangkat (
    perangkat_key BIGINT PRIMARY KEY,
    kategori_perangkat VARCHAR(50),
    etl_loaded_at TIMESTAMP,
    etl_batch_id VARCHAR(50)
);

-- ==========================================
-- FACT TABLES
-- ==========================================

CREATE TABLE fakta_pendaftaran_program (
    pendaftaran_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    waktu_key BIGINT,
    platform_key BIGINT,
    pengguna_key BIGINT,
    kelas_key BIGINT,
    id_pendaftaran_sumber VARCHAR(100),
    status_pendaftaran VARCHAR(50),
    aktif_flag TINYINT,
    batal_flag TINYINT,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(100),
    FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
    FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
    FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
    FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key)
);

CREATE TABLE fakta_aktivitas_belajar (
    aktivitas_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    waktu_key BIGINT,
    platform_key BIGINT,
    pengguna_key BIGINT,
    kelas_key BIGINT,
    perangkat_key BIGINT,
    id_aktivitas_sumber VARCHAR(100),
    durasi_menit INT,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(100),
    FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
    FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
    FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
    FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key),
    FOREIGN KEY (perangkat_key) REFERENCES dim_perangkat (perangkat_key)
);

CREATE TABLE fakta_hasil_latihan (
    hasil_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    waktu_key BIGINT,
    platform_key BIGINT,
    pengguna_key BIGINT,
    kelas_key BIGINT,
    id_hasil_sumber VARCHAR(100),
    nilai_standar DECIMAL(5, 2),
    status_lulus VARCHAR(50),
    lulus_flag TINYINT,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(100),
    FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
    FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
    FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
    FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key)
);

CREATE TABLE fakta_transaksi (
    transaksi_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    waktu_key BIGINT,
    platform_key BIGINT,
    pengguna_key BIGINT,
    paket_key BIGINT,
    metode_bayar_key BIGINT,
    id_transaksi_sumber VARCHAR(100),
    jumlah_bayar DECIMAL(15, 2),
    status_bayar VARCHAR(50),
    berhasil_flag TINYINT,
    gagal_flag TINYINT,
    pending_flag TINYINT,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(100),
    FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
    FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
    FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
    FOREIGN KEY (paket_key) REFERENCES dim_paket (paket_key),
    FOREIGN KEY (metode_bayar_key) REFERENCES dim_metode_bayar (metode_bayar_key)
);

CREATE TABLE fakta_ulasan (
    ulasan_fact_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    waktu_key BIGINT,
    platform_key BIGINT,
    pengguna_key BIGINT,
    kelas_key BIGINT,
    id_ulasan_sumber VARCHAR(100),
    rating_standar_5 DECIMAL(3, 1),
    komentar_tersedia_flag TINYINT,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id VARCHAR(100),
    FOREIGN KEY (waktu_key) REFERENCES dim_waktu (waktu_key),
    FOREIGN KEY (platform_key) REFERENCES dim_platform (platform_key),
    FOREIGN KEY (pengguna_key) REFERENCES dim_pengguna (pengguna_key),
    FOREIGN KEY (kelas_key) REFERENCES dim_kelas (kelas_key)
);