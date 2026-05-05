-- ============================================
-- RuangCerdas Database (PostgreSQL)
-- Platform dengan kualitas pembelajaran terbaik.
-- Durasi belajar pengguna paling panjang (engagement tinggi).
-- Rating ulasan cenderung tinggi (4-5 dominan).
-- Jumlah pengguna lebih sedikit tapi lebih berkualitas.
-- ============================================

DROP DATABASE IF EXISTS ruangcerdas;
CREATE DATABASE ruangcerdas;
\c ruangcerdas;

CREATE TABLE siswa (
    id_siswa VARCHAR(10) PRIMARY KEY,
    nama_siswa VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    jenjang VARCHAR(20) NOT NULL,
    kelas INT NOT NULL,
    domisili_provinsi VARCHAR(50) NOT NULL,
    tanggal_registrasi DATE NOT NULL
);

CREATE TABLE kelas (
    id_kelas VARCHAR(10) PRIMARY KEY,
    nama_kelas VARCHAR(150) NOT NULL,
    mata_pelajaran VARCHAR(50) NOT NULL,
    jenjang VARCHAR(20) NOT NULL,
    tingkat_kesulitan VARCHAR(20) NOT NULL,
    pengajar VARCHAR(100) NOT NULL
);

CREATE TABLE pendaftaran_kelas (
    id_pendaftaran VARCHAR(10) PRIMARY KEY,
    id_siswa VARCHAR(10) NOT NULL REFERENCES siswa(id_siswa),
    id_kelas VARCHAR(10) NOT NULL REFERENCES kelas(id_kelas),
    tanggal_daftar DATE NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE log_video (
    id_log VARCHAR(12) PRIMARY KEY,
    id_siswa VARCHAR(10) NOT NULL REFERENCES siswa(id_siswa),
    id_kelas VARCHAR(10) NOT NULL REFERENCES kelas(id_kelas),
    waktu_mulai TIMESTAMP NOT NULL,
    waktu_selesai TIMESTAMP NOT NULL,
    durasi_menit INT NOT NULL,
    device VARCHAR(20) NOT NULL
);

CREATE TABLE hasil_kuis (
    id_kuis VARCHAR(12) PRIMARY KEY,
    id_siswa VARCHAR(10) NOT NULL REFERENCES siswa(id_siswa),
    id_kelas VARCHAR(10) NOT NULL REFERENCES kelas(id_kelas),
    tanggal_kuis DATE NOT NULL,
    nilai NUMERIC(5,1) NOT NULL,
    jumlah_soal INT NOT NULL
);

CREATE TABLE transaksi_paket (
    id_transaksi VARCHAR(12) PRIMARY KEY,
    id_siswa VARCHAR(10) NOT NULL REFERENCES siswa(id_siswa),
    nama_paket VARCHAR(30) NOT NULL,
    tanggal_transaksi DATE NOT NULL,
    metode_pembayaran VARCHAR(30) NOT NULL,
    harga INT NOT NULL,
    status_transaksi VARCHAR(20) NOT NULL
);

CREATE TABLE ulasan_kelas (
    id_ulasan VARCHAR(12) PRIMARY KEY,
    id_siswa VARCHAR(10) NOT NULL REFERENCES siswa(id_siswa),
    id_kelas VARCHAR(10) NOT NULL REFERENCES kelas(id_kelas),
    tanggal_ulasan DATE NOT NULL,
    rating INT NOT NULL,
    device VARCHAR(20) NOT NULL,
    komentar TEXT
);
