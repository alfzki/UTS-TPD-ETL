-- ============================================
-- ZeniBelajar Database (MySQL)
-- Platform EdTech terbesar dengan basis pengguna terbanyak.
-- Strategi akuisisi agresif dengan konten gratis luas.
-- Konversi ke pelanggan berbayar masih rendah (~30%).
-- ============================================

DROP DATABASE IF EXISTS `zenibelajar`;
CREATE DATABASE `zenibelajar` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `zenibelajar`;

CREATE TABLE `member` (
    `kode_member` VARCHAR(10) PRIMARY KEY,
    `nama_lengkap` VARCHAR(100) NOT NULL,
    `alamat_email` VARCHAR(150) NOT NULL,
    `level_sekolah` VARCHAR(20) NOT NULL,
    `tingkat_kelas` INT NOT NULL,
    `domisili` VARCHAR(50) NOT NULL,
    `waktu_registrasi` DATE NOT NULL
) ENGINE=InnoDB;

CREATE TABLE `program_belajar` (
    `kode_program` VARCHAR(10) PRIMARY KEY,
    `judul_program` VARCHAR(150) NOT NULL,
    `mapel` VARCHAR(50) NOT NULL,
    `target_jenjang` VARCHAR(20) NOT NULL,
    `kategori_level` VARCHAR(20) NOT NULL,
    `mentor` VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE `peserta_program` (
    `id_peserta` VARCHAR(10) PRIMARY KEY,
    `kode_member` VARCHAR(10) NOT NULL,
    `kode_program` VARCHAR(10) NOT NULL,
    `tanggal_daftar` DATE NOT NULL,
    `status_peserta` VARCHAR(20) NOT NULL,
    FOREIGN KEY (`kode_member`) REFERENCES `member`(`kode_member`),
    FOREIGN KEY (`kode_program`) REFERENCES `program_belajar`(`kode_program`)
) ENGINE=InnoDB;

CREATE TABLE `riwayat_belajar` (
    `id_riwayat` VARCHAR(12) PRIMARY KEY,
    `kode_member` VARCHAR(10) NOT NULL,
    `kode_program` VARCHAR(10) NOT NULL,
    `waktu_mulai` DATETIME NOT NULL,
    `waktu_selesai` DATETIME NOT NULL,
    `lama_belajar_detik` INT NOT NULL,
    `device` VARCHAR(20) NOT NULL,
    FOREIGN KEY (`kode_member`) REFERENCES `member`(`kode_member`),
    FOREIGN KEY (`kode_program`) REFERENCES `program_belajar`(`kode_program`)
) ENGINE=InnoDB;

CREATE TABLE `nilai_tryout` (
    `id_tryout` VARCHAR(12) PRIMARY KEY,
    `kode_member` VARCHAR(10) NOT NULL,
    `kode_program` VARCHAR(10) NOT NULL,
    `tanggal_submit` DATE NOT NULL,
    `skor` INT NOT NULL,
    `jumlah_soal` INT NOT NULL,
    FOREIGN KEY (`kode_member`) REFERENCES `member`(`kode_member`),
    FOREIGN KEY (`kode_program`) REFERENCES `program_belajar`(`kode_program`)
) ENGINE=InnoDB;

CREATE TABLE `order_langganan` (
    `id_order` VARCHAR(12) PRIMARY KEY,
    `kode_member` VARCHAR(10) NOT NULL,
    `paket` VARCHAR(30) NOT NULL,
    `tanggal_order` DATE NOT NULL,
    `channel_pembayaran` VARCHAR(30) NOT NULL,
    `total_harga` INT NOT NULL,
    `status_order` VARCHAR(20) NOT NULL,
    FOREIGN KEY (`kode_member`) REFERENCES `member`(`kode_member`)
) ENGINE=InnoDB;

CREATE TABLE `feedback_program` (
    `id_feedback` VARCHAR(12) PRIMARY KEY,
    `kode_member` VARCHAR(10) NOT NULL,
    `kode_program` VARCHAR(10) NOT NULL,
    `tanggal_feedback` DATE NOT NULL,
    `skor_rating` INT NOT NULL,
    `device` VARCHAR(20) NOT NULL,
    `ulasan_text` TEXT,
    FOREIGN KEY (`kode_member`) REFERENCES `member`(`kode_member`),
    FOREIGN KEY (`kode_program`) REFERENCES `program_belajar`(`kode_program`)
) ENGINE=InnoDB;
