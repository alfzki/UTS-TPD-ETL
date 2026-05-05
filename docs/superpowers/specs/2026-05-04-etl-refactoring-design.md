# ETL Refactoring Design: EdTech Data Warehouse Integration

This document outlines the strategy to fix data quality issues (nulls, incorrect defaults, inconsistent mappings) in the ETL pipeline for RuangCerdas, ZeniBelajar, KelasJuara, and PintarNusa.

## 1. Core Principles
- **Direct Source Mapping**: Prioritize original data over hardcoded "Lintas Platform" or "Unknown" values.
- **Strict Standardization**: Use consistent mapping dictionaries for subjects, levels, and statuses.
- **Redundancy Removal**: Remove non-meaningful columns (`sumber_tabel`, `jumlah_*` where always 1, etc.).
- **Platform Integrity**: Ensure `platform_key` correctly identifies the source platform for every record.

## 2. Configuration Updates (`config.py`)
- Standardize `SUBJECT_MAPPING` to include all English-to-Indonesian variations found in `KelasJuara` and `PintarNusa`.
- Update `LEVEL_MAPPING` to cover `beginner`, `intermediate`, `advanced`, `easy`, `medium`, `hard`, `pemula`, `sedang`, `mahir`.
- Define `PAYMENT_METHOD_CATEGORY` for mapping `dim_metode_bayar.kategori_metode`.
- Define `REGISTRATION_STATUS_MAPPING` and `PAYMENT_STATUS_MAPPING` for fact tables.

## 3. Staging Transformation (`transform_staging.py`)
- **dim_pengguna**: Ensure `email_asli` and `tanggal_daftar` are correctly extracted from each source platform's user/student table.
- **dim_perangkat**: Extract actual device info from logs (e.g., `android`, `ios`, `web`) and map to standard categories (`mobile`, `web`).
- **dim_kelas**: Map `tingkat_kesulitan_asli` and standardize to `tingkat_kesulitan_standar` (dasar, menengah, lanjut).
- **fakta_aktivitas_belajar**: Calculate `durasi_menit` accurately based on source units (seconds/hours/minutes).

## 4. Warehouse Transformation (`transform_warehouse.py`)
- **Cleanup**: Remove `sistem_operasi`, `browser` from `dim_perangkat`. Remove `harga`, `durasi_hari` from `dim_paket`. Remove `harga` from `dim_kelas`.
- **Logic Refinement**:
    - `dim_metode_bayar`: Map `nama_metode` to `kategori_metode` using the new config.
    - `fakta_hasil_latihan`: Map `jumlah_soal` and `jumlah_benar` only if they exist in source (avoid default 1).
    - `fakta_ulasan`: Convert ZeniBelajar rating (1-10) to 5-star scale (`rating_asli / 2`).
    - `fakta_transaksi`: Map transaction statuses to `berhasil_flag`, `gagal_flag`, `pending_flag`.

## 5. Verification Plan
- Run `main_etl.py` and inspect sample data in `db_gudang_data_bimbel`.
- Check for NULLs in `email`, `tanggal_daftar`, `kategori_metode`, and `durasi_menit`.
- Verify no "Lintas Platform" strings exist in platform name fields.
- Confirm counts in fact tables are consistent with source data.

---
**Status**: Pending User Review.
