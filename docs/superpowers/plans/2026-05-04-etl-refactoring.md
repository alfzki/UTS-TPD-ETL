# ETL Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix data quality issues (nulls, incorrect defaults, inconsistent mappings) in the ETL pipeline for RuangCerdas, ZeniBelajar, KelasJuara, and PintarNusa.

**Architecture:** Refactor `config.py` for comprehensive mappings, update `transform_staging.py` for direct source mapping, and clean up `transform_warehouse.py` to remove non-meaningful columns and align with target schema.

**Tech Stack:** Python, PySpark, MySQL.

---

### Task 1: Update Configuration Mappings

**Files:**
- Modify: `etl/config.py`

- [ ] **Step 1: Update `SUBJECT_MAPPING` and `LEVEL_MAPPING`**

Add comprehensive mappings to handle English terms and all variations of difficulty levels.

```python
# SUBJECT_MAPPING update
SUBJECT_MAPPING = {
    "Mathematics": "Matematika",
    "Physics": "Fisika",
    "Chemistry": "Kimia",
    "Biology": "Biologi",
    "English": "Bahasa Inggris",
    "Indonesian": "Bahasa Indonesia",
    "Economics": "Ekonomi",
    "History": "Sejarah",
    "Geography": "Geografi",
    "Sociology": "Sejarah", # Fix: Sociology -> Sosiologi
    "Mathematics (Saintek)": "Matematika",
    "English (General)": "Bahasa Inggris",
}

# LEVEL_MAPPING update
LEVEL_MAPPING = {
    "beginner": "dasar",
    "intermediate": "menengah",
    "advanced": "lanjut",
    "easy": "dasar",
    "medium": "menengah",
    "hard": "lanjut",
    "pemula": "dasar",
    "sedang": "menengah",
    "mahir": "lanjut",
    "dasar": "dasar",
    "menengah": "menengah",
    "lanjut": "lanjut",
}
```

- [ ] **Step 2: Add `PAYMENT_METHOD_CATEGORY`**

```python
PAYMENT_METHOD_CATEGORY = {
    "e-wallet": "pembayaran digital",
    "QRIS": "pembayaran digital",
    "GoPay": "pembayaran digital",
    "OVO": "pembayaran digital",
    "ShopeePay": "pembayaran digital",
    "transfer_bank": "transfer",
    "bank_transfer": "transfer",
    "virtual_account": "transfer",
    "kartu_kredit": "kartu",
    "credit_card": "kartu",
    "minimarket": "retail",
    "alfamart": "retail",
    "indomaret": "retail",
    "gratis": "gratis",
    "free": "gratis",
}
```

- [ ] **Step 3: Commit changes**

```bash
git add etl/config.py
git commit -m "refactor: update subject, level, and payment mappings in config"
```

---

### Task 2: Refactor `dim_pengguna` in `transform_staging.py`

**Files:**
- Modify: `etl/transform_staging.py`

- [ ] **Step 1: Update `build_peta_pengguna` to prioritize email and registration date**

Ensure `email_asli` and `tanggal_daftar` are correctly mapped from source and `nama_platform` is NOT hardcoded as "Lintas Platform" (it already uses source name, but verify logic).

- [ ] **Step 2: Commit changes**

```bash
git add etl/transform_staging.py
git commit -m "fix: ensure dim_pengguna uses real email and registration date"
```

---

### Task 3: Refactor `dim_perangkat` and `dim_paket` in `transform_warehouse.py`

**Files:**
- Modify: `etl/transform_warehouse.py`

- [ ] **Step 1: Update `build_dim_perangkat`**

Remove `sistem_operasi` and `browser`. Ensure `kategori_perangkat` is mapped from source logs.

- [ ] **Step 2: Update `build_dim_paket`**

Remove `durasi_hari` and `harga` from `dim_paket`.

- [ ] **Step 3: Update `build_dim_kelas`**

Remove `harga` from `dim_kelas`.

- [ ] **Step 4: Commit changes**

```bash
git add etl/transform_warehouse.py
git commit -m "refactor: clean up dim_perangkat, dim_paket, and dim_kelas"
```

---

### Task 4: Refactor Fact Tables in `transform_warehouse.py`

**Files:**
- Modify: `etl/transform_warehouse.py`

- [ ] **Step 1: Remove `sumber_tabel` and `jumlah_*` columns**

Apply this to `fakta_pendaftaran_program`, `fakta_aktivitas_belajar`, `fakta_hasil_latihan`, `fakta_transaksi`, and `fakta_ulasan`.

- [ ] **Step 2: Fix `fakta_hasil_latihan` columns**

Ensure `jumlah_soal` and `jumlah_benar` are only populated if source has them.

- [ ] **Step 3: Fix `fakta_ulasan` rating scaling**

Implement `rating_standar_5 = rating_asli / 2` for ZeniBelajar.

- [ ] **Step 4: Fix `fakta_transaksi` status flags**

Map `status_bayar_standar` to `berhasil_flag`, `gagal_flag`, and `pending_flag`.

- [ ] **Step 5: Commit changes**

```bash
git add etl/transform_warehouse.py
git commit -m "refactor: clean up and fix fact tables"
```

---

### Task 5: Final Validation

**Files:**
- Run: `etl/main_etl.py`

- [ ] **Step 1: Run full ETL pipeline**

```bash
python etl/main_etl.py
```

- [ ] **Step 2: Verify data in MySQL Warehouse**

Check `db_gudang_data_bimbel` for expected improvements.

- [ ] **Step 3: Final commit and cleanup**

```bash
git commit -m "chore: complete ETL refactoring and verification"
```
