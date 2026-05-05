# Dokumentasi Pipeline ETL Bimbel Integration

Dokumen ini menjelaskan alur ETL yang saat ini diimplementasikan pada folder `etl/`, yaitu:

1. Extract dari 4 sumber data.
2. Transform ke tabel staging.
3. Load staging ke MySQL.
4. Transform staging ke skema warehouse.
5. Load warehouse ke MySQL.

Pipeline dijalankan lewat `main_etl.py` dengan parameter `--step`.

---

## 1. Arsitektur singkat

Pipeline menggunakan **Apache Spark (PySpark)** untuk pemrosesan data dan menyimpan hasil antar-step dalam format **Parquet**. Tujuannya agar setiap tahap bisa dijalankan terpisah tanpa harus mengulang seluruh proses dari awal.

Komponen utamanya:

- `extract.py` — membaca data mentah dari sumber.
- `transform_staging.py` — membentuk tabel staging yang sudah distandardisasi.
- `load_staging.py` — memuat staging ke MySQL staging database.
- `transform_warehouse.py` — membangun dimensi dan fakta warehouse.
- `load_warehouse.py` — memuat star schema ke MySQL warehouse database.
- `main_etl.py` — entry point untuk menjalankan step per step.

---

## 2. Extract

### `extract_zenibelajar(spark)`
Membaca tabel ZeniBelajar dari **MySQL** via JDBC:

- `member`
- `program_belajar`
- `peserta_program`
- `riwayat_belajar`
- `nilai_tryout`
- `order_langganan`
- `feedback_program`

### `extract_ruangcerdas(spark)`
Membaca tabel RuangCerdas dari **PostgreSQL** via JDBC:

- `siswa`
- `kelas`
- `log_video`
- `hasil_kuis`
- `transaksi_paket`
- `ulasan_kelas`

### `extract_kelasjuara(spark)`
Membaca koleksi KelasJuara dari **MongoDB** database `db_kelasjuara`:

- `pengguna`
- `produk_kelas`
- `pembelian_kelas`
- `akses_materi`
- `skor_evaluasi`
- `invoice_pembayaran`
- `review_kelas`

### `extract_pintarnusa(spark)`
Membaca file CSV PintarNusa dari folder sumber data:

- `siswa_pintarnusa.csv`
- `katalog_program.csv`
- `enrolemen.csv`
- `sesi_belajar.csv`
- `hasil_assessment.csv`
- `tagihan_program.csv`
- `ulasan_program.csv`

### Intermediate storage
Semua hasil extract disimpan ke Parquet di bawah `etl/temp_parquet/<platform>/<table>/`.

---

## 3. Transform ke staging

Tahap staging dibangun di `transform_staging.py` dan dipanggil oleh `step_transform_staging`.

### Tabel staging yang dihasilkan

- `kamus_platform`
- `kamus_mata_pelajaran`
- `peta_pengguna_lintas_platform`
- `peta_kelas_lintas_platform`
- `olah_aktivitas_belajar`
- `olah_hasil_latihan`
- `olah_transaksi`
- `olah_ulasan`
- `olah_pendaftaran_program`

### Ringkasan perilaku transform

- `kamus_platform` dibangun dari konstanta platform di `config.py`.
- `kamus_mata_pelajaran` menggabungkan nama mapel dari semua platform dan menstandarkan nama mapel dengan `SUBJECT_MAPPING`.
- `peta_pengguna_lintas_platform` membuat mapping pengguna lintas platform dengan `email_hash` dan `id_pengguna_global` berbasis hash.
- `peta_kelas_lintas_platform` membuat mapping kelas lintas platform dengan `id_kelas_global` berbasis hash.
- Tabel `olah_*` melakukan standarisasi nilai, tipe data, status, dan perangkat.
- `olah_transaksi` saat ini menyimpan status pembayaran terstandar pada kolom `status_bayar_standar`.

Setiap tabel staging juga membawa kolom ETL:

- `etl_loaded_at`
- `etl_batch_id`

Hasil transform staging disimpan lagi ke Parquet pada `etl/temp_parquet/_staging/`.

---

## 4. Load staging

Tahap ini diimplementasikan di `load_staging.py`.

### Strategi load

- **Tabel referensi** (`kamus_*`, `peta_*`) → overwrite.
- **Tabel transaksional** (`olah_*`) → append dengan deduplikasi berdasarkan primary key sumber.

### Tabel referensi staging

- `kamus_platform`
- `kamus_mata_pelajaran`
- `peta_pengguna_lintas_platform`
- `peta_kelas_lintas_platform`

### Tabel transaksional staging

- `olah_pendaftaran_program`
- `olah_aktivitas_belajar`
- `olah_hasil_latihan`
- `olah_transaksi`
- `olah_ulasan`

### Catatan load staging

- Setiap baris ditambah `etl_loaded_at` dan `etl_batch_id` sebelum ditulis.
- Deduplikasi append dilakukan dengan membaca data existing lalu memakai anti-join berdasarkan primary key sumber.

---

## 5. Transform ke warehouse

Tahap warehouse dibangun di `transform_warehouse.py`.

### Dimensi yang dibentuk

- `dim_waktu`
- `dim_platform`
- `dim_pengguna`
- `dim_kelas`
- `dim_mata_pelajaran`
- `dim_jenjang`
- `dim_perangkat`
- `dim_paket`
- `dim_metode_bayar`

### Fakta yang dibentuk

- `fakta_pendaftaran_program`
- `fakta_aktivitas_belajar`
- `fakta_hasil_latihan`
- `fakta_transaksi`
- `fakta_ulasan`

### Perilaku transform warehouse

- `dim_waktu` dibangun dari gabungan tanggal unik pada tabel staging transaksional.
- `dim_platform` berasal dari `kamus_platform`.
- `dim_pengguna` dibangun dari `peta_pengguna_lintas_platform`.
- `dim_kelas` dibangun dari `peta_kelas_lintas_platform` dan bergantung pada `dim_mata_pelajaran` serta `dim_jenjang`.
- `dim_perangkat` dibentuk dari perangkat unik pada aktivitas dan ulasan.
- `dim_paket` menstandarkan paket ke kategori `gratis`, `reguler`, dan `premium`.
- `dim_metode_bayar` menstandarkan metode pembayaran ke kategori metode.

### Catatan penting

- `fakta_aktivitas_belajar` menyimpan `perangkat_key`.
- `fakta_transaksi` menyimpan `status_bayar`.
- Surrogate key warehouse dibuat secara hash-based agar stabil antar-run.

Hasil transform warehouse disimpan kembali ke Parquet pada `etl/temp_parquet/_warehouse/`.

---

## 6. Load warehouse

Tahap ini diimplementasikan di `load_warehouse.py`.

### Strategi load yang saat ini dipakai

- Dimensi dibuat ulang dengan DDL eksplisit agar schema dan constraint MySQL tetap konsisten.
- FK check sementara dimatikan saat drop/recreate tabel dimensi.
- Fakta dimuat dengan strategi anti-join untuk menghindari duplikasi saat append.

### Perilaku detail

- `etl_loaded_at` diisi dengan `current_timestamp()` dari Spark.
- `etl_batch_id` diisi dari `cfg.get_batch_id()`.
- DDL warehouse saat ini mengikuti struktur pada `db_warehouse_mysql.sql`, termasuk:
  - primary key surrogate di tiap dimensi dan fakta,
  - foreign key ke tabel dimensi,
  - `perangkat_key` di `fakta_aktivitas_belajar`.

### Urutan load

1. Load dimensi
2. Load fakta

---

## 7. Entry point `main_etl.py`

Step yang tersedia saat ini:

- `extract_zenibelajar`
- `extract_ruangcerdas`
- `extract_kelasjuara`
- `extract_pintarnusa`
- `transform_staging`
- `load_staging`
- `transform_warehouse`
- `load_warehouse`
- `full`

Mode `full` menjalankan seluruh pipeline secara berurutan.

---

## 8. Ringkasan perubahan penting dari versi sebelumnya

- `extract_zenibelajar` saat ini membaca **MySQL**, bukan CSV.
- `extract_kelasjuara` saat ini membaca **MongoDB**.
- Staging tidak lagi memproduksi `olah_performa_platform_bulanan`.
- Loader warehouse saat ini memakai DDL eksplisit yang mengikuti schema SQL, bukan schema Spark default.
