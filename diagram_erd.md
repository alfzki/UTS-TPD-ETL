# Diagram ERD Database Staging dan Schema DWH

Dokumen ini mencerminkan struktur tabel yang saat ini didefinisikan di `db_staging_mysql.sql` dan `db_warehouse_mysql.sql`.

## 1. ERD Database Staging (`db_staging_mysql.sql`)

```mermaid
erDiagram
    kamus_platform {
        VARCHAR id_platform PK
        VARCHAR nama_platform
        VARCHAR jenis_platform
        TEXT keterangan
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    kamus_mata_pelajaran {
        INT id_mapel PK
        VARCHAR nama_mapel_standar
        VARCHAR nama_mapel_sumber
        VARCHAR nama_platform
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    peta_pengguna_lintas_platform {
        VARCHAR id_pengguna_global PK
        VARCHAR nama_platform PK
        VARCHAR id_pengguna_platform PK
        VARCHAR email_hash
        VARCHAR email_asli
        VARCHAR nama_standar
        VARCHAR jenjang_standar
        VARCHAR kelas_sekolah_standar
        VARCHAR provinsi_standar
        DATE tanggal_daftar
        DECIMAL confidence_score
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    peta_kelas_lintas_platform {
        VARCHAR id_kelas_global PK
        VARCHAR nama_platform PK
        VARCHAR id_kelas_platform PK
        VARCHAR id_platform
        VARCHAR nama_kelas_standar
        VARCHAR mata_pelajaran_standar
        VARCHAR jenjang_standar
        VARCHAR tingkat_kesulitan_standar
        VARCHAR pengajar
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    olah_aktivitas_belajar {
        VARCHAR id_aktivitas PK
        VARCHAR id_platform
        VARCHAR id_pengguna_global
        VARCHAR id_kelas_global
        DATE tanggal_aktivitas
        DECIMAL durasi_belajar_menit
        VARCHAR perangkat_standar
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    olah_hasil_latihan {
        VARCHAR id_hasil PK
        VARCHAR id_platform
        VARCHAR id_pengguna_global
        VARCHAR id_kelas_global
        DATE tanggal_latihan
        DECIMAL nilai_standar
        VARCHAR status_lulus
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    olah_transaksi {
        VARCHAR id_transaksi PK
        VARCHAR id_platform
        VARCHAR id_pengguna_global
        DATE tanggal_transaksi
        VARCHAR nama_paket_standar
        VARCHAR metode_bayar_standar
        DECIMAL jumlah_bayar
        VARCHAR status_bayar_standar
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    olah_ulasan {
        VARCHAR id_ulasan PK
        VARCHAR id_platform
        VARCHAR id_pengguna_global
        VARCHAR id_kelas_global
        DATE tanggal_ulasan
        DECIMAL rating_standar_5
        TEXT komentar
        VARCHAR perangkat_standar
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }
    olah_pendaftaran_program {
        VARCHAR id_pendaftaran PK
        VARCHAR id_platform
        VARCHAR id_pengguna_global
        VARCHAR id_kelas_global
        DATE tanggal_daftar
        VARCHAR status_pendaftaran_standar
        DATETIME etl_loaded_at
        VARCHAR etl_batch_id
    }

    kamus_platform ||--o{ olah_aktivitas_belajar : "dicatat_di"
    kamus_platform ||--o{ olah_hasil_latihan : "dicatat_di"
    kamus_platform ||--o{ olah_transaksi : "dicatat_di"
    kamus_platform ||--o{ olah_ulasan : "dicatat_di"
    kamus_platform ||--o{ olah_pendaftaran_program : "dicatat_di"

    peta_pengguna_lintas_platform ||--o{ olah_aktivitas_belajar : "melakukan"
    peta_pengguna_lintas_platform ||--o{ olah_hasil_latihan : "mengerjakan"
    peta_pengguna_lintas_platform ||--o{ olah_transaksi : "membayar"
    peta_pengguna_lintas_platform ||--o{ olah_ulasan : "mengirim"
    peta_pengguna_lintas_platform ||--o{ olah_pendaftaran_program : "mendaftar"

    peta_kelas_lintas_platform ||--o{ olah_aktivitas_belajar : "berlangsung_di"
    peta_kelas_lintas_platform ||--o{ olah_hasil_latihan : "terkait_dengan"
    peta_kelas_lintas_platform ||--o{ olah_ulasan : "mendapat"
    peta_kelas_lintas_platform ||--o{ olah_pendaftaran_program : "pada_kelas"
```

## 2. Schema DWH Data Warehouse (`db_warehouse_mysql.sql`)

```mermaid
erDiagram
    dim_waktu {
        BIGINT waktu_key PK
        DATE tanggal
        INT bulan
        VARCHAR nama_bulan
        VARCHAR kuartal
        INT tahun
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_platform {
        BIGINT platform_key PK
        VARCHAR id_platform
        VARCHAR nama_platform
        VARCHAR jenis_platform
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_mata_pelajaran {
        BIGINT mata_pelajaran_key PK
        VARCHAR nama_mata_pelajaran
        VARCHAR kategori
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_jenjang {
        BIGINT jenjang_key PK
        VARCHAR nama_jenjang
        INT tingkat
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_pengguna {
        BIGINT pengguna_key PK
        VARCHAR id_pengguna_sumber
        BIGINT platform_key FK
        VARCHAR nama_pengguna
        VARCHAR email
        VARCHAR provinsi
        DATE tanggal_daftar
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_kelas {
        BIGINT kelas_key PK
        VARCHAR id_kelas_sumber
        BIGINT platform_key FK
        VARCHAR nama_kelas
        BIGINT mata_pelajaran_key FK
        BIGINT jenjang_key FK
        VARCHAR tingkat_kesulitan_standar
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_perangkat {
        BIGINT perangkat_key PK
        VARCHAR kategori_perangkat
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_paket {
        BIGINT paket_key PK
        VARCHAR jenis_paket
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    dim_metode_bayar {
        BIGINT metode_bayar_key PK
        VARCHAR nama_metode
        VARCHAR kategori_metode
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }

    fakta_pendaftaran_program {
        BIGINT pendaftaran_fact_key PK
        BIGINT waktu_key FK
        BIGINT platform_key FK
        BIGINT pengguna_key FK
        BIGINT kelas_key FK
        VARCHAR id_pendaftaran_sumber
        VARCHAR status_pendaftaran
        TINYINT aktif_flag
        TINYINT batal_flag
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    fakta_aktivitas_belajar {
        BIGINT aktivitas_fact_key PK
        BIGINT waktu_key FK
        BIGINT platform_key FK
        BIGINT pengguna_key FK
        BIGINT kelas_key FK
        BIGINT perangkat_key FK
        VARCHAR id_aktivitas_sumber
        INT durasi_menit
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    fakta_hasil_latihan {
        BIGINT hasil_fact_key PK
        BIGINT waktu_key FK
        BIGINT platform_key FK
        BIGINT pengguna_key FK
        BIGINT kelas_key FK
        VARCHAR id_hasil_sumber
        DECIMAL nilai_standar
        VARCHAR status_lulus
        TINYINT lulus_flag
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    fakta_transaksi {
        BIGINT transaksi_fact_key PK
        BIGINT waktu_key FK
        BIGINT platform_key FK
        BIGINT pengguna_key FK
        BIGINT paket_key FK
        BIGINT metode_bayar_key FK
        VARCHAR id_transaksi_sumber
        DECIMAL jumlah_bayar
        VARCHAR status_bayar
        TINYINT berhasil_flag
        TINYINT gagal_flag
        TINYINT pending_flag
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }
    fakta_ulasan {
        BIGINT ulasan_fact_key PK
        BIGINT waktu_key FK
        BIGINT platform_key FK
        BIGINT pengguna_key FK
        BIGINT kelas_key FK
        VARCHAR id_ulasan_sumber
        DECIMAL rating_standar_5
        TINYINT komentar_tersedia_flag
        TIMESTAMP etl_loaded_at
        VARCHAR etl_batch_id
    }

    dim_platform ||--o{ dim_pengguna : "dipakai_oleh"
    dim_platform ||--o{ dim_kelas : "dipakai_oleh"
    dim_mata_pelajaran ||--o{ dim_kelas : "dipakai_oleh"
    dim_jenjang ||--o{ dim_kelas : "dipakai_oleh"

    dim_waktu ||--o{ fakta_pendaftaran_program : "saat"
    dim_platform ||--o{ fakta_pendaftaran_program : "menyediakan"
    dim_pengguna ||--o{ fakta_pendaftaran_program : "mendaftar"
    dim_kelas ||--o{ fakta_pendaftaran_program : "diikuti"

    dim_waktu ||--o{ fakta_aktivitas_belajar : "saat"
    dim_platform ||--o{ fakta_aktivitas_belajar : "menyediakan"
    dim_pengguna ||--o{ fakta_aktivitas_belajar : "melakukan"
    dim_kelas ||--o{ fakta_aktivitas_belajar : "diikuti"
    dim_perangkat ||--o{ fakta_aktivitas_belajar : "menggunakan"

    dim_waktu ||--o{ fakta_hasil_latihan : "saat"
    dim_platform ||--o{ fakta_hasil_latihan : "menyediakan"
    dim_pengguna ||--o{ fakta_hasil_latihan : "mengerjakan"
    dim_kelas ||--o{ fakta_hasil_latihan : "dari_kelas"

    dim_waktu ||--o{ fakta_transaksi : "saat"
    dim_platform ||--o{ fakta_transaksi : "melayani"
    dim_pengguna ||--o{ fakta_transaksi : "membayar"
    dim_paket ||--o{ fakta_transaksi : "membeli"
    dim_metode_bayar ||--o{ fakta_transaksi : "melalui"

    dim_waktu ||--o{ fakta_ulasan : "saat"
    dim_platform ||--o{ fakta_ulasan : "mendapat"
    dim_pengguna ||--o{ fakta_ulasan : "memberikan"
    dim_kelas ||--o{ fakta_ulasan : "ditujukan_pada"
```
