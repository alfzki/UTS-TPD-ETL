"""
config.py — Konfigurasi koneksi database dan konstanta mapping ETL
=========================================================================
Kredensial dibaca dari .env file untuk keamanan.
"""
import os
from dotenv import load_dotenv

# Load environment variables dari .env file
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

# ──────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..")
PARQUET_DIR = os.path.join(BASE_DIR, "temp_parquet")

# Deteksi Environment (Windows vs WSL/Linux)
# Gunakan 127.0.0.1 karena WSL user saat ini mendukung localhost forwarding (Mirrored Networking)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")

# PintarNusa — CSV files
PINTARNUSA_CSV_DIR = os.path.join(DATA_DIR, "sumber_data_new", "pintarnusa")

# KelasJuara — CSV files
KELASJUARA_CSV_DIR = os.path.join(DATA_DIR, "sumber_data_new", "kelasjuara")

# KelasJuara — MongoDB (collections dibuat dari CSV source)
KELASJUARA_MONGO = {
    "uri": f"mongodb://{os.getenv('MONGO_HOST', '127.0.0.1')}:{os.getenv('MONGO_PORT', '27017')}",
    "database": os.getenv("MONGO_DB", "db_kelasjuara"),
}

# ──────────────────────────────────────────────
# KONEKSI DATABASE (placeholder — sesuaikan environment)
# ──────────────────────────────────────────────

# ZeniBelajar — MySQL (port default 3306)
ZENIBELAJAR_JDBC = {
    "url": f"jdbc:mysql://{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('ZENIBELAJAR_PORT', '3306')}/{os.getenv('ZENIBELAJAR_DB', 'zenibelajar')}",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": os.getenv("ZENIBELAJAR_USER", "root"),
    "password": os.getenv("ZENIBELAJAR_PASSWORD", ""),
}

# RuangCerdas — PostgreSQL (port default 5432)
RUANGCERDAS_JDBC = {
    "url": f"jdbc:postgresql://{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('RUANGCERDAS_PORT', '5432')}/{os.getenv('RUANGCERDAS_DB', 'ruangcerdas')}",
    "driver": "org.postgresql.Driver",
    "user": os.getenv("RUANGCERDAS_USER", "postgres"),
    "password": os.getenv("RUANGCERDAS_PASSWORD", ""),
}


# ──────────────────────────────────────────────
# DATABASE DEFINITIONS
# ──────────────────────────────────────────────

# Staging — MySQL (port default 3306)
STAGING_JDBC = {
    "url": f"jdbc:mysql://{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('STAGING_PORT', '3306')}/{os.getenv('STAGING_DB', 'db_staging_integrasi_bimbel')}",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": os.getenv("STAGING_USER", "root"),
    "password": os.getenv("STAGING_PASSWORD", ""),
}

# Warehouse — MySQL (port default 3306)
WAREHOUSE_JDBC = {
    "url": f"jdbc:mysql://{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('WAREHOUSE_PORT', '3306')}/{os.getenv('WAREHOUSE_DB', 'db_gudang_data_bimbel')}",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": os.getenv("WAREHOUSE_USER", "root"),
    "password": os.getenv("WAREHOUSE_PASSWORD", ""),
}

# ──────────────────────────────────────────────
# SPARK PACKAGES (untuk spark-submit --packages)
# ──────────────────────────────────────────────
SPARK_PACKAGES = [
    # Use Spark-4-compatible connector (Scala 2.13)
    "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1",
    "org.postgresql:postgresql:42.7.3",
    "com.mysql:mysql-connector-j:8.3.0",
]

# ──────────────────────────────────────────────
# MAPPING CONSTANTS
# ──────────────────────────────────────────────

# Standarisasi level kesulitan (ZeniBelajar → standar)
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

# Standarisasi nama mata pelajaran (Inggris → Indonesia)
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
    "Sociology": "Sosiologi",
    "Mathematics (Saintek)": "Matematika",
    "English (General)": "Bahasa Inggris",
}

# Standarisasi status pendaftaran
REGISTRATION_STATUS_MAPPING = {
    "aktif": "aktif",
    "active": "aktif",
    "selesai": "selesai",
    "completed": "selesai",
    "batal": "batal",
    "cancelled": "batal",
    "dropout": "tidak_aktif",
    "nonaktif": "tidak_aktif",
}

# Kategori metode pembayaran
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

# Standarisasi status pembayaran
PAYMENT_STATUS_MAPPING = {
    "paid": "berhasil",
    "berhasil": "berhasil",
    "gagal": "gagal",
    "pending": "pending",
    "success": "berhasil",
    "failed": "gagal",
    "unpaid": "gagal",
    "cancelled": "gagal",
    # PintarNusa
    "lunas": "berhasil",
    "menunggu": "pending",
    "belum": "pending",
    "batal": "gagal",
}

# Standarisasi metode pembayaran
PAYMENT_METHOD_MAPPING = {
    "GoPay": "e-wallet",
    "transfer": "transfer",
    "transfer_bank": "transfer",
    "e-wallet": "e-wallet",
    "kartu": "kartu",
    "kartu_kredit": "kartu",
    "minimarket": "minimarket",
    "QRIS": "e-wallet",
    "gratis": "gratis",
    # PintarNusa
    "virtual_account": "transfer",
    "bank_transfer": "transfer",
    "e_wallet": "e-wallet",
}

# Standarisasi paket langganan
PACKAGE_MAPPING = {
    # ZeniBelajar
    "Gratis": "gratis",
    "Reguler": "reguler",
    "Premium": "premium",
    # RuangCerdas
    "Standard": "reguler",
    "Premium Plus": "premium",
    # KelasJuara
    "Free": "gratis",
    "Pro": "reguler",
    "Elite": "premium",
    # PintarNusa
    "Basic": "reguler",
    "Super": "premium",
}

# Standarisasi perangkat/device
DEVICE_MAPPING = {
    # Desktop/Web variants
    "desktop": "desktop",
    "web": "desktop",
    "browser": "desktop",
    "windows": "desktop",
    "mac": "desktop",
    # Mobile variants
    "mobile": "mobile",
    "smartphone": "mobile",
    "phone": "mobile",
    "android": "mobile",
    "ios": "mobile",
    "iphone": "mobile",
    # Tablet variants
    "tablet": "tablet",
    "ipad": "tablet",
    # Unknown/Default
    "unknown": "tidak diketahui",
    "": "tidak diketahui",
    "null": "tidak diketahui",
}

# Platform identifiers
PLATFORMS = {
    "zenibelajar": {"id": "PLT-ZB", "nama": "ZeniBelajar", "jenis": "MySQL/Web"},
    "ruangcerdas": {"id": "PLT-RC", "nama": "RuangCerdas", "jenis": "PostgreSQL/Web"},
    "kelasjuara":  {"id": "PLT-KJ", "nama": "KelasJuara",  "jenis": "MongoDB/Web"},
    "pintarnusa":  {"id": "PLT-PN", "nama": "PintarNusa",  "jenis": "CSV/Web"},
}

# ──────────────────────────────────────────────
# ETL TRACKING
# ──────────────────────────────────────────────
from datetime import datetime

def get_batch_id():
    """Generate unique batch ID berdasarkan waktu saat ini."""
    return datetime.now().strftime("BATCH-%Y%m%d-%H%M%S")

def get_current_timestamp():
    """Timestamp saat ini untuk kolom etl_loaded_at."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Tabel staging yang bersifat referensi (overwrite aman)
STAGING_REFERENCE_TABLES = [
    "kamus_platform",
    "kamus_mata_pelajaran",
    "peta_pengguna_lintas_platform",
    "peta_kelas_lintas_platform",
]

# Tabel staging transaksional (append + dedup)
STAGING_TRANSACTIONAL_TABLES = {
    "olah_pendaftaran_program": "id_pendaftaran",
    "olah_aktivitas_belajar": "id_aktivitas",
    "olah_hasil_latihan": "id_hasil",
    "olah_transaksi": "id_transaksi",
    "olah_ulasan": "id_ulasan",
}
