"""
Shared configuration & helpers for generating dummy data across 4 EdTech platforms.
Period: 2023-2025, Domisili: Provinsi, 3 paket (1 gratis), >3000 rows/db.
"""
import random
import numpy as np
import os
import csv
from datetime import date, timedelta, datetime

random.seed(42)
np.random.seed(42)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ======================== NAMA INDONESIA ========================
FIRST_NAMES = [
    "Ahmad","Budi","Citra","Dewi","Eka","Fajar","Gita","Hadi","Indah","Joko",
    "Kartika","Lestari","Muhammad","Nur","Oktavia","Putra","Rina","Sari","Taufik","Umi",
    "Vina","Wati","Yusuf","Zahra","Agus","Bayu","Cahya","Dian","Endang","Fitri",
    "Galih","Hendra","Ika","Jihan","Kurnia","Lina","Mega","Nanda","Putri","Rafi",
    "Sinta","Tika","Umar","Vera","Widya","Yuni","Andi","Bambang","Dina","Eko",
    "Fauzi","Gilang","Hana","Irma","Jasmine","Kiki","Lukman","Mira","Nisa","Pandu",
    "Riska","Siti","Tara","Vivi","Wulan","Yoga","Zara","Arif","Bella","Doni",
    "Elsa","Fahri","Gina","Haris","Intan","Kevin","Laras","Maya","Nova","Omar",
    "Rama","Salsa","Tiara","Ulfah","Rizki","Ayu","Dimas","Fira","Hasan","Ilham",
    "Luki","Maulana","Nabila","Rahma","Syifa","Taufan","Winda","Yuda","Alya","Bagus"
]
LAST_NAMES = [
    "Wijaya","Susanto","Pratama","Handoko","Setiawan","Gunawan","Permana","Kurniawan",
    "Hidayat","Santoso","Nugroho","Wibowo","Rahayu","Saputra","Purnama","Utami",
    "Firmansyah","Maharani","Cahyani","Hakim","Ramadhan","Anggraeni","Fitriani","Kusuma",
    "Wibisono","Hartono","Prasetyo","Suryadi","Wahyuni","Darmawan","Budiman","Ardiansyah",
    "Safitri","Mulyani","Dewanti","Hermawan","Prabowo","Suryana","Yulianto","Nuraini",
    "Iskandar","Sulistyo","Fadilah","Anwar","Rachmawati","Putranto","Adriani","Halim",
    "Tanjung","Siagian"
]
MENTORS = [
    "Dr. Anisa Rahma","Prof. Budi Santoso","Ir. Cahyo Wibowo","Drs. Dini Permata",
    "M.Pd. Eko Prasetyo","Dr. Fitri Handayani","Prof. Guntur Setiawan","Dr. Hesti Kurnia",
    "M.Sc. Irfan Hakim","Dr. Juwita Sari","Prof. Krisna Wijaya","Dr. Laila Nuraini",
    "M.Pd. Mario Budiman","Dr. Nadia Kusuma","Prof. Oscar Hermawan","Dr. Prita Anggraeni",
    "M.Si. Qadri Iskandar","Dr. Ratna Dewi","Prof. Surya Darma","Dr. Tanti Mulyani"
]

# ======================== PROVINSI (distribusi populasi) ========================
PROVINSI_WEIGHTS = {
    # Jawa (~57%)
    "Jawa Barat": 0.155, "Jawa Timur": 0.140, "Jawa Tengah": 0.120,
    "DKI Jakarta": 0.055, "Banten": 0.045, "DI Yogyakarta": 0.015,
    # Sumatera (~20%)
    "Sumatera Utara": 0.055, "Sumatera Selatan": 0.035, "Lampung": 0.035,
    "Riau": 0.030, "Sumatera Barat": 0.025, "Jambi": 0.012, "Aceh": 0.008,
    # Kalimantan (~7%)
    "Kalimantan Timur": 0.025, "Kalimantan Selatan": 0.020, "Kalimantan Barat": 0.015,
    "Kalimantan Tengah": 0.010,
    # Sulawesi (~7%)
    "Sulawesi Selatan": 0.030, "Sulawesi Utara": 0.015, "Sulawesi Tengah": 0.012,
    "Sulawesi Tenggara": 0.008,
    # Lainnya (~9%)
    "Bali": 0.020, "Nusa Tenggara Barat": 0.018, "Nusa Tenggara Timur": 0.015,
    "Papua": 0.012, "Maluku": 0.010, "Papua Barat": 0.008,
    "Maluku Utara": 0.007,
}
PROVINSI_LIST = list(PROVINSI_WEIGHTS.keys())
PROVINSI_PROBS = np.array(list(PROVINSI_WEIGHTS.values()))
PROVINSI_PROBS = PROVINSI_PROBS / PROVINSI_PROBS.sum()  # normalize

# ======================== MATA PELAJARAN ========================
MAPEL_ID = ["Matematika","Fisika","Kimia","Biologi","Bahasa Indonesia","Bahasa Inggris",
            "Ekonomi","Sejarah","Geografi","Sosiologi"]
MAPEL_EN = ["Mathematics","Physics","Chemistry","Biology","Indonesian","English",
            "Economics","History","Geography","Sociology"]
MAPEL_MIPA = {"Matematika","Fisika","Kimia","Mathematics","Physics","Chemistry"}
MAPEL_BAHASA = {"Bahasa Indonesia","Bahasa Inggris","Indonesian","English"}

# ======================== PAKET & HARGA ========================
PAKET_ZENI = [("Gratis",0),("Reguler",149000),("Premium",499000)]
PAKET_RUANG = [("Gratis",0),("Standard",199000),("Premium Plus",599000)]
PAKET_KELAS = [("Free",0),("Pro",249000),("Elite",799000)]
PAKET_PINTAR = [("Gratis",0),("Basic",99000),("Super",349000)]

JENJANG = ["SMP","SMA"]
KELAS_MAP = {"SMP":[7,8,9], "SMA":[10,11,12]}
DEVICES = ["ios","web","android"]
PAYMENT_CHANNELS = ["transfer_bank","e-wallet","kartu_kredit","minimarket","QRIS"]

# ======================== HELPER FUNCTIONS ========================
def rand_date(start, end):
    """Random date between start and end (inclusive)."""
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))

def rand_datetime(start_date, end_date):
    """Random datetime between two dates."""
    d = rand_date(start_date, end_date)
    h = random.randint(6, 22)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return datetime(d.year, d.month, d.day, h, m, s)

DATE_START = date(2023, 1, 1)
DATE_END = date(2025, 12, 31)

def gen_registration_date():
    """Generate registration date within 2023-2025."""
    return rand_date(DATE_START, DATE_END)

def gen_exam_date(after_date):
    """Generate exam/quiz date concentrated in April-June."""
    if random.random() < 0.60:
        # Target April-June: pick a year where Apr-Jun is AFTER after_date
        month = random.choice([4, 5, 6])
        day = random.randint(1, 28)
        # Try years starting from after_date's year
        possible_years = [y for y in [2023, 2024, 2025] if date(y, month, day) >= after_date]
        if possible_years:
            year = random.choice(possible_years)
            return date(year, month, day)
        # Fallback: just pick a date after after_date
        d = after_date + timedelta(days=random.randint(1, 90))
        return min(d, DATE_END)
    else:
        month = random.choice([1,2,3,7,8,9,10,11,12])
        day = random.randint(1, 28)
        possible_years = [y for y in [2023, 2024, 2025] if date(y, month, day) >= after_date]
        if possible_years:
            year = random.choice(possible_years)
            return date(year, month, day)
        d = after_date + timedelta(days=random.randint(1, 90))
        return min(d, DATE_END)

def gen_score_100(mapel):
    """Generate score 0-100 with skewness based on subject."""
    if mapel in MAPEL_MIPA:
        # Right-skewed: banyak nilai rendah
        val = np.random.beta(2, 5) * (100 - 15) + 15
    elif mapel in MAPEL_BAHASA:
        # Left-skewed: banyak nilai tinggi
        val = np.random.beta(5, 2) * (100 - 15) + 15
    else:
        val = np.random.beta(3, 3) * (100 - 15) + 15
    return round(min(100, max(15, val)), 1)

def gen_score_1000(mapel):
    """Generate score 0-1000 with skewness."""
    if mapel in MAPEL_MIPA:
        val = np.random.beta(2, 5) * (1000 - 150) + 150
    elif mapel in MAPEL_BAHASA:
        val = np.random.beta(5, 2) * (1000 - 150) + 150
    else:
        val = np.random.beta(3, 3) * (1000 - 150) + 150
    return round(min(1000, max(150, val)))

def pick_provinsi():
    """Pick a province based on population weights."""
    return np.random.choice(PROVINSI_LIST, p=PROVINSI_PROBS)

def gen_name():
    """Generate a random Indonesian full name."""
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def gen_email(name, platform_suffix, idx):
    """Generate email from name."""
    clean = name.lower().replace(" ", ".").replace("'", "")
    return f"{clean}{idx}@{platform_suffix}"

def sql_val(val):
    """Escape value for SQL INSERT."""
    if val is None:
        return 'NULL'
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, (date, datetime)):
        return f"'{val}'"
    s = str(val).replace("'", "''")
    return f"'{s}'"

def write_csv(filepath, headers, rows):
    """Write rows to CSV file."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  -> {filepath}: {len(rows)} rows")

def gen_review_text_id():
    """Generate random Indonesian review text."""
    texts = [
        "Materi sangat membantu untuk persiapan ujian",
        "Penjelasan guru cukup jelas dan mudah dipahami",
        "Fitur latihan soalnya bagus sekali",
        "Aplikasi kadang lambat tapi kontennya berkualitas",
        "Sangat cocok untuk belajar mandiri di rumah",
        "Video pembelajarannya menarik dan interaktif",
        "Harga langganan sebanding dengan kualitas materi",
        "Perlu ditambah lebih banyak latihan soal",
        "Tampilan aplikasi modern dan user-friendly",
        "Materi sesuai kurikulum terbaru",
        "Guru pengajarnya kompeten dan berpengalaman",
        "Belajar jadi lebih menyenangkan dengan platform ini",
        "Fitur tracking progress sangat membantu",
        "Cocok untuk persiapan UTBK dan ujian nasional",
        "Kualitas video perlu ditingkatkan",
        "Soal-soal latihannya menantang dan variatif",
        "Saya rekomendasikan untuk teman-teman saya",
        "Materi pembahasan soal sangat detail",
        "Customer service responsif dan membantu",
        "Perlu fitur diskusi antar siswa",
        "Konten gratis sudah cukup lengkap",
        "Paket premium worth it untuk persiapan ujian",
        "Loading cepat dan jarang error",
        "Butuh lebih banyak materi untuk kelas 12",
        "Penjelasan step-by-step sangat membantu pemahaman"
    ]
    return random.choice(texts)

def gen_review_text_en():
    """Generate random English review text."""
    texts = [
        "Great content for exam preparation",
        "Teacher explanations are clear and easy to follow",
        "Excellent practice question features",
        "App can be slow sometimes but content quality is good",
        "Perfect for self-study at home",
        "Interactive and engaging video lessons",
        "Subscription price is worth the quality",
        "Need more practice questions",
        "Modern and user-friendly interface",
        "Content follows the latest curriculum",
        "Competent and experienced instructors",
        "Learning becomes more enjoyable with this platform",
        "Progress tracking feature is very helpful",
        "Great for UTBK and national exam prep",
        "Video quality needs improvement",
        "Challenging and varied practice questions",
        "I recommend this to my friends",
        "Detailed solution explanations",
        "Customer service is responsive",
        "Need student discussion features"
    ]
    return random.choice(texts)

if __name__ == "__main__":
    print("Shared config loaded OK.")
    print(f"Provinces: {len(PROVINSI_LIST)}")
    print(f"Names pool: {len(FIRST_NAMES)}x{len(LAST_NAMES)} = {len(FIRST_NAMES)*len(LAST_NAMES)}")
    print(f"Subjects: {MAPEL_ID}")
