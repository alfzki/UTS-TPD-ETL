"""Generate RuangCerdas (PostgreSQL) — Quality & engagement focused."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shared_config import *

OUT = os.path.join(BASE_DIR, 'ruangcerdas')
SISWA_N = 200; KELAS_N = 25; DAFTAR_N = 500; LOG_N = 1000
KUIS_N = 650; TRANSAKSI_N = 500; ULASAN_N = 450
DB = "ruangcerdas"

def gen_kelas():
    rows = []
    idx = 1
    for j in JENJANG:
        for m in MAPEL_ID:
            if idx > KELAS_N:
                break
            diff = random.choice(["dasar","menengah","lanjut"])
            rows.append((f"KLS{idx:03d}", f"{m} Kelas {j}", m, j, diff, random.choice(MENTORS)))
            idx += 1
        if idx > KELAS_N:
            break
    while len(rows) < KELAS_N:
        m = random.choice(MAPEL_ID)
        j = random.choice(JENJANG)
        diff = random.choice(["dasar","menengah","lanjut"])
        rows.append((f"KLS{idx:03d}", f"{m} {j} Intensif", m, j, diff, random.choice(MENTORS)))
        idx += 1
    return rows

def gen_siswa():
    rows = []
    for i in range(1, SISWA_N+1):
        name = gen_name()
        email = gen_email(name, "ruangcerdas.id", i)
        j = random.choice(JENJANG)
        kls = random.choice(KELAS_MAP[j])
        prov = pick_provinsi()
        reg = gen_registration_date()
        rows.append((f"SIS{i:04d}", name, email, j, kls, prov, reg))
    return rows

def gen_pendaftaran(siswa, kelas):
    rows = []
    used = set()
    for i in range(1, DAFTAR_N+1):
        s = random.choice(siswa)
        k = random.choice(kelas)
        key = (s[0], k[0])
        att = 0
        while key in used and att < 20:
            s = random.choice(siswa)
            k = random.choice(kelas)
            key = (s[0], k[0])
            att += 1
        used.add(key)
        reg_date = s[6]
        daftar = rand_date(reg_date, min(reg_date + timedelta(days=300), DATE_END))
        status = random.choice(["aktif","selesai","nonaktif"])
        rows.append((f"DFT{i:04d}", s[0], k[0], daftar, status))
    return rows

def gen_log_video(siswa, kelas):
    rows = []
    for i in range(1, LOG_N+1):
        s = random.choice(siswa)
        k = random.choice(kelas)
        reg_date = s[6]
        start_dt = rand_datetime(reg_date, DATE_END)
        # Long duration: 30-120 min (high engagement)
        durasi = random.randint(30, 120)
        end_dt = start_dt + timedelta(minutes=durasi)
        device = random.choice(DEVICES)
        rows.append((f"LOG{i:05d}", s[0], k[0], start_dt, end_dt, durasi, device))
    return rows

def gen_kuis(siswa, kelas):
    rows = []
    for i in range(1, KUIS_N+1):
        s = random.choice(siswa)
        k = random.choice(kelas)
        mapel = k[2]
        reg_date = s[6]
        tgl = gen_exam_date(reg_date)
        nilai = gen_score_100(mapel)
        jml = random.choice([10,15,20,25,30])
        rows.append((f"KUI{i:05d}", s[0], k[0], tgl, nilai, jml))
    return rows

def gen_transaksi(siswa):
    rows = []
    # ~50% conversion
    for i, s in enumerate(siswa, 1):
        reg_date = s[6]
        tgl = rand_date(reg_date, min(reg_date + timedelta(days=180), DATE_END))
        if random.random() < 0.50:
            paket, harga = PAKET_RUANG[0]
        elif random.random() < 0.60:
            paket, harga = PAKET_RUANG[1]
        else:
            paket, harga = PAKET_RUANG[2]
        metode = random.choice(PAYMENT_CHANNELS)
        status = "berhasil" if harga == 0 else random.choices(["berhasil","gagal","pending"], weights=[0.8,0.1,0.1])[0]
        rows.append((f"TRX{i:05d}", s[0], paket, tgl, metode, harga, status))
    idx = len(rows) + 1
    while len(rows) < TRANSAKSI_N:
        s = random.choice(siswa)
        reg_date = s[6]
        tgl = rand_date(reg_date, DATE_END)
        r = random.random()
        if r < 0.40:
            paket, harga = PAKET_RUANG[0]
        elif r < 0.70:
            paket, harga = PAKET_RUANG[1]
        else:
            paket, harga = PAKET_RUANG[2]
        metode = random.choice(PAYMENT_CHANNELS)
        status = "berhasil" if harga == 0 else random.choices(["berhasil","gagal","pending"], weights=[0.8,0.1,0.1])[0]
        rows.append((f"TRX{idx:05d}", s[0], paket, tgl, metode, harga, status))
        idx += 1
    return rows

def gen_ulasan(siswa, kelas):
    rows = []
    for i in range(1, ULASAN_N+1):
        s = random.choice(siswa)
        k = random.choice(kelas)
        reg_date = s[6]
        tgl = rand_date(reg_date, DATE_END)
        # High ratings (engagement tinggi)
        rating = random.choices([1,2,3,4,5], weights=[2,3,10,35,50])[0]
        device = random.choice(DEVICES)
        text = gen_review_text_id()
        rows.append((f"ULS{i:05d}", s[0], k[0], tgl, rating, device, text))
    return rows

def write_sql():
    print("=== RuangCerdas (PostgreSQL) ===")
    kelas = gen_kelas()
    siswa = gen_siswa()
    daftar = gen_pendaftaran(siswa, kelas)
    log = gen_log_video(siswa, kelas)
    kuis = gen_kuis(siswa, kelas)
    transaksi = gen_transaksi(siswa)
    ulasan = gen_ulasan(siswa, kelas)

    total = len(siswa)+len(kelas)+len(daftar)+len(log)+len(kuis)+len(transaksi)+len(ulasan)
    print(f"  Total rows: {total}")

    ddl = f"""-- ============================================
-- RuangCerdas Database (PostgreSQL)
-- Platform dengan kualitas pembelajaran terbaik.
-- Durasi belajar pengguna paling panjang (engagement tinggi).
-- Rating ulasan cenderung tinggi (4-5 dominan).
-- Jumlah pengguna lebih sedikit tapi lebih berkualitas.
-- ============================================

DROP DATABASE IF EXISTS {DB};
CREATE DATABASE {DB};
\\c {DB};

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
"""
    with open(os.path.join(OUT, '01_create_db_ruangcerdas.sql'), 'w', encoding='utf-8') as f:
        f.write(ddl)
    print(f"  -> 01_create_db_ruangcerdas.sql")

    # DML
    lines = [f"\\c {DB};\n"]
    # siswa
    lines.append("-- siswa")
    for i in range(0, len(siswa), 50):
        batch = siswa[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{r[4]},{sql_val(r[5])},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO siswa VALUES\n{vals};\n")
    # kelas
    lines.append("-- kelas")
    vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])},{sql_val(r[5])})" for r in kelas])
    lines.append(f"INSERT INTO kelas VALUES\n{vals};\n")
    # pendaftaran_kelas
    lines.append("-- pendaftaran_kelas")
    for i in range(0, len(daftar), 50):
        batch = daftar[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])})" for r in batch])
        lines.append(f"INSERT INTO pendaftaran_kelas VALUES\n{vals};\n")
    # log_video
    lines.append("-- log_video")
    for i in range(0, len(log), 50):
        batch = log[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])},{r[5]},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO log_video VALUES\n{vals};\n")
    # hasil_kuis
    lines.append("-- hasil_kuis")
    for i in range(0, len(kuis), 50):
        batch = kuis[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{r[4]},{r[5]})" for r in batch])
        lines.append(f"INSERT INTO hasil_kuis VALUES\n{vals};\n")
    # transaksi_paket
    lines.append("-- transaksi_paket")
    for i in range(0, len(transaksi), 50):
        batch = transaksi[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])},{r[5]},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO transaksi_paket VALUES\n{vals};\n")
    # ulasan_kelas
    lines.append("-- ulasan_kelas")
    for i in range(0, len(ulasan), 50):
        batch = ulasan[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{r[4]},{sql_val(r[5])},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO ulasan_kelas VALUES\n{vals};\n")

    with open(os.path.join(OUT, '02_insert_db_ruangcerdas.sql'), 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"  -> 02_insert_db_ruangcerdas.sql")

if __name__ == "__main__":
    write_sql()
