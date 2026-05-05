"""Generate ZeniBelajar (MySQL) — Largest user base, low conversion."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shared_config import *

OUT = os.path.join(BASE_DIR, 'zenibelajar')
MEMBER_N = 400; PROGRAM_N = 30; PESERTA_N = 700; RIWAYAT_N = 1000
TRYOUT_N = 700; ORDER_N = 600; FEEDBACK_N = 500
DB = "zenibelajar"

def gen_programs():
    rows = []
    idx = 1
    for j in JENJANG:
        for m in MAPEL_ID:
            for lvl in ["beginner","intermediate","advanced"]:
                if idx > PROGRAM_N:
                    break
                title = f"{m} {j} - {lvl.capitalize()}"
                rows.append((f"PRG{idx:03d}", title, m, j, lvl, random.choice(MENTORS)))
                idx += 1
            if idx > PROGRAM_N:
                break
        if idx > PROGRAM_N:
            break
    while len(rows) < PROGRAM_N:
        m = random.choice(MAPEL_ID)
        j = random.choice(JENJANG)
        lvl = random.choice(["beginner","intermediate","advanced"])
        rows.append((f"PRG{idx:03d}", f"{m} {j} Ekstra", m, j, lvl, random.choice(MENTORS)))
        idx += 1
    return rows

def gen_members():
    rows = []
    for i in range(1, MEMBER_N+1):
        name = gen_name()
        email = gen_email(name, "zenibelajar.id", i)
        j = random.choice(JENJANG)
        kls = random.choice(KELAS_MAP[j])
        prov = pick_provinsi()
        reg = gen_registration_date()
        rows.append((f"MBR{i:04d}", name, email, j, kls, prov, reg))
    return rows

def gen_peserta(members, programs):
    rows = []
    used = set()
    for i in range(1, PESERTA_N+1):
        mbr = random.choice(members)
        prg = random.choice(programs)
        key = (mbr[0], prg[0])
        attempts = 0
        while key in used and attempts < 20:
            mbr = random.choice(members)
            prg = random.choice(programs)
            key = (mbr[0], prg[0])
            attempts += 1
        used.add(key)
        reg_date = mbr[6]
        daftar = rand_date(reg_date, min(reg_date + timedelta(days=365), DATE_END))
        status = random.choice(["aktif","selesai","dropout"])
        rows.append((f"PST{i:04d}", mbr[0], prg[0], daftar, status))
    return rows

def gen_riwayat(members, programs):
    rows = []
    for i in range(1, RIWAYAT_N+1):
        mbr = random.choice(members)
        prg = random.choice(programs)
        reg_date = mbr[6]
        start_dt = rand_datetime(reg_date, DATE_END)
        durasi = random.randint(300, 5400)  # 5-90 menit dalam detik
        end_dt = start_dt + timedelta(seconds=durasi)
        device = random.choice(["mobile","web"])
        rows.append((f"RWT{i:05d}", mbr[0], prg[0], start_dt, end_dt, durasi, device))
    return rows

def gen_tryout(members, programs):
    rows = []
    for i in range(1, TRYOUT_N+1):
        mbr = random.choice(members)
        prg = random.choice(programs)
        mapel = prg[2]
        reg_date = mbr[6]
        tgl = gen_exam_date(reg_date)
        skor = gen_score_1000(mapel)
        jml_soal = random.choice([20,25,30,40,50])
        rows.append((f"TRY{i:05d}", mbr[0], prg[0], tgl, skor, jml_soal))
    return rows

def gen_orders(members):
    rows = []
    # Ensure every member has at least 1 order
    for i, mbr in enumerate(members, 1):
        reg_date = mbr[6]
        tgl = rand_date(reg_date, min(reg_date + timedelta(days=180), DATE_END))
        # Low conversion: 70% free
        if random.random() < 0.70:
            paket, harga = PAKET_ZENI[0]
        elif random.random() < 0.65:
            paket, harga = PAKET_ZENI[1]
        else:
            paket, harga = PAKET_ZENI[2]
        ch = random.choice(PAYMENT_CHANNELS) if harga > 0 else "gratis"
        status = "paid" if harga == 0 else random.choices(["paid","unpaid","cancelled"], weights=[0.7,0.15,0.15])[0]
        rows.append((f"ORD{i:05d}", mbr[0], paket, tgl, ch, harga, status))
    # Extra orders
    idx = len(rows) + 1
    while len(rows) < ORDER_N:
        mbr = random.choice(members)
        reg_date = mbr[6]
        tgl = rand_date(reg_date, DATE_END)
        r = random.random()
        if r < 0.60:
            paket, harga = PAKET_ZENI[0]
        elif r < 0.85:
            paket, harga = PAKET_ZENI[1]
        else:
            paket, harga = PAKET_ZENI[2]
        ch = random.choice(PAYMENT_CHANNELS) if harga > 0 else "gratis"
        status = "paid" if harga == 0 else random.choices(["paid","unpaid","cancelled"], weights=[0.7,0.15,0.15])[0]
        rows.append((f"ORD{idx:05d}", mbr[0], paket, tgl, ch, harga, status))
        idx += 1
    return rows

def gen_feedback(members, programs):
    rows = []
    for i in range(1, FEEDBACK_N+1):
        mbr = random.choice(members)
        prg = random.choice(programs)
        reg_date = mbr[6]
        tgl = rand_date(reg_date, DATE_END)
        rating = random.choices([1,2,3,4,5,6,7,8,9,10],
                                weights=[3,4,8,10,15,15,15,12,10,8])[0]
        device = random.choice(DEVICES)
        text = gen_review_text_id()
        rows.append((f"FBK{i:05d}", mbr[0], prg[0], tgl, rating, device, text))
    return rows

def write_sql():
    print("=== ZeniBelajar (MySQL) ===")
    programs = gen_programs()
    members = gen_members()
    peserta = gen_peserta(members, programs)
    riwayat = gen_riwayat(members, programs)
    tryout = gen_tryout(members, programs)
    orders = gen_orders(members)
    feedback = gen_feedback(members, programs)

    total = len(members)+len(programs)+len(peserta)+len(riwayat)+len(tryout)+len(orders)+len(feedback)
    print(f"  Total rows: {total}")

    # DDL
    ddl = f"""-- ============================================
-- ZeniBelajar Database (MySQL)
-- Platform EdTech terbesar dengan basis pengguna terbanyak.
-- Strategi akuisisi agresif dengan konten gratis luas.
-- Konversi ke pelanggan berbayar masih rendah (~30%).
-- ============================================

DROP DATABASE IF EXISTS `{DB}`;
CREATE DATABASE `{DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `{DB}`;

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
"""
    with open(os.path.join(OUT, '01_create_db_zenibelajar.sql'), 'w', encoding='utf-8') as f:
        f.write(ddl)
    print(f"  -> 01_create_db_zenibelajar.sql")

    # DML
    lines = [f"USE `{DB}`;\n"]
    # members
    lines.append("-- member")
    for i in range(0, len(members), 50):
        batch = members[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{r[4]},{sql_val(r[5])},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO `member` VALUES\n{vals};\n")
    # programs
    lines.append("-- program_belajar")
    vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])},{sql_val(r[5])})" for r in programs])
    lines.append(f"INSERT INTO `program_belajar` VALUES\n{vals};\n")
    # peserta
    lines.append("-- peserta_program")
    for i in range(0, len(peserta), 50):
        batch = peserta[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])})" for r in batch])
        lines.append(f"INSERT INTO `peserta_program` VALUES\n{vals};\n")
    # riwayat
    lines.append("-- riwayat_belajar")
    for i in range(0, len(riwayat), 50):
        batch = riwayat[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])},{r[5]},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO `riwayat_belajar` VALUES\n{vals};\n")
    # tryout
    lines.append("-- nilai_tryout")
    for i in range(0, len(tryout), 50):
        batch = tryout[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{r[4]},{r[5]})" for r in batch])
        lines.append(f"INSERT INTO `nilai_tryout` VALUES\n{vals};\n")
    # orders
    lines.append("-- order_langganan")
    for i in range(0, len(orders), 50):
        batch = orders[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{sql_val(r[4])},{r[5]},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO `order_langganan` VALUES\n{vals};\n")
    # feedback
    lines.append("-- feedback_program")
    for i in range(0, len(feedback), 50):
        batch = feedback[i:i+50]
        vals = ",\n".join([f"({sql_val(r[0])},{sql_val(r[1])},{sql_val(r[2])},{sql_val(r[3])},{r[4]},{sql_val(r[5])},{sql_val(r[6])})" for r in batch])
        lines.append(f"INSERT INTO `feedback_program` VALUES\n{vals};\n")

    with open(os.path.join(OUT, '02_insert_db_zenibelajar.sql'), 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"  -> 02_insert_db_zenibelajar.sql")

if __name__ == "__main__":
    write_sql()
