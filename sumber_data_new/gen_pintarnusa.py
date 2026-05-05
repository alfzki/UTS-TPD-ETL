"""Generate PintarNusa (CSV) — High frequency, short sessions, micro-learning."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shared_config import *

OUT = os.path.join(BASE_DIR, 'pintarnusa')
SISWA_N = 300; KATALOG_N = 30; ENROL_N = 700; SESI_N = 1200
ASSESS_N = 600; TAGIHAN_N = 500; ULASAN_N = 400

def gen_katalog():
    rows = []
    idx = 1
    for j in JENJANG:
        for m in MAPEL_ID:
            if idx > KATALOG_N:
                break
            lvl = random.choice(["pemula","menengah","mahir"])
            rows.append((f"PGM{idx:03d}", f"Program {m} {j}", m, j, lvl, random.choice(MENTORS)))
            idx += 1
        if idx > KATALOG_N:
            break
    while len(rows) < KATALOG_N:
        m = random.choice(MAPEL_ID)
        j = random.choice(JENJANG)
        lvl = random.choice(["pemula","menengah","mahir"])
        rows.append((f"PGM{idx:03d}", f"Program {m} {j} Kilat", m, j, lvl, random.choice(MENTORS)))
        idx += 1
    return rows

def gen_siswa():
    rows = []
    for i in range(1, SISWA_N+1):
        name = gen_name()
        email = gen_email(name, "pintarnusa.id", i)
        j = random.choice(JENJANG)
        kls = random.choice(KELAS_MAP[j])
        prov = pick_provinsi()
        reg = gen_registration_date()
        rows.append((f"SPN{i:04d}", name, email, j, kls, prov, reg))
    return rows

def gen_enrolemen(siswa, katalog):
    rows = []
    used = set()
    for i in range(1, ENROL_N+1):
        s = random.choice(siswa)
        k = random.choice(katalog)
        key = (s[0], k[0])
        att = 0
        while key in used and att < 20:
            s = random.choice(siswa)
            k = random.choice(katalog)
            key = (s[0], k[0])
            att += 1
        used.add(key)
        reg_date = s[6]
        tgl = rand_date(reg_date, min(reg_date + timedelta(days=300), DATE_END))
        status = random.choice(["aktif","nonaktif","selesai"])
        rows.append((f"ENR{i:04d}", s[0], k[0], tgl, status))
    return rows

def gen_sesi(siswa, katalog):
    rows = []
    for i in range(1, SESI_N+1):
        s = random.choice(siswa)
        k = random.choice(katalog)
        reg_date = s[6]
        tgl = rand_date(reg_date, DATE_END)
        # Short duration: 5-20 min (micro-learning)
        durasi = random.randint(5, 20)
        device = random.choice(DEVICES)
        rows.append((f"SES{i:05d}", s[0], k[0], tgl, durasi, device))
    return rows

def gen_assessment(siswa, katalog):
    rows = []
    for i in range(1, ASSESS_N+1):
        s = random.choice(siswa)
        k = random.choice(katalog)
        mapel = k[2]
        reg_date = s[6]
        tgl = gen_exam_date(reg_date)
        nilai = gen_score_100(mapel)
        total_soal = random.choice([10,15,20,25])
        soal_benar = min(total_soal, max(0, round(total_soal * nilai / 100)))
        rows.append((f"ASM{i:05d}", s[0], k[0], tgl, nilai, total_soal, soal_benar))
    return rows

def gen_tagihan(siswa):
    rows = []
    # ~40% conversion (beginner-friendly, moderate)
    for i, s in enumerate(siswa, 1):
        reg_date = s[6]
        tgl = rand_date(reg_date, min(reg_date + timedelta(days=180), DATE_END))
        r = random.random()
        if r < 0.60:
            paket, harga = PAKET_PINTAR[0]
        elif r < 0.85:
            paket, harga = PAKET_PINTAR[1]
        else:
            paket, harga = PAKET_PINTAR[2]
        metode = random.choice(PAYMENT_CHANNELS)
        status = "lunas" if harga == 0 else random.choices(["lunas","belum","batal"], weights=[0.7,0.2,0.1])[0]
        rows.append((f"TGH{i:05d}", s[0], paket, tgl, metode, harga, status))
    idx = len(rows) + 1
    while len(rows) < TAGIHAN_N:
        s = random.choice(siswa)
        reg_date = s[6]
        tgl = rand_date(reg_date, DATE_END)
        r = random.random()
        if r < 0.55:
            paket, harga = PAKET_PINTAR[0]
        elif r < 0.82:
            paket, harga = PAKET_PINTAR[1]
        else:
            paket, harga = PAKET_PINTAR[2]
        metode = random.choice(PAYMENT_CHANNELS)
        status = "lunas" if harga == 0 else random.choices(["lunas","belum","batal"], weights=[0.7,0.2,0.1])[0]
        rows.append((f"TGH{idx:05d}", s[0], paket, tgl, metode, harga, status))
        idx += 1
    return rows

def gen_ulasan(siswa, katalog):
    rows = []
    for i in range(1, ULASAN_N+1):
        s = random.choice(siswa)
        k = random.choice(katalog)
        reg_date = s[6]
        tgl = rand_date(reg_date, DATE_END)
        # Varied ratings (flat distribution)
        rating = random.choices([1,2,3,4,5], weights=[15,18,25,22,20])[0]
        device = random.choice(DEVICES)
        text = gen_review_text_id()
        rows.append((f"ULP{i:05d}", s[0], k[0], tgl, rating, device, text))
    return rows

def write_csv_files():
    print("=== PintarNusa (CSV) ===")
    katalog = gen_katalog()
    siswa = gen_siswa()
    enrol = gen_enrolemen(siswa, katalog)
    sesi = gen_sesi(siswa, katalog)
    assess = gen_assessment(siswa, katalog)
    tagihan = gen_tagihan(siswa)
    ulasan = gen_ulasan(siswa, katalog)

    total = len(siswa)+len(katalog)+len(enrol)+len(sesi)+len(assess)+len(tagihan)+len(ulasan)
    print(f"  Total rows: {total}")

    write_csv(os.path.join(OUT, 'siswa_pintarnusa.csv'),
              ['id_siswa','nama_siswa','email','tingkat_pendidikan','kelas_tingkat','provinsi','tanggal_gabung'],
              siswa)
    write_csv(os.path.join(OUT, 'katalog_program.csv'),
              ['id_program','nama_program','bidang_studi','jenjang','level_kesulitan','pengajar'],
              katalog)
    write_csv(os.path.join(OUT, 'enrolemen.csv'),
              ['id_enrol','id_siswa','id_program','tanggal_enrol','status_aktif'],
              enrol)
    write_csv(os.path.join(OUT, 'sesi_belajar.csv'),
              ['id_sesi','id_siswa','id_program','tanggal_sesi','durasi_menit','perangkat'],
              sesi)
    write_csv(os.path.join(OUT, 'hasil_assessment.csv'),
              ['id_assessment','id_siswa','id_program','tanggal_assessment','nilai_akhir','total_soal','soal_benar'],
              assess)
    write_csv(os.path.join(OUT, 'tagihan_program.csv'),
              ['id_tagihan','id_siswa','nama_paket','tanggal_tagihan','metode_bayar','nominal','status_tagihan'],
              tagihan)
    write_csv(os.path.join(OUT, 'ulasan_program.csv'),
              ['id_ulasan','id_siswa','id_program','tanggal_ulasan','rating','device','isi_ulasan'],
              ulasan)

if __name__ == "__main__":
    write_csv_files()
