"""Generate KelasJuara (MongoDB/CSV) — Strong monetization, high conversion."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shared_config import *

OUT = os.path.join(BASE_DIR, 'kelasjuara')
PENGGUNA_N = 250; PRODUK_N = 25; BELI_N = 600; AKSES_N = 800
EVAL_N = 600; INVOICE_N = 600; REVIEW_N = 400

def gen_produk():
    rows = []
    idx = 1
    for j in JENJANG:
        for i, m_en in enumerate(MAPEL_EN):
            if idx > PRODUK_N:
                break
            diff = random.choice(["easy","medium","hard"])
            sl = "Junior High" if j == "SMP" else "Senior High"
            rows.append((f"PRD{idx:03d}", f"{m_en} {sl} Course", m_en, sl, diff, random.choice(MENTORS)))
            idx += 1
        if idx > PRODUK_N:
            break
    while len(rows) < PRODUK_N:
        m_en = random.choice(MAPEL_EN)
        sl = random.choice(["Junior High","Senior High"])
        diff = random.choice(["easy","medium","hard"])
        rows.append((f"PRD{idx:03d}", f"{m_en} {sl} Extra", m_en, sl, diff, random.choice(MENTORS)))
        idx += 1
    return rows

def gen_pengguna():
    rows = []
    for i in range(1, PENGGUNA_N+1):
        name = gen_name()
        email = gen_email(name, "kelasjuara.id", i)
        j = random.choice(JENJANG)
        kls = random.choice(KELAS_MAP[j])
        prov = pick_provinsi()
        reg = gen_registration_date()
        rows.append((f"USR{i:04d}", name, email, j, kls, prov, reg))
    return rows

def gen_pembelian(pengguna, produk):
    rows = []
    used = set()
    for i in range(1, BELI_N+1):
        u = random.choice(pengguna)
        p = random.choice(produk)
        key = (u[0], p[0])
        att = 0
        while key in used and att < 20:
            u = random.choice(pengguna)
            p = random.choice(produk)
            key = (u[0], p[0])
            att += 1
        used.add(key)
        reg_date = u[6]
        tgl = rand_date(reg_date, min(reg_date + timedelta(days=300), DATE_END))
        status = random.choice(["completed","active","cancelled"])
        rows.append((f"PBL{i:04d}", u[0], p[0], tgl, status))
    return rows

def gen_akses(pengguna, produk):
    rows = []
    for i in range(1, AKSES_N+1):
        u = random.choice(pengguna)
        p = random.choice(produk)
        reg_date = u[6]
        tgl = rand_date(reg_date, DATE_END)
        # Duration in hours (decimal): 0.5-2.0
        watch = round(random.uniform(0.5, 2.0), 2)
        device = random.choice(DEVICES)
        rows.append((f"AKS{i:05d}", u[0], p[0], tgl, watch, device))
    return rows

def gen_evaluasi(pengguna, produk):
    rows = []
    for i in range(1, EVAL_N+1):
        u = random.choice(pengguna)
        p = random.choice(produk)
        mapel = p[2]  # subject_name (English)
        reg_date = u[6]
        tgl = gen_exam_date(reg_date)
        score = gen_score_100(mapel)
        status_lulus = "lulus" if score >= 60 else "tidak lulus"
        rows.append((f"EVL{i:05d}", u[0], p[0], tgl, score, status_lulus))
    return rows

def gen_invoice(pengguna):
    rows = []
    # High conversion ~70%
    for i, u in enumerate(pengguna, 1):
        reg_date = u[6]
        tgl = rand_date(reg_date, min(reg_date + timedelta(days=180), DATE_END))
        r = random.random()
        if r < 0.30:
            paket, harga = PAKET_KELAS[0]
        elif r < 0.65:
            paket, harga = PAKET_KELAS[1]
        else:
            paket, harga = PAKET_KELAS[2]
        pay_type = random.choice(PAYMENT_CHANNELS)
        status = "success" if harga == 0 else random.choices(["success","failed","pending"], weights=[0.85,0.08,0.07])[0]
        rows.append((f"INV{i:05d}", u[0], paket, tgl, pay_type, harga, status))
    idx = len(rows) + 1
    while len(rows) < INVOICE_N:
        u = random.choice(pengguna)
        reg_date = u[6]
        tgl = rand_date(reg_date, DATE_END)
        r = random.random()
        if r < 0.25:
            paket, harga = PAKET_KELAS[0]
        elif r < 0.60:
            paket, harga = PAKET_KELAS[1]
        else:
            paket, harga = PAKET_KELAS[2]
        pay_type = random.choice(PAYMENT_CHANNELS)
        status = "success" if harga == 0 else random.choices(["success","failed","pending"], weights=[0.85,0.08,0.07])[0]
        rows.append((f"INV{idx:05d}", u[0], paket, tgl, pay_type, harga, status))
        idx += 1
    return rows

def gen_review(pengguna, produk):
    rows = []
    for i in range(1, REVIEW_N+1):
        u = random.choice(pengguna)
        p = random.choice(produk)
        reg_date = u[6]
        tgl = rand_date(reg_date, DATE_END)
        rating = random.choices([1,2,3,4,5], weights=[3,5,15,40,37])[0]
        device = random.choice(DEVICES)
        text = gen_review_text_en()
        rows.append((f"RVW{i:05d}", u[0], p[0], tgl, rating, device, text))
    return rows

def write_csv_files():
    print("=== KelasJuara (MongoDB/CSV) ===")
    produk = gen_produk()
    pengguna = gen_pengguna()
    beli = gen_pembelian(pengguna, produk)
    akses = gen_akses(pengguna, produk)
    evaluasi = gen_evaluasi(pengguna, produk)
    invoice = gen_invoice(pengguna)
    review = gen_review(pengguna, produk)

    total = len(pengguna)+len(produk)+len(beli)+len(akses)+len(evaluasi)+len(invoice)+len(review)
    print(f"  Total rows: {total}")

    write_csv(os.path.join(OUT, 'pengguna.csv'),
              ['user_id','nama','email_user','jenjang_pendidikan','kelas','asal_provinsi','created_date'],
              pengguna)
    write_csv(os.path.join(OUT, 'produk_kelas.csv'),
              ['produk_id','nama_produk','subject_name','school_level','difficulty','tutor_name'],
              produk)
    write_csv(os.path.join(OUT, 'pembelian_kelas.csv'),
              ['pembelian_id','user_id','produk_id','tanggal_beli','status_pembelian'],
              beli)
    write_csv(os.path.join(OUT, 'akses_materi.csv'),
              ['akses_id','user_id','produk_id','tanggal','watch_time','platform_device'],
              akses)
    write_csv(os.path.join(OUT, 'skor_evaluasi.csv'),
              ['evaluasi_id','user_id','produk_id','tanggal_evaluasi','score_percent','status_lulus'],
              evaluasi)
    write_csv(os.path.join(OUT, 'invoice_pembayaran.csv'),
              ['invoice_id','user_id','nama_produk_paket','tanggal_invoice','payment_type','amount','payment_status'],
              invoice)
    write_csv(os.path.join(OUT, 'review_kelas.csv'),
              ['review_id','user_id','produk_id','review_date','star_rating','device','review_text'],
              review)

if __name__ == "__main__":
    write_csv_files()
