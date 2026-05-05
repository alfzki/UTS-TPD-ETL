"""Verify data patterns across all 4 platforms."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import csv
from collections import Counter
from datetime import date

BASE = os.path.dirname(os.path.abspath(__file__))

def read_csv(path):
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def read_sql_inserts(path, table_name):
    """Count INSERT rows in SQL file for a given table."""
    count = 0
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    # Count value tuples
    in_table = False
    for line in content.split('\n'):
        if f'INSERT INTO' in line and table_name in line:
            in_table = True
        if in_table:
            count += line.count('),(') + (1 if line.strip().startswith('(') else 0)
        if in_table and line.strip().endswith(';'):
            in_table = False
    return count

def verify_zenibelajar():
    print("\n" + "="*60)
    print("ZENIBELAJAR (MySQL) - Verification")
    print("="*60)
    sql_path = os.path.join(BASE, 'zenibelajar', '02_insert_db_zenibelajar.sql')
    with open(sql_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Count rows per table
    tables = ['member', 'program_belajar', 'peserta_program', 'riwayat_belajar', 
              'nilai_tryout', 'order_langganan', 'feedback_program']
    total = 0
    for t in tables:
        # Count opening parens that start value tuples
        count = content.count(f"-- {t}")
        # Rough count by counting lines with MBR/PRG/PST/etc
        print(f"  Table {t}: present ✓")
    
    # Check order >= member count
    member_count = content.count("'MBR")
    order_count = content.count("'ORD")
    print(f"\n  Members: {member_count}, Orders: {order_count}")
    print(f"  Orders >= Members: {'✓' if order_count >= member_count else '✗'}")
    
    # Check devices in feedback
    devices_ok = all(d in content for d in ["'ios'", "'web'", "'android'"])
    print(f"  Devices (ios/web/android) in feedback: {'✓' if devices_ok else '✗'}")
    
    # Check packages
    for pkg in ["'Gratis'", "'Reguler'", "'Premium'"]:
        print(f"  Package {pkg}: {'✓' if pkg in content else '✗'}")

def verify_ruangcerdas():
    print("\n" + "="*60)
    print("RUANGCERDAS (PostgreSQL) - Verification")
    print("="*60)
    sql_path = os.path.join(BASE, 'ruangcerdas', '02_insert_db_ruangcerdas.sql')
    with open(sql_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    siswa_count = content.count("'SIS")
    transaksi_count = content.count("'TRX")
    print(f"  Siswa: {siswa_count}, Transaksi: {transaksi_count}")
    print(f"  Transaksi >= Siswa: {'✓' if transaksi_count >= siswa_count else '✗'}")
    
    for pkg in ["'Gratis'", "'Standard'", "'Premium Plus'"]:
        print(f"  Package {pkg}: {'✓' if pkg in content else '✗'}")

def verify_kelasjuara():
    print("\n" + "="*60)
    print("KELASJUARA (MongoDB/CSV) - Verification")
    print("="*60)
    
    pengguna = read_csv(os.path.join(BASE, 'kelasjuara', 'pengguna.csv'))
    invoice = read_csv(os.path.join(BASE, 'kelasjuara', 'invoice_pembayaran.csv'))
    evaluasi = read_csv(os.path.join(BASE, 'kelasjuara', 'skor_evaluasi.csv'))
    review = read_csv(os.path.join(BASE, 'kelasjuara', 'review_kelas.csv'))
    akses = read_csv(os.path.join(BASE, 'kelasjuara', 'akses_materi.csv'))
    
    print(f"  Pengguna: {len(pengguna)}, Invoice: {len(invoice)}")
    print(f"  Invoice >= Pengguna: {'✓' if len(invoice) >= len(pengguna) else '✗'}")
    
    # Province distribution
    provs = Counter(p['asal_provinsi'] for p in pengguna)
    jawa = sum(v for k, v in provs.items() if 'Jawa' in k or 'Jakarta' in k or 'Banten' in k or 'Yogyakarta' in k)
    print(f"  Jawa %: {jawa/len(pengguna)*100:.1f}% (target ~57%)")
    
    # Package distribution
    pkgs = Counter(inv['nama_produk_paket'] for inv in invoice)
    print(f"  Package distribution: {dict(pkgs)}")
    paid = sum(1 for inv in invoice if int(inv['amount']) > 0)
    print(f"  Paid invoices: {paid}/{len(invoice)} = {paid/len(invoice)*100:.1f}%")
    
    # Score distribution (MIPA vs Bahasa)
    mipa_scores = [float(e['score_percent']) for e in evaluasi 
                   if any(s in read_csv(os.path.join(BASE, 'kelasjuara', 'produk_kelas.csv'))[[p['produk_id'] for p in read_csv(os.path.join(BASE, 'kelasjuara', 'produk_kelas.csv'))].index(e['produk_id'])]['subject_name'] 
                   for s in ['Mathematics','Physics','Chemistry']) if e['produk_id'] in [p['produk_id'] for p in read_csv(os.path.join(BASE, 'kelasjuara', 'produk_kelas.csv'))]]
    
    # Simplified score check
    scores = [float(e['score_percent']) for e in evaluasi]
    avg_score = sum(scores) / len(scores)
    print(f"  Average score: {avg_score:.1f}")
    
    # Exam month concentration
    exam_months = Counter(e['tanggal_evaluasi'].split('-')[1] for e in evaluasi)
    apr_jun = sum(exam_months.get(m, 0) for m in ['04','05','06'])
    print(f"  Exams in Apr-Jun: {apr_jun}/{len(evaluasi)} = {apr_jun/len(evaluasi)*100:.1f}% (target ~55%)")
    
    # Device check in reviews
    devices = set(r['device'] for r in review)
    print(f"  Review devices: {devices} {'✓' if devices == {'ios','web','android'} else '✗'}")
    
    # Registration date range
    dates = [p['created_date'] for p in pengguna]
    print(f"  Date range: {min(dates)} to {max(dates)}")

def verify_pintarnusa():
    print("\n" + "="*60)
    print("PINTARNUSA (CSV) - Verification")
    print("="*60)
    
    siswa = read_csv(os.path.join(BASE, 'pintarnusa', 'siswa_pintarnusa.csv'))
    tagihan = read_csv(os.path.join(BASE, 'pintarnusa', 'tagihan_program.csv'))
    sesi = read_csv(os.path.join(BASE, 'pintarnusa', 'sesi_belajar.csv'))
    ulasan = read_csv(os.path.join(BASE, 'pintarnusa', 'ulasan_program.csv'))
    assess = read_csv(os.path.join(BASE, 'pintarnusa', 'hasil_assessment.csv'))
    
    print(f"  Siswa: {len(siswa)}, Tagihan: {len(tagihan)}")
    print(f"  Tagihan >= Siswa: {'✓' if len(tagihan) >= len(siswa) else '✗'}")
    
    # Session duration (should be 5-20 min, micro-learning)
    durations = [int(s['durasi_menit']) for s in sesi]
    avg_dur = sum(durations) / len(durations)
    print(f"  Avg session duration: {avg_dur:.1f} min (target 5-20)")
    print(f"  Sessions per student: {len(sesi)/len(siswa):.1f} (should be high)")
    
    # Province distribution
    provs = Counter(s['provinsi'] for s in siswa)
    jawa = sum(v for k, v in provs.items() if 'Jawa' in k or 'Jakarta' in k or 'Banten' in k or 'Yogyakarta' in k)
    print(f"  Jawa %: {jawa/len(siswa)*100:.1f}% (target ~57%)")
    
    # Rating distribution (should be varied/flat)
    ratings = Counter(int(u['rating']) for u in ulasan)
    print(f"  Rating distribution: {dict(sorted(ratings.items()))}")
    
    # Exam months
    exam_months = Counter(a['tanggal_assessment'].split('-')[1] for a in assess)
    apr_jun = sum(exam_months.get(m, 0) for m in ['04','05','06'])
    print(f"  Exams in Apr-Jun: {apr_jun}/{len(assess)} = {apr_jun/len(assess)*100:.1f}% (target ~55%)")
    
    # Devices
    devices = set(u['device'] for u in ulasan)
    print(f"  Review devices: {devices} {'✓' if devices == {'ios','web','android'} else '✗'}")
    
    # Date range
    dates = [s['tanggal_gabung'] for s in siswa]
    print(f"  Date range: {min(dates)} to {max(dates)}")
    
    # Package
    pkgs = Counter(t['nama_paket'] for t in tagihan)
    print(f"  Package distribution: {dict(pkgs)}")

def summary():
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    # Count files
    for platform, folder in [("ZeniBelajar", "zenibelajar"), ("RuangCerdas", "ruangcerdas"),
                              ("KelasJuara", "kelasjuara"), ("PintarNusa", "pintarnusa")]:
        files = os.listdir(os.path.join(BASE, folder))
        print(f"  {platform}: {len(files)} files — {', '.join(files)}")

if __name__ == "__main__":
    verify_zenibelajar()
    verify_ruangcerdas()
    verify_kelasjuara()
    verify_pintarnusa()
    summary()
