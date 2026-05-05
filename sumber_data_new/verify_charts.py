"""Visual verification of all 4 EdTech platform data patterns."""
import csv, os, re
from collections import Counter
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, 'verification_charts')
os.makedirs(OUT, exist_ok=True)

def read_csv(path):
    with open(path,'r',encoding='utf-8') as f: return list(csv.DictReader(f))

def parse_sql_col(sql_path, id_prefix, col_idx_map):
    """Extract columns from SQL INSERT values by ID prefix."""
    rows = []
    with open(sql_path,'r',encoding='utf-8') as f: content = f.read()
    pattern = r"\((" + f"'{id_prefix}" + r"[^)]+)\)"
    for m in re.finditer(pattern, content):
        vals = []
        raw = m.group(1)
        in_q = False; cur = ''
        for c in raw:
            if c == "'" and not in_q: in_q=True; continue
            elif c == "'" and in_q: in_q=False; continue
            elif c == ',' and not in_q: vals.append(cur.strip()); cur=''; continue
            cur += c
        vals.append(cur.strip())
        rows.append(vals)
    return rows

# ============ LOAD DATA ============
print("Loading data...")

# KelasJuara CSV
kj_users = read_csv(os.path.join(BASE,'kelasjuara','pengguna.csv'))
kj_invoice = read_csv(os.path.join(BASE,'kelasjuara','invoice_pembayaran.csv'))
kj_eval = read_csv(os.path.join(BASE,'kelasjuara','skor_evaluasi.csv'))
kj_review = read_csv(os.path.join(BASE,'kelasjuara','review_kelas.csv'))
kj_akses = read_csv(os.path.join(BASE,'kelasjuara','akses_materi.csv'))
kj_produk = read_csv(os.path.join(BASE,'kelasjuara','produk_kelas.csv'))

# PintarNusa CSV
pn_siswa = read_csv(os.path.join(BASE,'pintarnusa','siswa_pintarnusa.csv'))
pn_tagihan = read_csv(os.path.join(BASE,'pintarnusa','tagihan_program.csv'))
pn_assess = read_csv(os.path.join(BASE,'pintarnusa','hasil_assessment.csv'))
pn_ulasan = read_csv(os.path.join(BASE,'pintarnusa','ulasan_program.csv'))
pn_sesi = read_csv(os.path.join(BASE,'pintarnusa','sesi_belajar.csv'))
pn_katalog = read_csv(os.path.join(BASE,'pintarnusa','katalog_program.csv'))

# SQL - extract province and key fields
zb_sql = os.path.join(BASE,'zenibelajar','02_insert_db_zenibelajar.sql')
rc_sql = os.path.join(BASE,'ruangcerdas','02_insert_db_ruangcerdas.sql')

# Parse provinces from SQL
def extract_provs_sql(path, id_pfx):
    provs = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('" + id_pfx + r"\d+','[^']+','[^']+','[^']+',\d+,'([^']+)'", content):
        provs.append(m.group(1))
    return provs

zb_provs = extract_provs_sql(zb_sql, 'MBR')
rc_provs = extract_provs_sql(rc_sql, 'SIS')
kj_provs = [u['asal_provinsi'] for u in kj_users]
pn_provs = [s['provinsi'] for s in pn_siswa]

# Parse scores from SQL (tryout for ZB, kuis for RC)
def extract_scores_sql_zb(path):
    scores = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('TRY\d+','MBR\d+','PRG\d+','[^']+',(\d+),\d+", content):
        scores.append(int(m.group(1)))
    return scores

def extract_scores_sql_rc(path):
    scores = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('KUI\d+','SIS\d+','KLS\d+','[^']+',([\d.]+),\d+", content):
        scores.append(float(m.group(1)))
    return scores

zb_scores = extract_scores_sql_zb(zb_sql)
rc_scores = extract_scores_sql_rc(rc_sql)
kj_scores = [float(e['score_percent']) for e in kj_eval]
pn_scores = [float(a['nilai_akhir']) for a in pn_assess]

# Parse exam dates
def extract_exam_months_sql_zb(path):
    months = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('TRY\d+','MBR\d+','PRG\d+','(\d{4}-(\d{2})-\d{2})'", content):
        months.append(int(m.group(2)))
    return months

def extract_exam_months_sql_rc(path):
    months = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('KUI\d+','SIS\d+','KLS\d+','(\d{4}-(\d{2})-\d{2})'", content):
        months.append(int(m.group(2)))
    return months

zb_exam_m = extract_exam_months_sql_zb(zb_sql)
rc_exam_m = extract_exam_months_sql_rc(rc_sql)
kj_exam_m = [int(e['tanggal_evaluasi'].split('-')[1]) for e in kj_eval]
pn_exam_m = [int(a['tanggal_assessment'].split('-')[1]) for a in pn_assess]

# Parse ratings
def extract_ratings_sql_zb(path):
    rats = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('FBK\d+','MBR\d+','PRG\d+','[^']+',(\d+),'(ios|web|android)'", content):
        rats.append(int(m.group(1)))
    return rats

def extract_ratings_sql_rc(path):
    rats = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('ULS\d+','SIS\d+','KLS\d+','[^']+',(\d+),'(ios|web|android)'", content):
        rats.append(int(m.group(1)))
    return rats

zb_ratings = extract_ratings_sql_zb(zb_sql)
rc_ratings = extract_ratings_sql_rc(rc_sql)
kj_ratings = [int(r['star_rating']) for r in kj_review]
pn_ratings = [int(u['rating']) for u in pn_ulasan]

# Parse packages
def extract_pkgs_sql(path, id_pfx):
    pkgs = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('" + id_pfx + r"\d+','(?:MBR|SIS)\d+','([^']+)'", content):
        pkgs.append(m.group(1))
    return pkgs

zb_pkgs = extract_pkgs_sql(zb_sql, 'ORD')
rc_pkgs = extract_pkgs_sql(rc_sql, 'TRX')
kj_pkgs = [inv['nama_produk_paket'] for inv in kj_invoice]
pn_pkgs = [t['nama_paket'] for t in pn_tagihan]

# Parse reg dates
def extract_reg_months_sql(path, id_pfx):
    months = []
    with open(path,'r',encoding='utf-8') as f: content=f.read()
    for m in re.finditer(r"\('" + id_pfx + r"\d+','[^']+','[^']+','[^']+',\d+,'[^']+','(\d{4})-(\d{2})-\d{2}'", content):
        months.append(f"{m.group(1)}-{m.group(2)}")
    return months

zb_reg = extract_reg_months_sql(zb_sql, 'MBR')
rc_reg = extract_reg_months_sql(rc_sql, 'SIS')
kj_reg = [u['created_date'][:7] for u in kj_users]
pn_reg = [s['tanggal_gabung'][:7] for s in pn_siswa]

plt.style.use('seaborn-v0_8-darkgrid')
colors = ['#2196F3','#4CAF50','#FF9800','#E91E63']
names = ['ZeniBelajar','RuangCerdas','KelasJuara','PintarNusa']

print("Generating charts...")

# ============ CHART 1: Province Distribution ============
fig, axes = plt.subplots(2, 2, figsize=(18, 14))
fig.suptitle('Distribusi Provinsi per Platform', fontsize=16, fontweight='bold')
for ax, provs, name, color in zip(axes.flat, [zb_provs, rc_provs, kj_provs, pn_provs], names, colors):
    c = Counter(provs)
    top = c.most_common(10)
    labs, vals = zip(*top) if top else ([],[])
    bars = ax.barh(range(len(labs)), vals, color=color, alpha=0.8)
    ax.set_yticks(range(len(labs))); ax.set_yticklabels(labs, fontsize=8)
    ax.set_title(f'{name} (n={len(provs)})', fontweight='bold')
    ax.set_xlabel('Jumlah')
    jawa_n = sum(v for k,v in c.items() if 'Jawa' in k or 'Jakarta' in k or 'Banten' in k or 'Yogyakarta' in k)
    pct = jawa_n/len(provs)*100 if provs else 0
    ax.text(0.95, 0.05, f'Jawa: {pct:.1f}%', transform=ax.transAxes, ha='right', fontsize=11, 
            bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
plt.tight_layout()
plt.savefig(os.path.join(OUT, '01_provinsi.png'), dpi=150)
plt.close()
print("  01_provinsi.png")

# ============ CHART 2: Score Distribution ============
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Distribusi Nilai/Skor per Platform', fontsize=16, fontweight='bold')
for ax, scores, name, color in zip(axes.flat, [zb_scores, rc_scores, kj_scores, pn_scores], names, colors):
    if scores:
        ax.hist(scores, bins=30, color=color, alpha=0.7, edgecolor='white')
        ax.axvline(np.mean(scores), color='red', linestyle='--', label=f'Mean={np.mean(scores):.1f}')
        ax.axvline(np.median(scores), color='green', linestyle='--', label=f'Median={np.median(scores):.1f}')
        ax.legend(fontsize=8)
    ax.set_title(f'{name} (n={len(scores)})', fontweight='bold')
    ax.set_xlabel('Skor'); ax.set_ylabel('Frekuensi')
plt.tight_layout()
plt.savefig(os.path.join(OUT, '02_skor.png'), dpi=150)
plt.close()
print("  02_skor.png")

# ============ CHART 3: Exam Month Concentration ============
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Konsentrasi Bulan Ujian (Target: Apr-Jun)', fontsize=16, fontweight='bold')
month_labels = ['Jan','Feb','Mar','Apr','Mei','Jun','Jul','Agu','Sep','Okt','Nov','Des']
for ax, months, name, color in zip(axes.flat, [zb_exam_m, rc_exam_m, kj_exam_m, pn_exam_m], names, colors):
    c = Counter(months)
    vals = [c.get(m,0) for m in range(1,13)]
    bar_colors = [('#FF5722' if m in [4,5,6] else color) for m in range(1,13)]
    ax.bar(range(1,13), vals, color=bar_colors, alpha=0.8, edgecolor='white')
    ax.set_xticks(range(1,13)); ax.set_xticklabels(month_labels, fontsize=8)
    apr_jun = sum(c.get(m,0) for m in [4,5,6])
    total = len(months) if months else 1
    ax.set_title(f'{name} — Apr-Jun: {apr_jun/total*100:.1f}%', fontweight='bold')
    ax.set_ylabel('Jumlah Ujian')
plt.tight_layout()
plt.savefig(os.path.join(OUT, '03_bulan_ujian.png'), dpi=150)
plt.close()
print("  03_bulan_ujian.png")

# ============ CHART 4: Rating Distribution ============
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Distribusi Rating Ulasan per Platform', fontsize=16, fontweight='bold')
for ax, rats, name, color in zip(axes.flat, [zb_ratings, rc_ratings, kj_ratings, pn_ratings], names, colors):
    c = Counter(rats)
    all_vals = sorted(c.keys())
    vals = [c.get(v,0) for v in all_vals]
    ax.bar(all_vals, vals, color=color, alpha=0.8, edgecolor='white')
    if rats:
        ax.axvline(np.mean(rats), color='red', linestyle='--', label=f'Mean={np.mean(rats):.2f}')
        ax.legend()
    ax.set_title(f'{name} (n={len(rats)})', fontweight='bold')
    ax.set_xlabel('Rating'); ax.set_ylabel('Frekuensi')
plt.tight_layout()
plt.savefig(os.path.join(OUT, '04_rating.png'), dpi=150)
plt.close()
print("  04_rating.png")

# ============ CHART 5: Package Distribution ============
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Distribusi Paket Langganan per Platform', fontsize=16, fontweight='bold')
for ax, pkgs, name, color in zip(axes.flat, [zb_pkgs, rc_pkgs, kj_pkgs, pn_pkgs], names, colors):
    c = Counter(pkgs)
    labs = list(c.keys()); vals = list(c.values())
    wedges, texts, autotexts = ax.pie(vals, labels=labs, autopct='%1.1f%%', startangle=90,
                                       colors=plt.cm.Set3(np.linspace(0,1,len(labs))))
    ax.set_title(f'{name} (n={len(pkgs)})', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(OUT, '05_paket.png'), dpi=150)
plt.close()
print("  05_paket.png")

# ============ CHART 6: Registration Timeline ============
fig, axes = plt.subplots(2, 2, figsize=(18, 10))
fig.suptitle('Timeline Registrasi Member (2023-2025)', fontsize=16, fontweight='bold')
for ax, regs, name, color in zip(axes.flat, [zb_reg, rc_reg, kj_reg, pn_reg], names, colors):
    c = Counter(regs)
    sorted_months = sorted(c.keys())
    vals = [c[m] for m in sorted_months]
    ax.fill_between(range(len(sorted_months)), vals, alpha=0.3, color=color)
    ax.plot(range(len(sorted_months)), vals, color=color, linewidth=2)
    step = max(1, len(sorted_months)//12)
    ax.set_xticks(range(0, len(sorted_months), step))
    ax.set_xticklabels([sorted_months[i] for i in range(0, len(sorted_months), step)], rotation=45, fontsize=7)
    ax.set_title(f'{name} (n={len(regs)})', fontweight='bold')
    ax.set_ylabel('Jumlah Registrasi')
plt.tight_layout()
plt.savefig(os.path.join(OUT, '06_registrasi.png'), dpi=150)
plt.close()
print("  06_registrasi.png")

# ============ CHART 7: Cross-Platform Comparison ============
fig, axes = plt.subplots(2, 3, figsize=(20, 10))
fig.suptitle('Perbandingan Antar Platform', fontsize=16, fontweight='bold')

# 7a: Member count
ax = axes[0,0]
counts = [len(zb_provs), len(rc_provs), len(kj_provs), len(pn_provs)]
ax.bar(names, counts, color=colors, alpha=0.8)
ax.set_title('Jumlah Member', fontweight='bold')
for i,v in enumerate(counts): ax.text(i, v+3, str(v), ha='center', fontweight='bold')

# 7b: Avg Score
ax = axes[0,1]
avgs = [np.mean(zb_scores) if zb_scores else 0, np.mean(rc_scores) if rc_scores else 0,
        np.mean(kj_scores), np.mean(pn_scores)]
# Normalize ZB to 100 scale
avgs[0] = avgs[0] / 10
ax.bar(names, avgs, color=colors, alpha=0.8)
ax.set_title('Rata-rata Skor (normalized 0-100)', fontweight='bold')
for i,v in enumerate(avgs): ax.text(i, v+0.5, f'{v:.1f}', ha='center', fontweight='bold')

# 7c: Avg Rating
ax = axes[0,2]
avg_r = [np.mean(zb_ratings) if zb_ratings else 0, np.mean(rc_ratings) if rc_ratings else 0,
         np.mean(kj_ratings), np.mean(pn_ratings)]
ax.bar(names, avg_r, color=colors, alpha=0.8)
ax.set_title('Rata-rata Rating', fontweight='bold')
ax.set_ylim(0, max(10, max(avg_r)+1))
for i,v in enumerate(avg_r): ax.text(i, v+0.1, f'{v:.2f}', ha='center', fontweight='bold')

# 7d: Paid conversion
ax = axes[1,0]
def paid_pct(pkgs, free_labels):
    total = len(pkgs)
    if total == 0: return 0
    paid = sum(1 for p in pkgs if p not in free_labels)
    return paid/total*100
conv = [paid_pct(zb_pkgs,{'Gratis'}), paid_pct(rc_pkgs,{'Gratis'}),
        paid_pct(kj_pkgs,{'Free'}), paid_pct(pn_pkgs,{'Gratis'})]
ax.bar(names, conv, color=colors, alpha=0.8)
ax.set_title('Konversi Berbayar (%)', fontweight='bold')
ax.set_ylim(0, 100)
for i,v in enumerate(conv): ax.text(i, v+1, f'{v:.1f}%', ha='center', fontweight='bold')

# 7e: Exam Apr-Jun %
ax = axes[1,1]
def apr_jun_pct(months):
    if not months: return 0
    return sum(1 for m in months if m in [4,5,6])/len(months)*100
ej = [apr_jun_pct(zb_exam_m), apr_jun_pct(rc_exam_m), apr_jun_pct(kj_exam_m), apr_jun_pct(pn_exam_m)]
ax.bar(names, ej, color=colors, alpha=0.8)
ax.axhline(y=25, color='gray', linestyle=':', label='Baseline (25%)')
ax.set_title('Ujian Apr-Jun (%)', fontweight='bold')
ax.legend()
for i,v in enumerate(ej): ax.text(i, v+0.5, f'{v:.1f}%', ha='center', fontweight='bold')

# 7f: Jawa %
ax = axes[1,2]
def jawa_pct(provs):
    if not provs: return 0
    j = sum(1 for p in provs if 'Jawa' in p or 'Jakarta' in p or 'Banten' in p or 'Yogyakarta' in p)
    return j/len(provs)*100
jp = [jawa_pct(zb_provs), jawa_pct(rc_provs), jawa_pct(kj_provs), jawa_pct(pn_provs)]
ax.bar(names, jp, color=colors, alpha=0.8)
ax.axhline(y=57, color='red', linestyle='--', label='Target ~57%')
ax.set_title('Domisili Jawa (%)', fontweight='bold')
ax.legend()
for i,v in enumerate(jp): ax.text(i, v+0.5, f'{v:.1f}%', ha='center', fontweight='bold')

plt.tight_layout()
plt.savefig(os.path.join(OUT, '07_perbandingan.png'), dpi=150)
plt.close()
print("  07_perbandingan.png")

# ============ CHART 8: Session Duration Comparison ============
fig, ax = plt.subplots(figsize=(10, 6))
fig.suptitle('Durasi Sesi Belajar - PintarNusa vs RuangCerdas', fontsize=14, fontweight='bold')
pn_dur = [int(s['durasi_menit']) for s in pn_sesi]
# RC duration from SQL
rc_dur = []
with open(rc_sql,'r',encoding='utf-8') as f: rc_content=f.read()
for m in re.finditer(r"'LOG\d+','SIS\d+','KLS\d+','[^']+','[^']+',(\d+)", rc_content):
    rc_dur.append(int(m.group(1)))
ax.hist(pn_dur, bins=20, alpha=0.6, color=colors[3], label=f'PintarNusa (avg={np.mean(pn_dur):.1f} min)')
if rc_dur:
    ax.hist(rc_dur, bins=20, alpha=0.6, color=colors[1], label=f'RuangCerdas (avg={np.mean(rc_dur):.1f} min)')
ax.set_xlabel('Durasi (menit)'); ax.set_ylabel('Frekuensi')
ax.legend(fontsize=11)
plt.tight_layout()
plt.savefig(os.path.join(OUT, '08_durasi.png'), dpi=150)
plt.close()
print("  08_durasi.png")

print(f"\nAll charts saved to: {OUT}")
print("Done!")
