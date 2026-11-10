"""
Producer (input generator) untuk pencarian bilangan Armstrong.

Membangkitkan data masukan berupa potongan-potongan rentang angka,
mengirimnya ke broker (RabbitMQ) sebagai pesan task, lalu menunggu
hasil dari worker dan menggabungkannya.

Pemakaian:
    python producer.py [JUMLAH_BAGIAN] [BATAS]
Contoh:
    python producer.py 12 10000000
"""
import sys
import time

from celery import group

from tasks import cek_armstrong

JUMLAH_BAGIAN = int(sys.argv[1]) if len(sys.argv) > 1 else 12
BATAS = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000_000

# Daftar bilangan Armstrong yang sudah diketahui (untuk verifikasi)
DIKETAHUI = [1, 2, 3, 4, 5, 6, 7, 8, 9, 153, 370, 371, 407, 1634, 8208,
             9474, 54748, 92727, 93084, 548834, 1741725, 4210818,
             9800817, 9926315, 24678050, 24678051, 88593477]


def bangkitkan_input(batas, jumlah_bagian):
    """Input generator: memecah rentang 1..batas menjadi beberapa potongan."""
    ukuran = batas // jumlah_bagian
    for i in range(jumlah_bagian):
        awal = i * ukuran + 1
        akhir = batas if i == jumlah_bagian - 1 else (i + 1) * ukuran
        yield awal, akhir


if __name__ == "__main__":
    potongan = list(bangkitkan_input(BATAS, JUMLAH_BAGIAN))

    # 1. Kirim semua tugas sekaligus ke broker sebagai satu group (paralel)
    print(f"Producer mengirim {len(potongan)} tugas ke broker (queue 'armstrong')...")
    for awal, akhir in potongan:
        print(f"  Pesan: cek angka {awal:,} - {akhir:,}")

    waktu_mulai = time.time()
    hasil_group = group(cek_armstrong.s(awal, akhir) for awal, akhir in potongan).apply_async()

    # 2. Tunggu dan kumpulkan hasil dari semua worker
    print("Menunggu hasil dari worker...\n")
    hasil = hasil_group.get()
    waktu = time.time() - waktu_mulai

    # 3. Gabungkan, urutkan, dan tampilkan
    semua = []
    per_worker = {}
    for h in hasil:
        print(f"  {h['worker']:<22} {h['awal']:>11,} - {h['akhir']:>11,} "
              f"{h['durasi']:>6.2f} s  -> {h['ditemukan']}")
        semua.extend(h["ditemukan"])
        per_worker[h["worker"]] = per_worker.get(h["worker"], 0) + 1
    semua.sort()

    print("-" * 55)
    print(f"Rentang angka  : 1 - {BATAS:,}")
    print(f"Ditemukan      : {len(semua)} bilangan Armstrong")
    print(f"Daftar         : {semua}")
    print(f"Jumlah tugas   : {len(potongan)}")
    print(f"Worker aktif   : {len(per_worker)} "
          + ", ".join(f"{w} ({n} tugas)" for w, n in sorted(per_worker.items())))
    print(f"Waktu proses   : {waktu:.2f} detik")

    # 4. Verifikasi dengan daftar yang sudah diketahui
    seharusnya = [x for x in DIKETAHUI if x <= BATAS]
    print(f"Verifikasi     : {'BENAR' if semua == seharusnya else 'SALAH'}")
