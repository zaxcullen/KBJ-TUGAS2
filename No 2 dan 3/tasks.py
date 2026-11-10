"""
Task Celery: pengganti worker.py pada program UTS.

Pada versi socket, worker harus membuka koneksi sendiri, membaca
rentang dari socket, lalu mengirim balik hasil dalam bentuk teks.
Pada versi Celery, cukup tulis fungsi biasa dengan dekorator @app.task;
pengambilan tugas dari antrean dan pengiriman hasil diurus Celery.
"""
import time

from celery_app import app


def is_armstrong(n):
    """Logika inti sama persis dengan program UTS."""
    digit = str(n)
    pangkat = len(digit)
    return n == sum(int(d) ** pangkat for d in digit)


@app.task(bind=True, name="tasks.cek_armstrong",
          autoretry_for=(Exception,), retry_kwargs={"max_retries": 3, "countdown": 5})
def cek_armstrong(self, awal, akhir):
    """Cek setiap angka pada rentang [awal, akhir] dan kembalikan yang Armstrong."""
    worker = self.request.hostname
    print(f"[{worker}] Tugas diterima: cek angka {awal:,} - {akhir:,}")

    mulai = time.time()
    ditemukan = []
    for angka in range(awal, akhir + 1):
        if is_armstrong(angka):
            ditemukan.append(angka)
            print(f"[{worker}]   Ketemu: {angka}")

    durasi = time.time() - mulai
    print(f"[{worker}] Selesai dalam {durasi:.2f} detik, {len(ditemukan)} bilangan")

    # Hasil dikembalikan sebagai dict (JSON), bukan string dipisah koma
    return {
        "awal": awal,
        "akhir": akhir,
        "ditemukan": ditemukan,
        "worker": worker,
        "durasi": round(durasi, 2),
    }
