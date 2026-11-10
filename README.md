# KBJ-TUGAS2 – Job Queue dengan Celery

Tugas 2 mata kuliah **Komputasi Berbasis Jaringan**, Program Studi S2 Informatika, Universitas Negeri Surabaya (2026).
Dosen pengampu: Pradini Puspitaningayu dan Ibnu Febry Kurniawan, S.Kom., M.Sc., Ph.D.

**Kelompok**
- Achmad Zakariyah (NIM 26051905010)
- [Nama Anggota 2] (NIM [NIM Anggota 2])

## Ringkasan

Repositori ini berisi kode Tugas 2 yang memakai Celery sebagai job queue, RabbitMQ sebagai message broker, dan Flower untuk pemantauan. Program pencarian bilangan Armstrong pada rentang 1 sampai 10.000.000 dari UTS (versi socket TCP) dikembangkan menjadi versi Celery.

| Folder | Isi |
|---|---|
| `No 1` | Uji penanganan pesan: **Fan-Out**, **Topic**, dan **Wildcard** |
| `No 2 dan 3` | Program UTS (bilangan Armstrong) dengan Celery (no. 2) dan producer sebagai input generator (no. 3) |

Laporan lengkap (pembahasan, hasil, dan screenshot) ada pada berkas laporan Word yang dikumpulkan bersama tugas ini.

## Persiapan

Kebutuhan: Python 3.10+, broker RabbitMQ (CloudAMQP atau RabbitMQ lokal), dan opsional Docker.

```powershell
python -m venv myenv
.\myenv\Scripts\Activate.ps1
pip install -r requirements.txt     # celery, flower
```

Alamat broker dibaca dari berkas `.env` di dalam folder yang dijalankan:

```
CELERY_BROKER_URL=amqps://USER:PASSWORD@HOST.rmq.cloudamqp.com/VHOST
```

- Untuk CloudAMQP, isi dengan **AMQP URL** dari dashboard instance.
- Untuk RabbitMQ lokal, isi dengan `amqp://guest:guest@localhost:5672//`.
- Berkas `.env` berisi sandi dan **tidak boleh di-commit** (sudah ada di `.gitignore`).

Paket gratis CloudAMQP (Little Lemur) dibatasi 20 koneksi bersamaan, jadi jalankan worker per skenario dan tutup worker skenario sebelumnya.

## No 1 – Fan-Out, Topic, dan Wildcard

Rancangan exchange dan queue:

| Exchange | Jenis | Queue (binding) |
|---|---|---|
| `armstrong.fanout` | fanout | `fanout_a`, `fanout_b`, `fanout_c` |
| `armstrong.topic` | topic | `q_kecil` (`armstrong.kecil`), `q_besar` (`armstrong.besar`), `q_semua` (`armstrong.*`), `q_audit` (`armstrong.#`) |

Jalankan dari folder `No 1`. Setiap worker memakai terminal sendiri (di Windows tambahkan `-P solo`):

```powershell
cd "No 1"

# Fan-Out: tiga worker
celery -A celery_app worker -Q fanout_a -n worker_a@%h -l info -P solo
celery -A celery_app worker -Q fanout_b -n worker_b@%h -l info -P solo
celery -A celery_app worker -Q fanout_c -n worker_c@%h -l info -P solo
python producer_pesan.py fanout "Halo semua worker"

# Topic dan Wildcard: empat worker
celery -A celery_app worker -Q q_kecil -n worker_kecil@%h -l info -P solo
celery -A celery_app worker -Q q_besar -n worker_besar@%h -l info -P solo
celery -A celery_app worker -Q q_semua -n worker_semua@%h -l info -P solo
celery -A celery_app worker -Q q_audit -n worker_audit@%h -l info -P solo
python producer_pesan.py topic "Uji topic"
python producer_pesan.py wildcard "Uji wildcard"
```

Hasil yang diharapkan:

| Routing key pesan | `q_kecil` | `q_besar` | `q_semua` (`armstrong.*`) | `q_audit` (`armstrong.#`) |
|---|:-:|:-:|:-:|:-:|
| `armstrong.kecil` | ✓ | – | ✓ | ✓ |
| `armstrong.besar` | – | ✓ | ✓ | ✓ |
| `armstrong.log.info` | – | – | – | ✓ |

Perbedaan singkat:

| Skema | Perilaku |
|---|---|
| Fan-Out | Satu pesan disalin ke semua queue yang terikat; routing key diabaikan |
| Topic | Pesan masuk ke queue yang binding-nya sama persis dengan routing key |
| Wildcard | Pesan masuk ke queue yang polanya cocok: `*` = tepat satu kata, `#` = nol kata atau lebih |

## No 2 dan 3 – Bilangan Armstrong dengan Celery dan Producer

Logika `is_armstrong()` dari UTS tidak berubah, dibungkus menjadi task `cek_armstrong`. Peran `broker.py` dan `worker.py` (socket) digantikan oleh broker RabbitMQ, worker Celery, dan producer.

| Berkas | Fungsi |
|---|---|
| `celery_app.py` | Konfigurasi Celery, queue `armstrong`, routing, `prefetch=1`, `acks_late` |
| `tasks.py` | Task `cek_armstrong(awal, akhir)` dengan retry otomatis |
| `producer.py` | Input generator: memecah rentang 1..BATAS menjadi potongan, mengirim ke broker, menggabungkan dan memverifikasi hasil |

Jalankan dari folder `No 2 dan 3`:

```powershell
cd "No 2 dan 3"

# Terminal 1-3 (atau 4): worker Armstrong
celery -A celery_app worker -Q armstrong -n worker1@%h -l info -P solo
celery -A celery_app worker -Q armstrong -n worker2@%h -l info -P solo
celery -A celery_app worker -Q armstrong -n worker3@%h -l info -P solo

# Terminal lain: Flower (opsional)
celery -A celery_app flower --port=5555

# Producer: 12 potongan, batas 10.000.000
python producer.py 12 10000000
```

Keluaran producer memuat daftar bilangan Armstrong yang ditemukan, jumlah tugas, worker yang aktif, waktu proses, dan hasil verifikasi terhadap 27 bilangan Armstrong yang sudah diketahui. Pada rentang 1 sampai 10.000.000 ditemukan 24 bilangan.

Catatan: pada Windows, pool bawaan Celery (prefork) tidak stabil sehingga dipakai `-P solo`. Setiap worker tetap berjalan sebagai proses terpisah, sehingga pekerjaan tetap paralel.

## Pemantauan

- **Flower**: http://localhost:5555 (worker online, status task, waktu pengerjaan)
- **RabbitMQ Manager**: tombol *RabbitMQ Manager* pada dashboard CloudAMQP (exchange, queue, dan binding)

## Perbandingan dengan UTS (socket)

| Aspek | UTS (socket TCP) | Tugas 2 (Celery) |
|---|---|---|
| Protokol pesan | Dirancang sendiri | Disediakan Celery (JSON) |
| Jumlah worker | Ditunggu tepat N worker | Worker bebas bergabung atau keluar |
| Pembagian rentang | Statis, N bagian | Potongan kecil, diambil worker yang siap |
| Worker gagal | Tidak ditangani | Retry dan `acks_late` |
| Pemantauan | Cetak di terminal | Flower dan RabbitMQ Manager |
