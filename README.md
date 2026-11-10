# Celery + RabbitMQ: Task Chain, Exchange, dan Pencarian Bilangan Armstrong

**Kelompok 1**

| Nama              | NIM          |
|-------------------|--------------|
| Achmad Zakariyah  | 26051905010  |
| Hilda Bernika     | 26051905030  |

Repo ini berisi dua program yang memakai Celery dengan RabbitMQ sebagai broker:

1. **`task_chain/`**: merangkai dua task di queue berbeda, lalu menguji tiga tipe exchange (fan-out, topic, wildcard).
2. **`armstrong/`**: program pencarian bilangan Armstrong dari UTS. Versi UTS memakai socket buatan sendiri, versi ini ditulis ulang dengan Celery.

## Struktur

```
task_chain/
├── celery_config.py      # app Celery, exchange, queue, routing
├── tasks.py              # process_data, generate_report, process_and_report, tangani_pesan
├── run_chain.py          # chain process_data -> generate_report
├── producer.py           # menjalankan chain dengan dua cara
├── producer_exchange.py  # uji fan-out, topic, wildcard
└── demo.log              # log worker saat uji exchange

armstrong/
├── celery_app.py         # app Celery, queue 'armstrong'
├── tasks.py              # task cek_armstrong
└── producer.py           # memecah rentang, mengirim task, menggabungkan hasil
```

## Persiapan

Butuh Python 3 dan RabbitMQ yang jalan di `localhost:5672` dengan user `guest`.

```bash
pip install celery
```

Cara cepat menjalankan RabbitMQ lewat Docker:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
```

Panel admin ada di http://localhost:15672 (login `guest` / `guest`). Dari sini isi tiap queue bisa dilihat langsung.

Semua perintah worker di bawah memakai `-P threads` atau `-P solo` karena pool `prefork` bawaan Celery tidak jalan di Windows. Di Linux atau macOS opsi ini boleh dihapus.

---

## 1. Task Chain (`task_chain/`)

### Alur

```
producer.py
   │
   ├─ Cara 1: process_and_report  (queue: celery)
   │             ├─ process_data.delay()     → queue data_processing   → tunggu hasil
   │             └─ generate_report.delay()  → queue report_generation → tunggu hasil
   │
   └─ Cara 2: chain(process_data.s(), generate_report.s())
                 hasil process_data langsung diteruskan ke generate_report oleh Celery
```

`process_data` menghitung jumlah kata dari input (simulasi kerja 2 detik). `generate_report` membuat laporan dari hasil itu (simulasi 3 detik). Keduanya mengembalikan dict yang diserialisasi sebagai JSON.

Perbedaan dua cara itu: pada Cara 1, task `process_and_report` memanggil `.get()` di dalam task, jadi satu slot worker di queue `celery` ikut tertahan selama kedua subtask berjalan. Celery melarang ini secara default, makanya perlu `disable_sync_subtasks=False`. Pada Cara 2 tidak ada task yang menunggu task lain. Celery yang meneruskan hasilnya, jadi tidak ada slot worker yang terpakai untuk menunggu.

### Menjalankan

Buka tiga terminal di folder `task_chain/`, satu worker per queue:

```bash
celery -A celery_config worker --loglevel=info --queue=data_processing   -P threads --concurrency=2 -n w1@%h
celery -A celery_config worker --loglevel=info --queue=report_generation -P threads --concurrency=2 -n w2@%h
celery -A celery_config worker --loglevel=info --queue=celery            -P threads --concurrency=2 -n w3@%h
```

Lalu di terminal keempat:

```bash
python producer.py
```

Worker `w3` wajib diberi `--queue=celery`. Tanpa opsi itu, worker membaca semua queue yang terdaftar di `celery_config.py`, termasuk queue uji exchange, dan ikut memakan pesan demo di bagian berikutnya.

Kalau `w1` atau `w2` tidak dinyalakan, Cara 1 akan menggantung selamanya karena `process_and_report` menunggu subtask yang tidak pernah diambil.

---

## 2. Uji Exchange (`task_chain/producer_exchange.py`)

Satu task yang sama (`tangani_pesan`) dikirim ke tiga exchange dengan routing key berbeda. Queue tujuan ditentukan oleh tipe exchange dan binding key, bukan oleh task-nya. Task ini hanya mencetak exchange, routing key, dan isi pesan yang diterimanya.

| Exchange      | Tipe   | Queue dan binding key                                                  |
|---------------|--------|------------------------------------------------------------------------|
| `ex_fanout`   | fanout | `fan_email`, `fan_sms`, `fan_push` (routing key diabaikan)             |
| `ex_topic`    | topic  | `top_error` ← `log.error`, `top_info` ← `log.info`                     |
| `ex_wildcard` | topic  | `wc_semua_log` ← `log.#`, `wc_error_satu` ← `*.error`, `wc_error_mana` ← `#.error` |

Pada topic exchange, `*` cocok dengan tepat satu kata dan `#` cocok dengan nol kata atau lebih. Kata dipisah titik.

### Menjalankan

Jalankan producer **sebelum** worker menyala, supaya jumlah pesan yang menunggu di tiap queue bisa terlihat:

```bash
python producer_exchange.py            # ketiga skema
python producer_exchange.py wildcard   # atau satu skema saja: fanout / topic / wildcard
```

Setelah itu nyalakan worker yang membaca delapan queue uji:

```bash
celery -A celery_config worker --loglevel=info -P solo -n demo@%h -Q fan_email,fan_sms,fan_push,top_error,top_info,wc_semua_log,wc_error_satu,wc_error_mana
```

### Hasil

Hasil ini dicocokkan dengan `demo.log` (Celery 5.7.0, Windows 11).

**Fan-out**: 1 pesan dikirim, 3 pesan masuk queue.

| Routing key | `fan_email` | `fan_sms` | `fan_push` |
|-------------|:-----------:|:---------:|:----------:|
| `apa.saja`  | ✓           | ✓         | ✓          |

Di log, task ID `cd5ce93b…` muncul tiga kali. Itu karena exchange menyalin pesan yang sama ke tiap queue, bukan karena producer mengirim tiga kali.

**Topic**: 3 pesan dikirim, 2 pesan masuk queue.

| Routing key   | `top_error` | `top_info` |
|---------------|:-----------:|:----------:|
| `log.error`   | ✓           |            |
| `log.info`    |             | ✓          |
| `log.warning` |             |            |

`log.warning` tidak cocok dengan binding mana pun, jadi RabbitMQ membuangnya. Pesan ini tidak muncul di log worker.

**Wildcard**: 5 pesan dikirim, 9 pesan masuk queue.

| Routing key    | `wc_semua_log` (`log.#`) | `wc_error_satu` (`*.error`) | `wc_error_mana` (`#.error`) |
|----------------|:------------------------:|:---------------------------:|:---------------------------:|
| `log.error`    | ✓                        | ✓                           | ✓                           |
| `log.info`     | ✓                        |                             |                             |
| `log.db.error` | ✓                        |                             | ✓                           |
| `app.error`    |                          | ✓                           | ✓                           |
| `log`          | ✓                        |                             |                             |

Dua kasus yang membedakan `*` dan `#`:

- `log.db.error` tidak masuk `*.error` karena ada dua kata sebelum `error`, sedangkan `*` hanya cocok dengan satu kata. `#.error` menerimanya.
- `log` tanpa titik tetap masuk `log.#` karena `#` boleh cocok dengan nol kata.

---

## 3. Bilangan Armstrong (`armstrong/`)

Bilangan Armstrong adalah bilangan yang sama dengan jumlah tiap digitnya dipangkatkan banyaknya digit, misalnya 153 = 1³ + 5³ + 3³. Producer memecah rentang 1 sampai `BATAS` menjadi beberapa potongan, mengirim semuanya ke queue `armstrong` sebagai satu `group`, lalu menunggu dan menggabungkan hasil dari worker.

### Perbedaan dengan versi socket (UTS)

| Bagian            | Versi socket (UTS)                              | Versi Celery                                      |
|-------------------|-------------------------------------------------|---------------------------------------------------|
| Broker            | `broker.py`, ditulis sendiri                    | RabbitMQ                                          |
| Worker            | `worker.py`: buka koneksi, baca rentang, kirim teks | Fungsi biasa dengan `@app.task`              |
| Format hasil      | String dipisah koma                             | Dict JSON (rentang, hasil, nama worker, durasi)   |
| Pembagian kerja   | Satu rentang per worker, tetap sejak awal       | Potongan diambil dari antrean oleh worker yang sedang kosong |
| Worker harus siap duluan | Ya, broker menunggu semua worker terhubung | Tidak. Pesan menunggu di queue sampai ada worker |
| Worker gagal      | Broker menggantung                              | Task diulang sampai 3 kali dengan jeda 5 detik    |

Pembagian kerjanya bisa adil karena dua setelan di `celery_app.py`:

- `worker_prefetch_multiplier=1`: tiap proses worker hanya mengambil satu task dalam satu waktu. Tanpa ini, satu worker bisa menimbun beberapa potongan sementara worker lain menganggur.
- `task_acks_late=True`: pesan baru dihapus dari queue setelah task selesai. Kalau worker mati di tengah jalan, potongannya dikembalikan ke queue dan diambil worker lain.

Karena itu jumlah potongan sebaiknya lebih banyak dari jumlah worker. Defaultnya 12 potongan.

### Menjalankan

Nyalakan satu worker atau lebih di folder `armstrong/`. Beri nama berbeda dengan `-n` supaya hasil per worker terpisah di output producer:

```bash
celery -A celery_app worker --loglevel=info -Q armstrong -P solo -n w1@%h
celery -A celery_app worker --loglevel=info -Q armstrong -P solo -n w2@%h
```

Worker di komputer lain bisa ikut asalkan URL broker di `celery_app.py` diganti dari `localhost` ke IP komputer RabbitMQ. User `guest` RabbitMQ secara default hanya boleh login dari localhost, jadi untuk worker dari komputer lain perlu dibuat user baru.

Lalu jalankan producer:

```bash
python producer.py [JUMLAH_BAGIAN] [BATAS]
python producer.py 12 10000000
```

| Argumen         | Default      | Keterangan                          |
|-----------------|--------------|-------------------------------------|
| `JUMLAH_BAGIAN` | `12`         | Banyaknya potongan rentang (task)   |
| `BATAS`         | `10000000`   | Rentang yang dicek: 1 sampai `BATAS`|

Producer mencetak rentang, durasi, dan hasil tiap potongan, lalu ringkasan: jumlah bilangan yang ditemukan, berapa task yang dikerjakan tiap worker, waktu total, dan hasil verifikasi.

### Batasan

- Verifikasi hanya akurat sampai `BATAS` 99.999.999. Daftar `DIKETAHUI` berhenti di 88593477, jadi untuk rentang di atas itu bilangan 9 digit seperti 146511208 akan membuat verifikasi `SALAH` walaupun hasil hitungannya benar.
- `autoretry_for=(Exception,)` mengulang task untuk error apa pun, termasuk bug di kode. Bug seperti itu baru terlihat setelah tiga kali retry gagal.