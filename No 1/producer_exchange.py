"""
Producer uji coba message handling: Fan-Out, Topic, dan Wildcard.

Mengirim task Celery `tangani_pesan` ke tiga exchange berbeda, lalu
menampilkan berapa pesan yang masuk ke setiap queue.

Pemakaian:
    python producer_exchange.py            # ketiga skema
    python producer_exchange.py fanout     # hanya fan-out
    python producer_exchange.py topic      # hanya topic
    python producer_exchange.py wildcard   # hanya wildcard

Langkah:
  1. Jalankan producer ini SEBELUM worker demo menyala, agar jumlah pesan
     per queue terlihat (pesan masih menunggu di antrean).
  2. Nyalakan worker demo; worker akan mengeksekusi semua pesan dan
     menampilkan exchange + routing key setiap pesan di log-nya.
"""
import sys
import time

from celery_config import app, QUEUE_FANOUT, QUEUE_TOPIC, QUEUE_WILDCARD
from tasks import tangani_pesan

SKEMA = {
    "fanout": {
        "judul": "1. FAN-OUT",
        "exchange": "ex_fanout",
        "queues": QUEUE_FANOUT,
        "pesan": [("apa.saja", "Promo 11.11 dimulai!")],
    },
    "topic": {
        "judul": "2. TOPIC (key persis)",
        "exchange": "ex_topic",
        "queues": QUEUE_TOPIC,
        "pesan": [("log.error", "Database down"),
                  ("log.info", "User login"),
                  ("log.warning", "Disk 85%")],
    },
    "wildcard": {
        "judul": "3. WILDCARD (* dan #)",
        "exchange": "ex_wildcard",
        "queues": QUEUE_WILDCARD,
        "pesan": [("log.error", "Database down"),
                  ("log.info", "User login"),
                  ("log.db.error", "Query timeout"),
                  ("app.error", "Crash"),
                  ("log", "Log tanpa level")],
    },
}


def uji(conn, nama):
    s = SKEMA[nama]
    channel = conn.default_channel
    print("=" * 72)
    print(f"{s['judul']}  (exchange '{s['exchange']}')")
    print("=" * 72)

    # Pastikan exchange & queue ada dan kosong sebelum uji
    for q in s["queues"]:
        bq = q(channel)
        bq.declare()
        bq.purge()

    # Kirim task Celery ke exchange dengan routing key tertentu
    for routing_key, isi in s["pesan"]:
        tangani_pesan.apply_async(args=[isi], exchange=s["exchange"],
                                  routing_key=routing_key)
        print(f"  KIRIM  routing_key={routing_key!r:<16} isi={isi!r}")

    time.sleep(1)

    # Hitung pesan yang menunggu di setiap queue
    print("  ISI QUEUE:")
    total = 0
    for q in s["queues"]:
        _, jumlah, consumer = channel.queue_declare(queue=q.name, passive=True)
        total += jumlah
        binding = "(diabaikan)" if q.exchange.type == "fanout" else repr(q.routing_key)
        catatan = f"  ({consumer} worker sedang membaca)" if consumer else ""
        print(f"    queue {q.name:<14} binding={binding:<12} -> {jumlah} pesan{catatan}")
    print(f"  Total: {len(s['pesan'])} pesan dikirim, {total} pesan masuk ke queue\n")


if __name__ == "__main__":
    pilihan = sys.argv[1:] or ["fanout", "topic", "wildcard"]
    with app.connection_for_write() as conn:
        conn.ensure_connection(max_retries=3)
        for nama in pilihan:
            uji(conn, nama)
    print("Nyalakan worker demo untuk mengeksekusi pesan-pesan di atas.")
