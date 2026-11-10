"""
Konfigurasi aplikasi Celery untuk pencarian bilangan Armstrong.

Menggantikan peran broker.py (socket server) pada program UTS:
broker kini dipegang RabbitMQ, bukan program buatan sendiri.
"""
from celery import Celery
from kombu import Exchange, Queue

app = Celery(
    "armstrong",
    broker="amqp://guest:guest@localhost:5672//",   # RabbitMQ sebagai broker
    backend="rpc://",                               # hasil dikirim balik ke producer
    include=["tasks"],                              # modul task yang dimuat worker
)

app.conf.update(
    # Satu queue khusus untuk tugas Armstrong (task routing)
    task_queues=[
        Queue("armstrong", Exchange("armstrong", type="direct"), routing_key="armstrong"),
    ],
    task_routes={"tasks.cek_armstrong": {"queue": "armstrong"}},

    # Pesan dalam format JSON
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Pembagian kerja yang adil: setiap worker hanya mengambil 1 tugas
    # sebelum selesai, dan pesan baru dianggap selesai (ack) setelah
    # task benar-benar selesai dikerjakan.
    worker_prefetch_multiplier=1,
    task_acks_late=True,

    timezone="Asia/Jakarta",
    enable_utc=True,
)
