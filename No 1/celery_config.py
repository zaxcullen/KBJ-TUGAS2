from celery import Celery
from kombu import Exchange, Queue

# Initialize Celery app
app = Celery('task_chain', include=['tasks'])

# ---------------------------------------------------------------------------
# Exchange untuk uji coba message handling
# ---------------------------------------------------------------------------
# 1. FAN-OUT : routing key diabaikan, semua queue yang terikat dapat salinan
ex_fanout = Exchange('ex_fanout', type='fanout')
# 2. TOPIC   : pesan masuk ke queue yang binding key-nya SAMA PERSIS
ex_topic = Exchange('ex_topic', type='topic')
# 3. WILDCARD: exchange topic dengan pola  * (tepat 1 kata)  dan  # (0+ kata)
ex_wildcard = Exchange('ex_wildcard', type='topic')

# Queue untuk masing-masing skema (dipakai juga oleh producer_exchange.py)
QUEUE_FANOUT = [
    Queue('fan_email', ex_fanout),
    Queue('fan_sms', ex_fanout),
    Queue('fan_push', ex_fanout),
]
QUEUE_TOPIC = [
    Queue('top_error', ex_topic, routing_key='log.error'),
    Queue('top_info', ex_topic, routing_key='log.info'),
]
QUEUE_WILDCARD = [
    Queue('wc_semua_log', ex_wildcard, routing_key='log.#'),
    Queue('wc_error_satu', ex_wildcard, routing_key='*.error'),
    Queue('wc_error_mana', ex_wildcard, routing_key='#.error'),
]

# Configure Celery
app.conf.update(
    broker_url='amqp://guest@localhost//',
    result_backend='rpc://',

    # Semua queue didaftarkan: queue bawaan (celery), queue task chain,
    # dan queue uji coba fan-out / topic / wildcard.
    task_queues=[
        Queue('celery', Exchange('celery', type='direct'), routing_key='celery'),
        Queue('data_processing', Exchange('data_processing', type='direct'),
              routing_key='data_processing'),
        Queue('report_generation', Exchange('report_generation', type='direct'),
              routing_key='report_generation'),
        *QUEUE_FANOUT,
        *QUEUE_TOPIC,
        *QUEUE_WILDCARD,
    ],
    task_default_queue='celery',

    task_routes={
        'tasks.process_data': {'queue': 'data_processing'},
        'tasks.generate_report': {'queue': 'report_generation'},
    },
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)
