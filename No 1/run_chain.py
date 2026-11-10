from celery import chain
from tasks import process_data, generate_report

def create_task_chain(raw_data):
    """
    Menggunakan chain primitive milik Celery
    """
    job = chain(
        process_data.s(raw_data),
        generate_report.s()
    )
    return job.apply_async()