from celery_config import app
import json
import time

# Task 1 (ringan): Memproses input data
@app.task(bind=True, queue='data_processing')
def process_data(self, raw_data):
    """
    Task 1: Memproses input data
    Queue: 'data_processing'
    """
    try:
        print(f"Processing data: {raw_data}")
        
        # Memproses... (di sini dimisalkan dengan sleep())
        time.sleep(2)
        
        # Membuat luaran (result) dalam .json (perhatikan celery_config telah di-setup mengembalikan dalam .json)
        processed_data = {
            'original': raw_data,
            'processed_at': time.time(),
            'word_count': len(str(raw_data).split()) if isinstance(raw_data, str) else 0,
            'status': 'processed'
        }
        
        print(f"Data processing completed: {processed_data}")
        return processed_data
        
    except Exception as exc:
        print(f"Error in process_data: {exc}")
        self.retry(countdown=60, max_retries=3, exc=exc)

# Task 2 (berat): Membuat report (laporan)
@app.task(bind=True, queue='report_generation')
def generate_report(self, processed_data):
    """
    Task 2: Membuat report dari data yang terproses
    Queue: 'report_generation'
    """
    try:
        print(f"Generating report from: {processed_data}")
        
        # Memproses... (di sini dimisalkan dengan sleep())
        time.sleep(3)
        
        # Membuat luaran (result) dalam .json (perhatikan celery_config telah di-setup mengembalikan dalam .json)
        report = {
            'report_id': f"RPT_{int(time.time())}",
            'data_summary': {
                'original_data': processed_data.get('original'),
                'word_count': processed_data.get('word_count', 0),
                'processed_at': processed_data.get('processed_at')
            },
            'generated_at': time.time(),
            'status': 'completed'
        }
        
        print(f"Report generation completed: {report}")
        return report
        
    except Exception as exc:
        print(f"Error in generate_report: {exc}")
        self.retry(countdown=60, max_retries=3, exc=exc)

# Melakukan chain pada Task 1 dan 2
@app.task(bind=True)
def process_and_report(self, raw_data):
    """
    Task yang bersifat sebagai orchestrator: merangkai (chain) Task 1 dan 2
    """
    try:
        # Eksekusi Task 1 dan simpan pada processed_data
        process_result = process_data.delay(raw_data)
        processed_data = process_result.get(disable_sync_subtasks=False)  # Wait for completion
        
        # Eksekusi Task 2 dan simpan pada final_report
        report_result = generate_report.delay(processed_data)
        final_report = report_result.get(disable_sync_subtasks=False)  # Wait for completion
        
        return final_report
        
    except Exception as exc:
        print(f"Error in process_and_report: {exc}")
        raise


# Task untuk uji coba message handling (fan-out, topic, wildcard).
# Satu task yang sama dikirim ke exchange berbeda; yang menentukan queue
# tujuan adalah tipe exchange + routing key, bukan task-nya.
@app.task(bind=True, ignore_result=True)
def tangani_pesan(self, isi):
    info = self.request.delivery_info or {}
    exchange = info.get('exchange')
    routing_key = info.get('routing_key')
    print(f"[{self.request.hostname}] exchange={exchange} "
          f"routing_key={routing_key} isi={isi!r}")
    return isi