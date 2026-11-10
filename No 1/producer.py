import json
from tasks import process_and_report
from run_chain import create_task_chain

if __name__ == "__main__":
    # Cara 1: Dengan orchestrator
    print("=== Cara 1: Dengan orchestrator task ===")
    raw_input = "Ini adalah data contoh yang harus diproses dan dilaporkan."
    
    result = process_and_report.delay(raw_input)
    final_output = result.get()
    print(f"Final result: {json.dumps(final_output, indent=2)}")
    
    # Cara 2: Dengan chain milik Celery
    print("\n=== Cara 2: Dengan chain milik Celery ===")
    chain_result = create_task_chain(raw_input)
    chain_output = chain_result.get()
    print(f"Chain result: {json.dumps(chain_output, indent=2)}")


"""
Untuk menjalankan worker pada queues yang berbeda, gunakan perintah-perintah berikut pada terminal terpisah:

(Windows: -P threads wajib karena pool prefork tidak didukung)

# Terminal 1: Worker untuk Queue: data processing
celery -A celery_config worker --loglevel=info --queue=data_processing -P threads --concurrency=2 -n w1@%h

# Terminal 2: Worker untuk Queue: report generation
celery -A celery_config worker --loglevel=info --queue=report_generation -P threads --concurrency=2 -n w2@%h

# Terminal 3: General worker (untuk Task: orchestrator)
# --queue=celery wajib: tanpa itu worker juga membaca queue uji coba
# fan-out/topic/wildcard yang terdaftar di celery_config.py
celery -A celery_config worker --loglevel=info --queue=celery -P threads --concurrency=2 -n w3@%h

Uji coba message handling (fan-out, topic, wildcard): lihat producer_exchange.py

"""