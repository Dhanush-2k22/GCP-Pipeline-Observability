import json
import random
import time
from google.cloud import pubsub_v1

PROJECT_ID = "enterprise-pipeline-sys"
TOPIC_ID = "pipeline-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

PIPELINES = [
    {
        "name": "sales_pipeline",
        "avg_duration": 300,
        "avg_records": 100000,
        "failure_rate": 0.05,
        "possible_errors": [
            {"type": "SchemaMismatch", "message": "Column 'unit_price' expected FLOAT, received STRING."},
            {"type": "DuplicateTransactionBatch", "message": "Primary key conflict on batch_id 2026-X481."}
        ]
    },
    {
        "name": "inventory_sync",
        "avg_duration": 120,
        "avg_records": 25000,
        "failure_rate": 0.08,
        "possible_errors": [
            {"type": "DatabaseDeadlock", "message": "Deadlock detected when acquiring row locks on warehouse_stock."},
            {"type": "ConnectionTimeout", "message": "Postgres connection pool exhausted after 30s timeout."}
        ]
    },
    {
        "name": "user_analytics_pipeline",
        "avg_duration": 450,
        "avg_records": 500000,
        "failure_rate": 0.12,
        "possible_errors": [
            {"type": "OutOfMemoryError", "message": "Container killed by OOM killer; peak memory exceeded 8GB limit."},
            {"type": "ClickstreamParseError", "message": "Malformed JSON payload encountered in event batch offset 19042."}
        ]
    },
    {
        "name": "financial_reconciliation",
        "avg_duration": 600,
        "avg_records": 15000,
        "failure_rate": 0.10,
        "possible_errors": [
            {"type": "LedgerImbalance", "message": "Debit/credit total mismatch exceeds delta tolerance ($0.03 delta)."},
            {"type": "RateLimitExceeded", "message": "Downstream banking gateway returned HTTP 429 Too Many Requests."}
        ]
    }
]

def generate_event():
    pipeline_cfg = random.choice(PIPELINES)
    is_failure = random.random() < pipeline_cfg["failure_rate"]

    status = "FAILED" if is_failure else "SUCCESS"
    duration = max(5, int(random.gauss(pipeline_cfg["avg_duration"], pipeline_cfg["avg_duration"] * 0.15)))
    records = 0 if is_failure else max(0, int(random.gauss(pipeline_cfg["avg_records"], pipeline_cfg["avg_records"] * 0.2)))

    if is_failure:
        error_sample = random.choice(pipeline_cfg["possible_errors"])
        error_type = error_sample["type"]
        error_message = error_sample["message"]
    else:
        error_type = None
        error_message = None

    return {
        "pipeline": pipeline_cfg["name"],
        "status": status,
        "duration_seconds": duration,
        "records_processed": records,
        "error_type": error_type,
        "error_message": error_message
    }

def run_simulation(iterations=12, delay_seconds=1.5):
    print(f"Starting emission of {iterations} pipeline events to topic: {topic_path}...")
    for i in range(1, iterations + 1):
        event = generate_event()
        payload = json.dumps(event).encode("utf-8")
        
        future = publisher.publish(topic_path, payload)
        message_id = future.result()
        
        log_tag = "[FAIL]" if event["status"] == "FAILED" else "[OK]"
        print(f"[{i}/{iterations}] {log_tag} {event['pipeline']} -> Status: {event['status']} | Message ID: {message_id}")
        time.sleep(delay_seconds)
    print("Simulation run completed.")

if __name__ == "__main__":
    run_simulation()
