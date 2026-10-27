# Enterprise Data Pipeline Observability & AI Root-Cause Engine on GCP

An event-driven monitoring architecture built on Google Cloud Platform that ingests pipeline telemetry, evaluates failures with Gemini on Vertex AI, sends incident emails, and bifurcates data into operational (Firestore) and analytical (BigQuery) storage.

## Architecture

```
[ Pipelines / Simulator ]
           │
           ▼ (JSON via Pub/Sub)
   [ Cloud Pub/Sub ] (Topic: pipeline-events)
           │
           ▼ (Eventarc Trigger)
 [ Cloud Functions Gen 2 / Cloud Run ]
           │
     ┌─────┴───────────────┐
     ▼                     ▼
[Firestore]            [BigQuery]
(Live State)          (Run History)
     │                     │
     └──────────┬──────────┘
                ▼
      [Streamlit Dashboard]
```

## Setup & Deployment

### 1. Ingestion & Storage Setup
```bash
# Create Pub/Sub Topic
gcloud pubsub topics create pipeline-events

# Create Firestore Database
gcloud firestore databases create --database=pipeline-db --location=asia-south1 --type=firestore-native

# Create BigQuery Dataset and Table
bq --location=asia-south1 mk -d pipeline_analytics
bq mk --table pipeline_analytics.pipeline_runs \
  pipeline:STRING,status:STRING,duration_seconds:INTEGER,records_processed:INTEGER,error_type:STRING,error_message:STRING,suggested_action:STRING,timestamp:TIMESTAMP
```

### 2. Deploy Event Processor (Cloud Run Functions)
```bash
cd processor
gcloud functions deploy pipeline-processor \
  --runtime python311 \
  --trigger-topic pipeline-events \
  --entry-point process_pipeline_event \
  --region asia-south1 \
  --gen2 \
  --memory 512MB \
  --set-env-vars GOOGLE_CLOUD_PROJECT="YOUR_PROJECT_ID",ALERT_EMAIL_SENDER="YOUR_EMAIL@gmail.com",ALERT_EMAIL_PASSWORD="YOUR_APP_PASSWORD",ALERT_EMAIL_RECEIVER="RECEIVER_EMAIL@gmail.com"
```

### 3. Run Observability Dashboard
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py --server.port=8080 --server.enableCORS=false
```

### 4. Run Simulator
```bash
cd simulator
pip install -r requirements.txt
python3 simulate_pipelines.py
```
