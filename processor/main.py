import base64
import json
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google import genai
from google.cloud import bigquery
from google.cloud import firestore

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "enterprise-pipeline-sys")
DATASET_ID = "pipeline_analytics"
TABLE_ID = "pipeline_runs"
DATABASE_ID = "pipeline-db"

# SMTP Alert configuration from environment variables
ALERT_EMAIL_SENDER = os.environ.get("ALERT_EMAIL_SENDER")
ALERT_EMAIL_PASSWORD = os.environ.get("ALERT_EMAIL_PASSWORD")
ALERT_EMAIL_RECEIVER = os.environ.get("ALERT_EMAIL_RECEIVER")

db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
bq_client = bigquery.Client(project=PROJECT_ID)
ai_client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")

def send_failure_email(pipeline_name, error_type, error_message, suggested_fix):
    """Sends an incident notification email containing Gemini's remediation action."""
    if not (ALERT_EMAIL_SENDER and ALERT_EMAIL_PASSWORD and ALERT_EMAIL_RECEIVER):
        print("Email alerts skipped: Missing email credentials in environment variables.")
        return

    subject = f"🚨 [CRITICAL] Pipeline Failure Alert: {pipeline_name}"
    
    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <h2 style="color: #d9534f;">🚨 Production Pipeline Incident Detected</h2>
        <p>A mission-critical data pipeline encountered an unhandled execution failure.</p>
        
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
          <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;">Pipeline:</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{pipeline_name}</td>
          </tr>
          <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;">Error Classification:</td>
            <td style="padding: 8px; border: 1px solid #ddd; color: #d9534f;"><b>{error_type}</b></td>
          </tr>
          <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #f9f9f9;">Error Details:</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{error_message}</td>
          </tr>
        </table>
        
        <div style="margin-top: 20px; padding: 15px; background: #e8f4fd; border-left: 4px solid #0288d1; max-width: 580px;">
          <h4 style="margin: 0 0 8px 0; color: #0288d1;">🤖 Gemini AI Root-Cause Remediation:</h4>
          <p style="margin: 0; font-size: 14px;">{suggested_fix}</p>
        </div>
        
        <p style="margin-top: 25px; font-size: 12px; color: #777;">
          Sent automatically from Google Cloud Run Functions ({PROJECT_ID}).
        </p>
      </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_SENDER
    msg["To"] = ALERT_EMAIL_RECEIVER
    msg.attach(MIMEText(html_content, "html"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(ALERT_EMAIL_SENDER, ALERT_EMAIL_PASSWORD)
        server.sendmail(ALERT_EMAIL_SENDER, ALERT_EMAIL_RECEIVER, msg.as_string())
        server.quit()
        print(f"Alert email successfully dispatched to {ALERT_EMAIL_RECEIVER}")
    except Exception as e:
        print(f"Failed to dispatch alert email: {e}")

def get_ai_suggested_fix(pipeline_name, error_type, error_message):
    """Invokes Vertex AI (Gemini 2.5 Flash) to generate remediation actions."""
    prompt = f"""
You are an expert Data Reliability Engineer. A production data pipeline has failed.

Pipeline Name: {pipeline_name}
Error Type: {error_type}
Error Message: {error_message}

Provide a concise, highly actionable 2-sentence fix recommendation for an on-call engineer. 
Focus on specific configuration, infrastructure, or code changes they should verify immediately.
Do not use bullet points or filler text.
"""
    try:
        response = ai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        return response.text.strip()
    except Exception as e:
        print(f"Vertex AI Gemini invocation error: {e}")
        return "Inspect application logs in Cloud Logging and verify upstream dependencies."

def process_pipeline_event(event, context):
    """Entry point for Cloud Functions Gen 2 triggered by Pub/Sub."""
    if "data" not in event:
        return

    try:
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        payload = json.loads(raw_data)
    except Exception as e:
        print(f"Failed to decode message: {e}")
        return

    pipeline_name = payload.get("pipeline")
    status = payload.get("status")
    duration = payload.get("duration_seconds")
    records = payload.get("records_processed")
    error_type = payload.get("error_type")
    error_message = payload.get("error_message")
    now_utc = datetime.now(timezone.utc)

    if not pipeline_name or not status:
        return

    if status == "FAILED":
        print(f"Generating AI diagnosis for {pipeline_name} failure...")
        suggested_action = get_ai_suggested_fix(pipeline_name, error_type, error_message)
        send_failure_email(pipeline_name, error_type, error_message, suggested_action)
    else:
        suggested_action = None

    # 1. Update Firestore (Operational State - Overwrite)
    try:
        doc_ref = db.collection("pipeline_state").document(pipeline_name)
        doc_ref.set({
            "current_status": status,
            "last_run_duration_sec": duration,
            "records_processed": records,
            "error_type": error_type,
            "error_message": error_message,
            "suggested_action": suggested_action,
            "last_updated": now_utc
        }, merge=True)
    except Exception as e:
        print(f"Firestore write error: {e}")

    # 2. Append to BigQuery (Analytical History - Append Only)
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        rows_to_insert = [{
            "pipeline": pipeline_name,
            "status": status,
            "duration_seconds": duration,
            "records_processed": records,
            "error_type": error_type,
            "error_message": error_message,
            "suggested_action": suggested_action,
            "timestamp": now_utc.isoformat()
        }]
        bq_client.insert_rows_json(table_ref, rows_to_insert)
    except Exception as e:
        print(f"BigQuery write error: {e}")
