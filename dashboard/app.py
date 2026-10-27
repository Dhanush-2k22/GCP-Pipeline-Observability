import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import firestore
from google.cloud import bigquery

st.set_page_config(page_title="Pipeline Monitor & Diagnostics", layout="wide", page_icon="⚙️")
st.title("Enterprise Data Pipeline Monitor & Root-Cause Engine")

PROJECT_ID = "enterprise-pipeline-sys"
DATABASE_ID = "pipeline-db"
DATASET_ID = "pipeline_analytics"
TABLE_ID = "pipeline_runs"

@st.cache_resource
def get_clients():
    db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
    bq = bigquery.Client(project=PROJECT_ID)
    return db, bq

db, bq = get_clients()

col_title, col_btn = st.columns([6, 1])
with col_btn:
    if st.button("🔄 Refresh"):
        st.rerun()

# -------------------------------------------------------------
# 1. LIVE STATUS & ROOT-CAUSE DIAGNOSTICS (Firestore)
# -------------------------------------------------------------
st.subheader("Live Operational Status")

try:
    docs = db.collection("pipeline_state").stream()
    live_records = []
    for doc in docs:
        data = doc.to_dict()
        data["pipeline"] = doc.id
        live_records.append(data)
    
    if live_records:
        cols = st.columns(len(live_records))
        for idx, item in enumerate(live_records):
            with cols[idx]:
                status = item.get("current_status", "UNKNOWN")
                st.metric(
                    label=item.get("pipeline"),
                    value=status,
                    delta=f"{item.get('last_run_duration_sec', 0)}s duration"
                )
                st.caption(f"Records: {item.get('records_processed', 0):,}")
                
                if status == "FAILED":
                    st.error(f"**Error:** {item.get('error_type', 'Unknown')}")
                    st.caption(f"**Message:** {item.get('error_message', 'No message provided')}")
                    if item.get("suggested_action"):
                        st.info(f"💡 **Suggested Fix:**\n{item.get('suggested_action')}")
                else:
                    st.success("Operating normally")
    else:
        st.info("No active pipeline states found in Firestore.")
except Exception as e:
    st.error(f"Error reading Firestore: {e}")

st.divider()

# -------------------------------------------------------------
# 2. HISTORICAL LOGS & ERROR BREAKDOWN (BigQuery)
# -------------------------------------------------------------
st.subheader("Historical Analytics & Failure Trends")

query = f"""
SELECT 
    pipeline,
    status,
    duration_seconds,
    records_processed,
    error_type,
    error_message,
    suggested_action,
    timestamp
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
ORDER BY timestamp DESC
LIMIT 200;
"""

try:
    df = bq.query(query).to_dataframe()
    
    if not df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Execution Status Distribution")
            fig_pie = px.pie(df, names="status", color="status",
                             color_discrete_map={"SUCCESS": "#2ecc71", "FAILED": "#e74c3c"})
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with col2:
            st.markdown("### Failures by Error Type")
            failed_df = df[df["status"] == "FAILED"]
            if not failed_df.empty and "error_type" in failed_df.columns:
                fig_err = px.bar(failed_df, x="error_type", color="pipeline",
                                 title="Error Type Frequency")
                st.plotly_chart(fig_err, use_container_width=True)
            else:
                st.info("No failures recorded yet.")

        st.markdown("### Run History Log")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No historical records found in BigQuery.")
except Exception as e:
    st.error(f"Error querying BigQuery: {e}")
