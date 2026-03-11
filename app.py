# app.py
import io
import json
import os
import boto3
import pandas as pd
import requests
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException

# Import your custom modules
from baseline import BaselineManager
from processor import process_file

# ── 1. LOGGING CONFIGURATION ───────────────────────────────────────────────
# Define the local log file name
LOG_FILE = "app_events.log"

# Setup the logger instance
logger = logging.getLogger("anomaly-pipeline")
logger.setLevel(logging.INFO)

# Handler 1: Console (Standard Output)
c_handler = logging.StreamHandler()

# Handler 2: Local File (Rotates at 5MB, keeps 2 backups)
f_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=2)

# Create a consistent format for both handlers
log_format = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
c_handler.setFormatter(log_format)
f_handler.setFormatter(log_format)

# Attach handlers to logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

# ── 2. APP INITIALIZATION ──────────────────────────────────────────────────
app = FastAPI(title="Anomaly Detection Pipeline")

# Initialize AWS clients
try:
    s3 = boto3.client("s3")
    BUCKET_NAME = os.environ["BUCKET_NAME"]
    logger.info(f"App starting. Targeted Bucket: {BUCKET_NAME}")
except KeyError:
    logger.error("CRITICAL: Environment variable 'BUCKET_NAME' is missing.")
    raise RuntimeError("BUCKET_NAME not set")

# ── 3. UTILITY FUNCTIONS ───────────────────────────────────────────────────

def sync_logs_to_s3():
    """
    Backs up the local app_events.log to the S3 bucket.
    Called whenever a baseline update occurs.
    """
    try:
        # We append a date to the S3 key so we have a history of logs
        datestamp = datetime.utcnow().strftime('%Y-%m-%d')
        s3_key = f"logs/app_events_{datestamp}.log"
        
        s3.upload_file(LOG_FILE, BUCKET_NAME, s3_key)
        logger.info(f"EVENT: Synced local logs to s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to sync logs to S3: {e}")

# ── 4. SNS NOTIFICATION HANDLER ─────────────────────────────────────────────

@app.post("/notify")
async def handle_sns(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except json.JSONDecodeError:
        logger.error("Failed to decode JSON from SNS request body")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    msg_type = request.headers.get("x-amz-sns-message-type")

    # Handle SNS Subscription Confirmation
    if msg_type == "SubscriptionConfirmation":
        confirm_url = body.get("SubscribeURL")
        if confirm_url:
            requests.get(confirm_url)
            logger.info(f"SNS Subscription confirmed via URL: {confirm_url}")
            return {"status": "confirmed"}
        
    # Handle S3 Event Notifications
    if msg_type == "Notification":
        try:
            s3_event = json.loads(body["Message"])
            for record in s3_event.get("Records", []):
                key = record["s3"]["object"]["key"]
                
                if key.startswith("raw/") and key.endswith(".csv"):
                    # LOGGING: Arrival of new file
                    logger.info(f"EVENT: New file detected: {key}")
                    
                    # Process the file in the background
                    background_tasks.add_task(process_file, BUCKET_NAME, key)
                else:
                    logger.debug(f"Skipping non-target S3 object: {key}")
                    
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse SNS Notification message: {e}")
            return {"status": "error", "message": "Malformed SNS message"}

    return {"status": "ok"}

# ── 5. QUERY ENDPOINTS ──────────────────────────────────────────────────────

@app.get("/anomalies/recent")
def get_recent_anomalies(limit: int = 50):
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")
        keys = sorted([
            obj["Key"] for page in pages for obj in page.get("Contents", [])
            if obj["Key"].endswith(".csv")
        ], reverse=True)[:10]

        if not keys: return {"count": 0, "anomalies": []}

        all_anomalies = []
        for key in keys:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_csv(io.BytesIO(response["Body"].read()))
            if "anomaly" in df.columns:
                flagged = df[df["anomaly"] == True].copy()
                flagged["source_file"] = key
                all_anomalies.append(flagged)

        combined = pd.concat(all_anomalies).head(limit) if all_anomalies else pd.DataFrame()
        return {"count": len(combined), "anomalies": combined.to_dict(orient="records")}
    except Exception as e:
        logger.exception("Error retrieving recent anomalies")
        raise HTTPException(status_code=500, detail="Server error")

@app.get("/anomalies/summary")
def get_anomaly_summary():
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")
        summaries = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("_summary.json"):
                    response = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
                    summaries.append(json.loads(response["Body"].read()))

        if not summaries: return {"message": "No processed files yet."}

        total_rows = sum(s["total_rows"] for s in summaries)
        total_anomalies = sum(s["anomaly_count"] for s in summaries)

        return {
            "files_processed": len(summaries),
            "total_rows_scored": total_rows,
            "total_anomalies": total_anomalies,
            "overall_anomaly_rate": round(total_anomalies / total_rows, 4) if total_rows > 0 else 0,
            "most_recent": sorted(summaries, key=lambda x: x["processed_at"], reverse=True)[:5],
        }
    except Exception as e:
        logger.exception("Error generating summary")
        raise HTTPException(status_code=500, detail="Server error")

@app.get("/baseline/current")
def get_current_baseline():
    try:
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()
        channels = {
            ch: {
                "observations": stats.get("count", 0),
                "mean": round(stats.get("mean", 0.0), 4),
                "std": round(stats.get("std", 0.0), 4),
                "baseline_mature": stats.get("count", 0) >= 30
            } for ch, stats in baseline.items() if ch != "last_updated"
        }
        return {"last_updated": baseline.get("last_updated"), "channels": channels}
    except Exception as e:
        logger.error(f"Error loading baseline: {e}")
        return {"status": "error", "message": "Could not load baseline."}

@app.get("/health")
def health():
    return {"status": "ok", "bucket": BUCKET_NAME, "timestamp": datetime.utcnow().isoformat()}
