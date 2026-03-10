#!/usr/bin/env python3
import json
import io
import boto3
import pandas as pd
import logging
from datetime import datetime

# Import the sync function from your main app file
from app import sync_logs_to_s3
from baseline import BaselineManager
from detector import AnomalyDetector

# ── 1. RETRIEVE THE SHARED LOGGER ───────────────────────────────────────────
# This retrieves the logger instance configured in app.py. 
# Any logs sent here go to both the console and 'app_events.log'.
logger = logging.getLogger("anomaly-pipeline")

s3 = boto3.client("s3")

# Define the columns we are monitoring for anomalies
NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"]

def process_file(bucket: str, key: str):
    # LOGGING: Record the start of a new processing event
    logger.info(f"EVENT: Processing started for s3://{bucket}/{key}")

    try:
        # 1. Download raw file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        # Read the CSV bytes into a Pandas DataFrame
        df = pd.read_csv(io.BytesIO(response["Body"].read()))

        logger.info(f"Loaded {len(df)} rows from {key}. Columns: {list(df.columns)}")

        # 2. Initialize the Baseline Manager and load the current stats from S3
        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()

        # 3. Update baseline with new batch values BEFORE scoring
        # This ensures the "normal" range evolves as new data comes in
        for col in NUMERIC_COLS:
            if col in df.columns:
                # Remove NaNs to avoid breaking math calculations
                clean_values = df[col].dropna().tolist()
                if clean_values:
                    baseline = baseline_mgr.update(baseline, col, clean_values)
        
        # LOGGING: Note that the baseline stats have been recalculated
        logger.info(f"EVENT: Baseline calculations updated for {key}")

        # 4. Initialize the Detector and score the current DataFrame
        # 'both' usually refers to Z-score + Isolation Forest methods
        detector = AnomalyDetector(z_threshold=3.0, contamination=0.05)
        scored_df = detector.run(df, NUMERIC_COLS, baseline, method="both")

        # 5. Prepare the scored CSV data for upload
        output_key = key.replace("raw/", "processed/")
        csv_buffer = io.StringIO()
        scored_df.to_csv(csv_buffer, index=False)
        
        # Upload the processed CSV back to the 'processed/' folder
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )

        # 6. Save the newly updated baseline JSON back to S3
        baseline_mgr.save(baseline)
        logger.info(f"EVENT: Updated baseline.json saved to s3://{bucket}/")

        # 7. Build a summary dictionary for metadata tracking
        anomaly_count = int(scored_df["anomaly"].sum()) if "anomaly" in scored_df else 0
        summary = {
            "source_key": key,
            "output_key": output_key,
            "processed_at": datetime.utcnow().isoformat(),
            "total_rows": len(df),
            "anomaly_count": anomaly_count,
            "anomaly_rate": round(anomaly_count / len(df), 4) if len(df) > 0 else 0,
            "baseline_observation_counts": {
                col: baseline.get(col, {}).get("count", 0) for col in NUMERIC_COLS
            }
        }

        # Write summary JSON alongside the processed file (useful for the /summary API)
        summary_key = output_key.replace(".csv", "_summary.json")
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )

        # 8. SYNC LOGS TO S3
        # As per requirements, we backup the local log file to S3 whenever a baseline is saved.
        sync_logs_to_s3()

        logger.info(f"EVENT: Processing Complete. {anomaly_count}/{len(df)} anomalies flagged.")
        return summary

    except Exception as e:
        # LOGGING: Use logger.exception to capture the full stack trace for debugging
        logger.exception(f"CRITICAL ERROR processing {key}: {e}")
        return None