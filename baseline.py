#!/usr/bin/env python3
import json
import math
import boto3
import logging
from datetime import datetime
from typing import Optional

# Retrieve the logger instance configured in app.py
logger = logging.getLogger("anomaly-pipeline")
s3 = boto3.client("s3")

class BaselineManager:
    """
    Maintains a per-channel running baseline using Welford's online algorithm.
    Logs state changes and S3 interactions to the central app_events.log.
    """

    def __init__(self, bucket: str, baseline_key: str = "state/baseline.json"):
        self.bucket = bucket
        self.baseline_key = baseline_key

    def load(self) -> dict:
        """Downloads the current baseline stats from S3."""
        try:
            response = s3.get_object(Bucket=self.bucket, Key=self.baseline_key)
            logger.info(f"Successfully loaded baseline from s3://{self.bucket}/{self.baseline_key}")
            return json.loads(response["Body"].read())
        except s3.exceptions.NoSuchKey:
            # This is expected the very first time the app runs
            logger.warning(f"No baseline file found at {self.baseline_key}. Initializing new baseline.")
            return {}
        except Exception as e:
            logger.error(f"Error loading baseline from S3: {e}")
            return {}

    def save(self, baseline: dict):
        """Persists the updated baseline stats to S3."""
        try:
            baseline["last_updated"] = datetime.utcnow().isoformat()
            s3.put_object(
                Bucket=self.bucket,
                Key=self.baseline_key,
                Body=json.dumps(baseline, indent=2),
                ContentType="application/json"
            )
            logger.info(f"EVENT: Baseline state successfully saved to S3 ({self.baseline_key})")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to save baseline to S3: {e}")

    def update(self, baseline: dict, channel: str, new_values: list[float]) -> dict:
        """
        Updates Mean and M2 using Welford's algorithm.
        Logs the change in observation counts for the channel.
        """
        if channel not in baseline:
            logger.info(f"Initializing new channel tracking for: {channel}")
            baseline[channel] = {"count": 0, "mean": 0.0, "M2": 0.0}

        state = baseline[channel]
        initial_count = state["count"]

        # The math block: Numerically stable incremental mean/variance
        for value in new_values:
            state["count"] += 1
            delta = value - state["mean"]
            state["mean"] += delta / state["count"]
            delta2 = value - state["mean"]
            state["M2"] += delta * delta2

        # Update standard deviation
        if state["count"] >= 2:
            variance = state["M2"] / state["count"]
            state["std"] = math.sqrt(variance)
        else:
            state["std"] = 0.0

        # Use debug level so we don't spam the logs with every single channel update
        logger.debug(f"Updated {channel}: {initial_count} -> {state['count']} observations.")
        
        baseline[channel] = state
        return baseline

    def get_stats(self, baseline: dict, channel: str) -> Optional[dict]:
        """Retrieves stats for a specific column if they exist."""
        return baseline.get(channel)