#!/bin/bash

# Define the path to your Python script
SCRIPT_PATH="/home/lamisplus/lamisplus_ingestion_pipeline/file_deletion_process.py"
LOG_FILE="/home/lamisplus/lamisplus_ingestion_pipeline/logs/file_deletion_pipeline.log"

# Check if the script file exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "$(date +"%Y-%m-%d %H:%M:%S"): Error: Python script '$SCRIPT_PATH' not found. Exiting." >> "$LOG_FILE"
    exit 1
fi

# Function to check if the pipeline is running
is_pipeline_running() {
    pgrep -f "$SCRIPT_PATH" >/dev/null
}

# Check if the pipeline is already running
if is_pipeline_running; then
    echo "$(date +"%Y-%m-%d %H:%M:%S"): Pipeline is already running. Exiting." >> "$LOG_FILE"
    exit 0
else
    echo "$(date +"%Y-%m-%d %H:%M:%S"): Starting the pipeline..." >> "$LOG_FILE"
    python3 "$SCRIPT_PATH" >> "$LOG_FILE" 2>&1 &
    echo "$(date +"%Y-%m-%d %H:%M:%S"): Pipeline started successfully." >> "$LOG_FILE"
fi
