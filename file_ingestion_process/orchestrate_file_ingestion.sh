#!/bin/bash
# Define the path to your Python script
SCRIPT_PATH="/home/lamisplus/lamisplus_ingestion_pipeline/file_ingestion_process.py"
LOG_FILE="/home/lamisplus/lamisplus_ingestion_pipeline/logs/file_ingestion.log"

# Change directory to your project directory
cd /home/lamisplus || { echo "Error: Unable to change directory." >&2; exit 1; }

# Activate the virtual environment
source lamisplus_venv/bin/activate || { echo "Error: Unable to activate virtual environment." >&2; exit 1; }

# Change to project directory
cd /home/lamisplus/lamisplus_ingestion_pipeline || { echo "Error: Unable to change directory." >&2; exit 1; }

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
    # Run the script and capture both stdout and stderr
    if python3 "$SCRIPT_PATH" >> "$LOG_FILE" 2>&1; then
        echo "$(date +"%Y-%m-%d %H:%M:%S"): Pipeline completed successfully." >> "$LOG_FILE"
        exit 0
    else
        echo "$(date +"%Y-%m-%d %H:%M:%S"): Pipeline failed. See log file for details." >> "$LOG_FILE"
        exit 1
    fi
fi
