#!/bin/bash

# add schedule using cron right now every 6 hours
# 0 */6 * * * /bin/bash/ /Users/carter.louchheim/Desktop/ALP/ALP_firstbeat_api/run_firstbeat.sh >> /Users/carter.louchheim/Desktop/ALP/ALP_firstbeat_api/run_firstbeat.log 2>&1

# Path to scripts (adjust as needed)
PYTHON_SCRIPT="/Users/carter.louchheim/Desktop/ALP/ALP_firstbeat_api/firstbeat_api.py"
R_SCRIPT="/Users/carter.louchheim/Desktop/ALP/ALP_firstbeat_api/firstbeat_api.R"

# Run Python script to generate CSV
python3 "$PYTHON_SCRIPT"

# Short delay (5 seconds) to let write time
sleep 5

# Run R script to upload CSV and delete it
Rscript "$R_SCRIPT"