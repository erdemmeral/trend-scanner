#!/bin/bash

# Set the time you want the script to run
SCHEDULED_HOUR=9
SCHEDULED_MINUTE=8

# Create logs directory
mkdir -p logs

# Set up logging
exec 1> >(tee -a "logs/daily_run.log")
exec 2>&1

echo "[$(date)] Daily run script starting"
echo "[$(date)] Python path: $(which python3)"
echo "[$(date)] Current directory: $(pwd)"

while true; do
    CURRENT_HOUR=$(date +%H)
    CURRENT_MINUTE=$(date +%M)
    
    echo "[$(date)] Checking time: Current ${CURRENT_HOUR}:${CURRENT_MINUTE}, Target ${SCHEDULED_HOUR}:${SCHEDULED_MINUTE}"
    
    if [ "$CURRENT_HOUR" -eq "$SCHEDULED_HOUR" ] && [ "$CURRENT_MINUTE" -eq "$SCHEDULED_MINUTE" ]; then
        echo "[$(date)] Starting trend scanner"
        source venv/bin/activate
        PYTHONUNBUFFERED=1 python3 -u trend_scanner.py 2>&1
        deactivate
        echo "[$(date)] Scan complete"
        echo "[$(date)] Sleeping for 23 hours"
        sleep 82800
    else
        sleep 60
    fi
done 