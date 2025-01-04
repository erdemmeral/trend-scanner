#!/bin/bash

# Set the time you want the script to run
SCHEDULED_HOUR=8
SCHEDULED_MINUTE=40

# Create logs directory if it doesn't exist
mkdir -p logs

while true; do
    # Get current hour and minute
    CURRENT_HOUR=$(date +%H)
    CURRENT_MINUTE=$(date +%M)
    
    # Log current check
    echo "[$(date)] Checking time: Current ${CURRENT_HOUR}:${CURRENT_MINUTE}, Target ${SCHEDULED_HOUR}:${SCHEDULED_MINUTE}" >> logs/scheduler.log
    
    # If it's the scheduled time, run the scanner
    if [ "$CURRENT_HOUR" -eq "$SCHEDULED_HOUR" ] && [ "$CURRENT_MINUTE" -eq "$SCHEDULED_MINUTE" ]; then
        echo "[$(date)] Starting trend scanner" >> logs/scheduler.log
        python3 trend_scanner.py 2>&1 | tee -a logs/scanner.log
        echo "[$(date)] Scan complete" >> logs/scheduler.log
        
        # Sleep for 23 hours to avoid running multiple times
        echo "[$(date)] Sleeping for 23 hours" >> logs/scheduler.log
        sleep 82800
    else
        # Check every minute
        sleep 60
    fi
done 