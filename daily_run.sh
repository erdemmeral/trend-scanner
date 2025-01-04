#!/bin/bash

# Set the time you want the script to run (e.g., 2 AM UTC)
SCHEDULED_HOUR=2
SCHEDULED_MINUTE=0

while true; do
    # Get current hour and minute
    CURRENT_HOUR=$(date +%H)
    CURRENT_MINUTE=$(date +%M)
    
    # If it's the scheduled time, run the scanner
    if [ "$CURRENT_HOUR" -eq "$SCHEDULED_HOUR" ] && [ "$CURRENT_MINUTE" -eq "$SCHEDULED_MINUTE" ]; then
        echo "Starting trend scanner at $(date)"
        python3 trend_scanner.py
        echo "Scan complete at $(date)"
        
        # Sleep for 23 hours to avoid running multiple times
        sleep 82800  # 23 hours in seconds
    else
        # Check every minute
        sleep 60
    fi
done 