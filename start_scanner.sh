#!/bin/bash

# Set working directory
cd /home/newstracker502/trend-scanner

# Create logs directory
mkdir -p logs

# Activate virtual environment and set environment variables
source venv/bin/activate
export PYTHONUNBUFFERED=1

# Start the scanner with nohup
nohup python3 trend_scanner.py > logs/nohup.log 2>&1 &

# Save the process ID
echo $! > scanner.pid

echo "Scanner started with PID $(cat scanner.pid)" 