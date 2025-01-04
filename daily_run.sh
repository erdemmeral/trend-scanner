#!/bin/bash

# Create logs directory
mkdir -p logs

# Activate virtual environment
source venv/bin/activate

# Run the scanner directly (no need for cloud_scheduler.py anymore)
python3 trend_scanner.py 