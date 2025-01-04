#!/bin/bash

# Create logs directory
mkdir -p logs

# Activate virtual environment
source venv/bin/activate

# Run the scheduler
python3 cloud_scheduler.py 