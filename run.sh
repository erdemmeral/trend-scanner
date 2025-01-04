#!/bin/bash

# Install dependencies
apt update
apt install -y python3-pip git screen

# Install Python packages
pip3 install -r requirements.txt

# Start the bot in a screen session
screen -dmS trendbot python3 trend_scanner.py 