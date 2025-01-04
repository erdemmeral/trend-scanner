#!/bin/bash

# Update system
echo "Updating system..."
sudo apt update && sudo apt upgrade -y

# Install required packages
echo "Installing required packages..."
sudo apt install -y docker.io git python3-pip

# Start and enable Docker
echo "Setting up Docker..."
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER

# Clone the repository (replace with your repo URL)
echo "Cloning repository..."
git clone https://github.com/yourusername/trend-scanner.git
cd trend-scanner

# Create environment file
echo "Creating .env file..."
cat > .env << EOL
TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
TELEGRAM_CHAT_IDS=${TELEGRAM_CHAT_IDS}
SCAN_INTERVAL_HOURS=24
PORT=8070
HEALTH_CHECK_TIMEOUT=30
WEB_CONCURRENCY=2
MAX_REQUESTS=1000
KEEP_ALIVE=75
EOL

# Build and run Docker container
echo "Building Docker container..."
sudo docker build -t trend-scanner .

echo "Starting Docker container..."
sudo docker run -d \
  --name trend-scanner \
  --restart unless-stopped \
  --env-file .env \
  -p 8070:8070 \
  trend-scanner

echo "Setup complete!" 