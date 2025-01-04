#!/bin/bash

# Check if container is running
if ! docker ps | grep -q trend-scanner; then
    echo "Container not running. Restarting..."
    docker start trend-scanner
fi

# Check memory usage
docker stats trend-scanner --no-stream --format "Memory usage: {{.MemUsage}}" 