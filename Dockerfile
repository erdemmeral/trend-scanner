FROM python:3.12-slim

WORKDIR /app

# Install only required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY trend_scanner.py .
COPY pa_runner.py .
COPY .env .

# Set memory optimization environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONMALLOC=malloc
ENV MALLOC_ARENA_MAX=2

# Run with minimal memory settings
CMD ["python3", "pa_runner.py"] 