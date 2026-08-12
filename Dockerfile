FROM python:3.11-slim

WORKDIR /app

# System deps needed by some Python packages (e.g. pyarrow, confluent-kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command is overridden per-service in docker-compose.yml
CMD ["python", "--version"]