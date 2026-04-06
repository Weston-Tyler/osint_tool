FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential librdkafka-dev cmake libssl-dev libicu-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY opensanctions_sync.py .

ENTRYPOINT ["python", "opensanctions_sync.py"]
