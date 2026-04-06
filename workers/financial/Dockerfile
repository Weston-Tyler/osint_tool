FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default entrypoint runs sanctions aggregator; override with docker run args
# for icij_ingester.py
CMD ["python", "-u", "sanctions_aggregator.py"]
