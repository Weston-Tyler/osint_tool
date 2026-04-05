FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir elasticsearch>=8.12.0 gqlalchemy>=1.6.0

COPY . .

CMD ["python", "-u", "es_sync.py"]
