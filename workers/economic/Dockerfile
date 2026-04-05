FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default entrypoint runs FRED ingester; override with docker run args
# for worldbank_ingester.py
CMD ["python", "-u", "fred_ingester.py"]
