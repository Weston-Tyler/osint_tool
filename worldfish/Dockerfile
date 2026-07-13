FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Keep the package importable: files land under /app/worldfish so the relative
# imports resolve and `python -m worldfish` works.
COPY . ./worldfish/

CMD ["python", "-u", "-m", "worldfish"]
