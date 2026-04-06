FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default entrypoint — override with docker run command or compose service.
CMD ["python", "-u", "wfp_ingester.py"]
