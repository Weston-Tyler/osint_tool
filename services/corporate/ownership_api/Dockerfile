FROM python:3.12-slim AS base

LABEL maintainer="MDA Platform Team"
LABEL component="corporate-ownership-api"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ownership_api.py .

# Copy the shell_detector module (imported at runtime by /shell-check)
COPY ../shell_detector /app/services/corporate/shell_detector

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "ownership_api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
