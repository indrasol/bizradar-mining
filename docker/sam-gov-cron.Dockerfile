# ---------------------------------------------------------------------------
# Dockerfile — SAM.gov Ingestion Pipeline
#
# Standalone image for caj-sam-gov-scheduler (Azure Container App Job).
# Does NOT extend the shared base image — fully self-contained.
#
# Includes:
#   - Python 3.11
#   - Playwright + Chromium (for SAM.gov scraping)
#   - pandas, requests, aiohttp (for dedup, chunking, ingestion)
#   - Azure SDK (for blob upload via managed identity)
#
# Build (from repo root):
#   docker build -t sam-gov-pipeline:latest -f docker/sam-gov-cron.Dockerfile .
#
# The image is ~1.5-2 GB due to Chromium. This is normal for headless
# browser containers and only downloaded once by ACR.
# ---------------------------------------------------------------------------

    FROM python:3.11-slim

    ENV PYTHONDONTWRITEBYTECODE=1 \
        PYTHONUNBUFFERED=1
    
    WORKDIR /app
    
    # System dependencies:
    #   - gcc, python3-dev: compile native Python extensions
    #   - libpq-dev: psycopg2 (Postgres client, if needed downstream)
    #   - curl: health check probes
    RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        libpq-dev \
        curl \
        && rm -rf /var/lib/apt/lists/*
    
    # Python dependencies (separate from the shared requirements.txt)
    COPY sam_gov_requirements.txt .
    RUN pip install --no-cache-dir -r sam_gov_requirements.txt
    
    # Playwright: install the Chromium binary + all system-level libs it needs
    # (libnss3, libatk, libgbm, etc.). The --with-deps flag is critical on slim images.
    RUN playwright install --with-deps chromium
    
    # Copy the entire sam_gov package (includes acj_cron pipeline + shared modules)
    COPY sam_gov/ ./sam_gov/
    
    ENV PYTHONPATH="/app"
    
    # Default entrypoint: run the ACJ cron pipeline orchestrator
    CMD ["python", "-m", "sam_gov.services.acj_cron.main"]