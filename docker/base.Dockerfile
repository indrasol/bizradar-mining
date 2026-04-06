# --- Shared Base Image ---
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (GCC, Postgres drivers)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install shared requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire sam_gov folder to ensure all shared modules are available
COPY sam_gov/ ./sam_gov

# Set PYTHONPATH to include the app directory
ENV PYTHONPATH="/app"
