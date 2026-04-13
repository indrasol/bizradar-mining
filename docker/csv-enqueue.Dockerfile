# --- CSV Enqueue Job ---
# Extends the shared base image and adds Playwright (headless Chromium)
# for downloading the SAM.gov CSV via browser automation.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

WORKDIR /app

# Install Playwright system dependencies and the Chromium browser binary.
# The playwright Python package is already in requirements.txt (installed in base),
# but the actual browser binary must be installed separately.
RUN playwright install --with-deps chromium

# Copy latest application code (rebuilt by caj-csv-enqueue-cicd.yml on every
# change to csv_enqueue_servicebus.py, data_extraction.py, or this Dockerfile)
COPY sam_gov/ ./sam_gov/

# Copy and make entrypoint executable
COPY docker/csv-enqueue-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
