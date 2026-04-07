# --- Container 3: Embedding ---
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

WORKDIR /app

# Install specialized vector embedding dependencies (Heavy)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY sam_gov/ ./sam_gov/

# Run the worker with the dispatcher
CMD ["python", "-m", "sam_gov.workers.queue_pipeline.run_worker", "--worker", "embedding"]
