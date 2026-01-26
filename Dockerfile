# MAUDE Analyzer Docker Image
# Multi-stage build for production deployment

# Stage 1: Build frontend
FROM node:20-slim AS frontend-builder

WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build


# Stage 2: Python runtime
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY api/ api/
COPY src/ src/
COPY config/ config/
COPY scripts/ scripts/

# Copy built frontend from builder stage
COPY --from=frontend-builder /app/frontend/dist /app/static

# Create data directory for mounted volumes
RUN mkdir -p /app/data

# Environment variables
ENV MAUDE_DB_PATH=/app/data/maude.duckdb
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
