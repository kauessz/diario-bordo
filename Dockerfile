# Dockerfile — FastAPI backend for Koyeb
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    # Tuning to reduce memory usage with numpy/pandas
    OPENBLAS_NUM_THREADS=1 \
    OMP_NUM_THREADS=1 \
    NUMEXPR_MAX_THREADS=1

WORKDIR /app

# System deps (psycopg2, openpyxl/xlrd use libpq/libxml/zlib)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Requirements first for better layer caching
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# App code
COPY backend ./backend

# Default port (Koyeb will route 80/443 to this container port)
ENV PORT=8080 UVICORN_WORKERS=1
EXPOSE 8080

# Start
CMD ["sh","-c","uvicorn backend.app:app --host 0.0.0.0 --port 8080 --workers ${UVICORN_WORKERS:-1} --timeout-keep-alive 50"]
