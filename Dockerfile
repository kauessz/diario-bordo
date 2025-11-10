# Dockerfile — FastAPI Backend (OTIMIZADO ANTI-OOM)
FROM python:3.11-slim

# OTIMIZAÇÕES DE MEMÓRIA (CRÍTICO!)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0 \
    PIP_NO_CACHE_DIR=1 \
    # Malloc otimizado (reduz fragmentação)
    MALLOC_ARENA_MAX=2 \
    MALLOC_MMAP_THRESHOLD_=131072 \
    MALLOC_TRIM_THRESHOLD_=131072 \
    MALLOC_TOP_PAD_=131072 \
    MALLOC_MMAP_MAX_=65536 \
    # NumPy/Pandas/OpenBLAS (limitar threads)
    OPENBLAS_NUM_THREADS=1 \
    OMP_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    MKL_NUM_THREADS=1

WORKDIR /app

# System deps (mínimos necessários)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Requirements first (cache de layer)
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    # Limpar cache pip manualmente
    rm -rf /root/.cache/pip

# App code
COPY backend ./backend

# Porta
ENV PORT=8080
EXPOSE 8080

# CRÍTICO: Usar uvicorn direto (sem gunicorn)
# 1 worker apenas (não multiplica memória)
CMD ["sh", "-c", "uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1 --limit-concurrency 10 --timeout-keep-alive 5"]