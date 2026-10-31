FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080 \
    CACHE_DIR=/tmp/layz_spotify \
    CACHE_TTL=3600

# ffmpeg is used by SpotiFLAC for tagging/conversion; ca-certificates for HTTPS
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY server.py .

# Run as a non-root user; give it ownership of the temp download folder
RUN useradd -m appuser \
    && mkdir -p /tmp/layz_spotify \
    && chown -R appuser:appuser /app /tmp/layz_spotify
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8080')+'/health', timeout=4)"

# One process (download locks + cleanup thread live in-process), many threads,
# long timeout so slow lossless downloads are not killed.
CMD ["sh", "-c", "exec gunicorn -w 1 --threads 8 --timeout 180 -b 0.0.0.0:${PORT} server:app"]
