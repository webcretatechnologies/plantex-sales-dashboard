FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    default-libmysqlclient-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Copy requirements first (docker cache optimization)
COPY pltx_dashboard/requirements.txt .

# Install dependencies
RUN uv pip install --system --no-cache -r requirements.txt

# ADD THESE
RUN pip install --no-cache-dir \
    gunicorn \
    uvicorn \
    uvloop \
    httptools \
    mysqlclient

# Copy project
COPY . .

# Create static directory
RUN mkdir -p /app/pltx_dashboard/staticfiles

EXPOSE 8000

# IMPORTANT
CMD ["gunicorn", \
    "pltx_dashboard.asgi:application", \
    "-k", "uvicorn.workers.UvicornWorker", \
    "--workers", "6", \
    "--worker-connections", "1000", \
    "--timeout", "300", \
    "--keep-alive", "5", \
    "--bind", "0.0.0.0:8000"]