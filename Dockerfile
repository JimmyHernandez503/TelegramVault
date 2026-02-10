FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-spa \
    tesseract-ocr-eng \
    libgl1-mesa-glx \
    libglib2.0-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml poetry.lock ./
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

# Install frontend dependencies
COPY client/package*.json ./client/
RUN cd client && npm ci --only=production

# Copy application code
COPY backend ./backend
COPY client ./client
COPY main.py ./

# Build frontend
RUN cd client && npm run build

# Create necessary directories
RUN mkdir -p media sessions logs

# Expose ports
EXPOSE 8000 5000 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Start services
CMD ["bash", "-c", "python main.py & python -m backend.crawler_server & cd client && npm run preview -- --host 0.0.0.0 --port 5000"]
