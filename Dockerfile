# Use Python 3.11 slim for better performance
FROM python:3.11-slim

# Install system dependencies including required Playwright dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    libxss1 \
    libgconf-2-4 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt requirements_scraper.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements_scraper.txt

# Install Playwright and browsers
RUN playwright install chromium && \
    playwright install-deps chromium

# Copy application code
COPY *.py ./

# Create necessary directories (these will be populated by the application)
RUN mkdir -p /app/downloads /app/pipeline_output /app/weekly_output /app/batches

# Set environment variables for Render
ENV PYTHONUNBUFFERED=1
ENV RENDER=true
ENV BROWARD_HEADLESS=true

# Run the weekly automation script
CMD ["python", "weekly_automation.py"]
