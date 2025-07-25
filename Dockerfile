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
    dnsutils \
    iputils-ping \
    net-tools \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt requirements_scraper.txt ./

# Install Python dependencies with no cache to reduce image size
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements_scraper.txt && \
    pip cache purge

# Install Playwright and browsers
RUN playwright install chromium && \
    playwright install-deps chromium

# Copy application code
COPY *.py ./

# Create necessary directories with proper permissions
RUN mkdir -p /app/downloads /app/pipeline_output /app/weekly_output /app/batches && \
    chmod 755 /app/downloads /app/pipeline_output /app/weekly_output /app/batches

# Test network connectivity during build (optional, for debugging)
RUN echo "Testing DNS resolution..." && nslookup officialrecords.broward.org || echo "DNS test completed"

# Set environment variables for Render
ENV PYTHONUNBUFFERED=1
ENV RENDER=true
ENV BROWARD_HEADLESS=true
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Run the weekly automation script
CMD ["python", "weekly_automation.py"]
