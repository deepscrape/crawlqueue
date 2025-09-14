# Use an official Python runtime as a base image
FROM python:3.10-slim AS base

# Use the Playwright runtime as a secondary stage
# FROM zenika/alpine-chrome:with-playwright AS playwright

# Switch back to the base image
FROM base

# Set the working directory inside the container
WORKDIR /app

# Install minimal system dependencies for Playwright and Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libxcomposite1 libxcursor1 libxdamage1 libxrandr2 libdrm2 \
    libgbm1 libxss1 libasound2 libatk1.0-0 libatk-bridge2.0-0 libgtk-3-0 \
    curl fonts-liberation ca-certificates && \
    # \
    # libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf-2.0-0 \
    # libwoff1 libharfbuzz0b libwebpdemux2 libcairo2 libwebp7
    pip3 install --no-cache-dir --upgrade pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV NODE_ENV=production
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir playwright


# https://playwright.dev/docs/browsers 
# Install only the required Playwright browser (Chromium) or --with-deps add webkit firefox
RUN playwright install chromium

# Copy the application code into the container
COPY . .

# Use ENTRYPOINT instead of CMD
ENTRYPOINT ["python3", "scrape.py"]
