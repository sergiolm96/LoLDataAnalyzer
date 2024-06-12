# syntax=docker/dockerfile:1

# Base image with specified Python version
ARG PYTHON_VERSION=3.10.11
FROM python:${PYTHON_VERSION}-slim as base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr.
ENV PYTHONUNBUFFERED=1

# Set the working directory
WORKDIR /app

# Create a non-privileged user to run the app.
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --uid "${UID}" \
    appuser

# Copy the requirements.txt into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Switch to the non-privileged user to run the application.
USER appuser

# Copy the source code into the container.
COPY . .

# Default command to run the application
CMD ["python", "Extracción_datos.py"]

