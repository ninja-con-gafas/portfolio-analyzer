#!/bin/bash

set -e  # Exit on error

# Variables
DOCKER_IMAGE="portfolio-analyzer"
DOCKER_CONTAINER="portfolio-analyzer"
DOCKER_NETWORK="portfolio-analyzer-network"
DOCKER_SUBNET="192.168.0.0/24"
DOCKER_COMPOSE_FILE="docker-compose.yaml"
DOCKERFILE="Dockerfile"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Cleanup on exit
trap 'echo -e "${RED}Script interrupted! Cleaning up...${NC}"; exit 1' INT TERM

# Function to print messages
echo_success() {
    echo -e "${GREEN}$1${NC}"
}

echo_error() {
    echo -e "${RED}$1${NC}"
}

# Ensure necessary tools are installed
for cmd in docker docker-compose python3 pip; do
    if ! command -v $cmd &> /dev/null; then
        echo_error "Error: $cmd is not installed!"
        exit 1
    fi
done

# Create Dockerfile
echo_success "Creating Dockerfile"
cat <<EOF > "$DOCKERFILE"
# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Install git
RUN apt-get update && apt-get install -y --no-install-recommends \\
    git \\
    openjdk-17-jre-headless \\
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Java and Apache Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="\$JAVA_HOME/bin:\$PATH"

# Set the working directory in the container
WORKDIR /$DOCKER_IMAGE

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application to the container
COPY . .

# Expose Streamlit's default port
EXPOSE 8501

# Set Streamlit configuration
ENV STREAMLIT_CONFIG_DIR=/$DOCKER_IMAGE/.streamlit

# Run the Streamlit app
CMD ["streamlit", "run", "run.py", "--server.port=8501", "--server.address=0.0.0.0"]
EOF

# Create Docker Compose file
echo_success "Creating docker-compose.yaml"
cat <<EOF > "$DOCKER_COMPOSE_FILE"
version: '3.8'

services:
  $DOCKER_IMAGE:
    build:
      context: .
      dockerfile: $DOCKERFILE
    container_name: $DOCKER_CONTAINER
    restart: unless-stopped
    environment:
      - STREAMLIT_CONFIG_DIR=/$DOCKER_IMAGE/.streamlit
    volumes:
      - .:/$DOCKER_IMAGE
    ports:
      - "8501:8501"
    networks:
      - $DOCKER_NETWORK

networks:
  $DOCKER_NETWORK:
    driver: bridge
    ipam:
      config:
        - subnet: $DOCKER_SUBNET
EOF

# Build and run the Docker application
echo_success "Building the Docker image"
docker build -t "$DOCKER_IMAGE" .

echo_success "Application successfully built!"
echo_success "Run \`docker compose up -d\` to deploy the application."
