#!/bin/bash
set -e

# Define variables
DOCKERFILE="function_docker/analytics.dockerfile"
DOCKER_IMAGE_NAME="analytics-function-builder"
FUNCTION_NAME="analytics"
OUTPUT_DIR="function_packages"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

echo "Building Docker image for $FUNCTION_NAME function..."
sudo docker build -t "$DOCKER_IMAGE_NAME" -f "$DOCKERFILE" .

echo "Creating container and copying function package..."
sudo docker create --name temp-container "$DOCKER_IMAGE_NAME"
sudo docker cp temp-container:/build/function.zip "$OUTPUT_DIR/${FUNCTION_NAME}_function.zip"
sudo docker rm temp-container

echo "Cleaning up..."
sudo docker rmi "$DOCKER_IMAGE_NAME"

echo "$FUNCTION_NAME function package built successfully at $OUTPUT_DIR/${FUNCTION_NAME}_function.zip"