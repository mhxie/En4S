#!/bin/bash

# Set the target directory for the layers
TARGET_DIR="./layers"

# Remove the old build
rm -rf "$TARGET_DIR"
# Ensure the target directory exists
mkdir -p "$TARGET_DIR"

# Function to build and extract a layer
build_and_extract_layer() {
    local name=$1
    local dockerfile=$2
    local zip_name=$3

    echo "Building $name layer..."
    docker build -t "$name-layer" -f "$dockerfile" .
    docker create --name "$name-container" "$name-layer"
    docker cp "$name-container:/packages/$zip_name" "$TARGET_DIR/"
    docker rm "$name-container"

    echo "Unzipping $name layer..."
    unzip -q "$TARGET_DIR/$zip_name" -d "$TARGET_DIR/$name-python310"
    rm "$TARGET_DIR/$zip_name"
}

# Build CV2 layer
build_and_extract_layer "cv2" "layer_docker/cv2_headless.dockerfile" "cv2-python310.zip"

# Build ImageIO layer
build_and_extract_layer "imageio" "layer_docker/iio.dockerfile" "iio-python310.zip"

# Build Thrift layer
build_and_extract_layer "thrift" "layer_docker/thrift.dockerfile" "thrift-python310.zip"

# Build NumPy layer
build_and_extract_layer "numpy" "layer_docker/numpy.dockerfile" "numpy-python310.zip"

echo "Layers have been built, unzipped, and organized in $TARGET_DIR"

# Optionally, remove the Docker images if you don't need them anymore
# docker rmi cv2-layer imageio-layer thrift-layer numpy-layer

# List the contents of the target directory
echo "Contents of $TARGET_DIR:"
ls -l "$TARGET_DIR"

# List the contents of each unzipped directory
for layer in cv2 imageio thrift numpy; do
    echo "Contents of ${layer}-python310:"
    ls -l "$TARGET_DIR/${layer}-python310"
done