#!/bin/bash

# Remove Poetry's build artifacts
poetry run python -c "import shutil; shutil.rmtree('dist', ignore_errors=True)"
poetry run python -c "import shutil; shutil.rmtree('build', ignore_errors=True)"

# Remove Python cache files
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
find . -type f -name "*.pyo" -delete
find . -type f -name "*.pyd" -delete

# Remove Cython-generated C files
find . -type f -name "*.c" -not -path "./venv/*" -delete

# Remove compiled extensions
find . -type f -name "*.so" -not -path "./venv/*" -delete

# Remove .egg-info directory
rm -rf *.egg-info

# Remove .mypy_cache if you're using mypy
rm -rf .mypy_cache

# Remove .pytest_cache if you're using pytest
rm -rf .pytest_cache

# Remove coverage reports if you're using coverage.py
rm -rf htmlcov
rm -f .coverage

echo "Build artifacts cleaned."
