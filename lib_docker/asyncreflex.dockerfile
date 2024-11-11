FROM amazon/aws-lambda-python:3.10

# Install system dependencies
RUN yum install -y gcc

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Set the working directory
WORKDIR /asyncreflex

# Copy the asyncreflex directory
COPY lib /asyncreflex

# Install project dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

# Don't build the wheel, just install the package in editable mode
RUN pip install -e .
