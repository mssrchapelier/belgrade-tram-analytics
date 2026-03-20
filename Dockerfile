FROM python:3.13-slim
LABEL authors="Kirill Karpenko" description="Tram analytics system"

# Set the working directory
WORKDIR /app

# Copy the list of dependencies
COPY requirements/base-headless.locked.txt requirements.txt

# Install dependencies
# Note: Utilising a cache for pip for faster rebuilds
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the files needed
COPY common ./common
COPY tram_analytics ./tram_analytics
COPY paths.py ./paths.py
COPY vendor ./vendor

# Set the application's entry point
ENTRYPOINT ["python", "-m", "tram_analytics.v1.launcher_joint"]