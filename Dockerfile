FROM python:3.13-slim
LABEL authors="Kirill Karpenko" description="Tram analytics system"

# Set the working directory
WORKDIR /app

# Copy the files needed
COPY requirements/base/reqs-base.txt requirements/base.txt
COPY requirements/base/reqs-ultralytics.txt requirements/ultralytics.txt
COPY common ./common
COPY tram_analytics ./tram_analytics
COPY paths.py ./paths.py
COPY vendor ./vendor
COPY docker/download_demo_assets.sh ./download_demo_assets.sh
COPY docker/entrypoint.sh ./entrypoint.sh

# Make the shell scripts executable
RUN chmod +x ./download_demo_assets.sh ./entrypoint.sh

# install wget and unzip (needed to fetch and unzip demo assets)
RUN apt update && apt install -y --no-install-recommends wget unzip

# Install dependencies
# Note: Utilising a cache for pip for faster rebuilds
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements/base.txt

# Ultralytics: specifying a CPU-only version of PyTorch
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements/ultralytics.txt \
    --extra-index-url=https://download.pytorch.org/whl/cpu

# Set the application's entry point
ENTRYPOINT ["./entrypoint.sh"]
