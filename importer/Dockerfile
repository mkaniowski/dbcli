FROM debian:11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install necessary packages
RUN apt-get update && \
    apt-get install -y \
        bash \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /usr/src/app

# Copy and set permissions for dbcli
COPY dbcli /usr/src/app/dbcli
RUN chmod +x /usr/src/app/dbcli

# Copy the data directory directly into /usr/src/app/data/
COPY data/. /usr/src/app/data/

# Debugging: List the files and their contents
RUN ls -lh /usr/src/app/data/ && \
    cat /usr/src/app/data/popularity_iw.csv && \
    cat /usr/src/app/data/taxonomy_iw.csv

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
