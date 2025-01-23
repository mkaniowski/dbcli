FROM debian:11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
        bash \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY dbcli ./dbcli
RUN chmod +x ./dbcli
CMD ["tail", "-f", "/dev/null"]

