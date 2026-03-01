FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    indi-full \
    gphoto2 \
    libgphoto2-dev \
    indi-gphoto \
    udev \
    && rm -rf /var/lib/apt/lists/*

# INDI server port
EXPOSE 7624

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
