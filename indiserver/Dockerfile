FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN echo "deb http://ports.ubuntu.com/ubuntu-ports jammy universe" >> /etc/apt/sources.list \
    && echo "deb http://ports.ubuntu.com/ubuntu-ports jammy-updates universe" >> /etc/apt/sources.list \
    && apt-get update && apt-get install -y \
    indi-bin \
    indi-gphoto \
    indi-eqmod \
    gphoto2 \
    libgphoto2-dev \
    udev \
    && rm -rf /var/lib/apt/lists/*

# INDI server port
EXPOSE 7624

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
