FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Add INDI official PPA for up-to-date drivers (includes Sony gphoto fixes)
RUN apt-get update && apt-get install -y software-properties-common \
    && add-apt-repository -y ppa:mutlaqja/ppa \
    && apt-get update && apt-get install -y \
    indi-bin \
    indi-gphoto \
    indi-eqmod \
    gphoto2 \
    libgphoto2-dev \
    python3 \
    udev \
    && rm -rf /var/lib/apt/lists/*

# INDI server + capture service ports
EXPOSE 7624 7625

COPY entrypoint.sh /entrypoint.sh
COPY capture_service.py /capture_service.py
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
