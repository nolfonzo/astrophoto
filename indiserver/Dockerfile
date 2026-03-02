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
    udev \
    && rm -rf /var/lib/apt/lists/*

# INDI server port
EXPOSE 7624

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
