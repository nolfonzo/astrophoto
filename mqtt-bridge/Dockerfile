FROM python:3.11-slim

RUN apt-get update && apt-get install -y gphoto2 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir paho-mqtt Pillow

COPY mqtt_bridge.py /mqtt_bridge.py

CMD ["python", "-u", "/mqtt_bridge.py"]
