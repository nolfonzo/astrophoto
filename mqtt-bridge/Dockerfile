FROM python:3.11-slim

RUN pip install --no-cache-dir paho-mqtt

COPY mqtt_bridge.py /mqtt_bridge.py

CMD ["python", "-u", "/mqtt_bridge.py"]
