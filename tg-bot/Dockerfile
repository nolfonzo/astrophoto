FROM python:3.11-slim

RUN pip install --no-cache-dir paho-mqtt requests

COPY tg_bot.py /tg_bot.py

CMD ["python", "-u", "/tg_bot.py"]
