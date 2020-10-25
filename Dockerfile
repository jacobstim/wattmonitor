FROM python:3.8-alpine

COPY requirements.txt /
RUN pip install -r /requirements.txt

RUN mkdir -p /app
COPY src/ /app

WORKDIR /app
CMD python -u wattmonitor.py
