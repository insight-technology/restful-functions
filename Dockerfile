FROM python:3.7-slim
ENV PYTHONBUFFERD=1

WORKDIR /app

COPY requirements.txt /app/
COPY dev-requirements.txt /app/
COPY test-requirements.txt /app/

RUN apt-get update && apt-get install -y git gcc \
    && pip install -r requirements.txt && pip install -r dev-requirements.txt && pip install -r test-requirements.txt

COPY . /app/
