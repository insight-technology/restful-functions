FROM python:3.7-slim
ENV PYTHONBUFFERD=1

WORKDIR /app

COPY requirements.txt /app/
COPY dev-requirements.txt /app/
COPY test-requirements.txt /app/

RUN apt-get update \
    && apt-get install -y git \
    && pip install -r requirements.txt -r dev-requirements.txt -r test-requirements.txt
