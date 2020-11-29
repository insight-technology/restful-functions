FROM python:3.6-slim
ENV PYTHONBUFFERD=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y curl jq make git

RUN curl -sSLO https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py \
    && POETRY_HOME=/etc/poetry python get-poetry.py -y --version 1.1.4 \
    && rm get-poetry.py \
    && cat /etc/poetry/env >> /root/.bashrc \
    && . /etc/poetry/env \
    && mkdir /etc/bash_completion \
    && poetry completions bash > /etc/bash_completion.d/poetry.bash-completion

COPY pyproject.toml poetry.lock /app/

RUN . /etc/poetry/env \
    && poetry install
