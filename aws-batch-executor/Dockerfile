# Python 3.6 Image without Dependecies
FROM python:3.7

LABEL maintainer="vijay.vammi@astrazeneca.com"

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

ENV VIRTUAL_ENV=/opt/venv
RUN python -m virtualenv --python=/usr/local/bin/python $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY . /app
WORKDIR /app

RUN poetry install