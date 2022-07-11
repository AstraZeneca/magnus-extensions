# Python 3.6 Image without Dependecies
FROM python:3.7-slim

LABEL maintainer="vijay.vammi@astrazeneca.com"

RUN apt-get update && apt-get install -y

RUN pip install poetry

ENV VIRTUAL_ENV=/opt/venv
RUN python -m virtualenv --python=/usr/local/bin/python $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ADD . /app
WORKDIR /app

RUN poetry install
