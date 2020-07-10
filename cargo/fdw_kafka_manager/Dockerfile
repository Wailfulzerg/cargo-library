FROM python:3.7

ENV PYTHONUNBUFFERED=1

RUN apt update -y && apt install -y
WORKDIR /cargo
COPY ./cargo .
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt


