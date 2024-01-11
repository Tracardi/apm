FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && apt-get purge -y --auto-remove && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:/app:$PATH"
ENV PYTHONPATH="$VIRTUAL_ENV/bin:/app:$PYTHONPATH"

RUN $VIRTUAL_ENV/bin/python3 -m pip install --upgrade pip

WORKDIR /app

COPY requirements.txt requirements.txt
RUN $VIRTUAL_ENV/bin/pip install wheel
RUN $VIRTUAL_ENV/bin/pip --default-timeout=360 install -r requirements.txt

COPY app app

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/opt/venv/bin/python3", "app/main.py"]
