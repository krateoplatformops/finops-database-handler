FROM python:3.12.3

COPY . /tmp/database-handler
WORKDIR /tmp/database-handler

RUN pip install -r requirements.txt
CMD ["gunicorn", "main:app", "-k", "gevent", "--workers", "4", "--worker-connections", "1000", "--bind", "0.0.0.0:8088"]