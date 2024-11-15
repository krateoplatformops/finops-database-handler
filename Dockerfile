FROM python:3.12.3

COPY . /tmp/database-handler
WORKDIR /tmp/database-handler

RUN pip install -r requirements.txt
CMD ["python3", "main.py"]