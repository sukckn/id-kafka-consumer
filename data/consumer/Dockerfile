FROM python:3.14-rc-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt 

COPY ./consumer/id-kafka-consumer.py /app/id-kafka-consumer.py

CMD ["python", "-u", "id-kafka-consumer.py"]
