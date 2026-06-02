FROM python:3.11-slim

WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/consumer.py .
COPY schemas/warehouse_event.avsc /app/schemas/warehouse_event.avsc
COPY schemas/warehouse_event_v2.avsc /app/schemas/warehouse_event_v2.avsc

CMD ["python", "consumer.py"]
