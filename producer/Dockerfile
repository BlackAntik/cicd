FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py .
COPY producer/register_schemas.py .
COPY schemas/warehouse_event.avsc /app/schemas/warehouse_event.avsc
COPY schemas/warehouse_event_v2.avsc /app/schemas/warehouse_event_v2.avsc

CMD ["python", "producer.py"]
