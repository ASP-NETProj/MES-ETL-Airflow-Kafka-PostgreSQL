# MES ETL — Airflow + Kafka → PostgreSQL

**GitHub README + scripts**

This repository contains a minimal, practical setup to collect logs from multiple PC testers, publish them to **Apache Kafka**, and run scheduled **Apache Airflow** ETL tasks that read Kafka messages and write them into **PostgreSQL** (server). Suitable for MES (Manufacturing Execution System) pass/fail test logs.

> **Design note:** This solution uses Kafka as the message transport (decouples testers from server). Airflow runs a short-lived consumer job frequently (e.g., every minute) which polls Kafka for new messages and writes them into Postgres. For high-throughput streaming consider using a dedicated stream processing framework (Flink, Kafka Streams, or long-running consumers).

---

## Repository layout

```
README.md  (this file)
docker-compose.yml
/prod/producer.py        # simple log tail -> kafka producer (run on tester PCs)
/etl/consumer_etl.py     # kafka -> postgres ETL (invoked by Airflow DAG)
/airflow/dags/etl_dag.py # Airflow DAG that runs ETL every minute
/env/.env                # example env vars
/scripts/init_db.sql     # example table creation
```

---

## Quick start (development)

Requirements: `docker`, `docker-compose`, `python3` (for local testing). The docker-compose will spin up Zookeeper, Kafka, Postgres and a minimal Airflow stack.

1. Copy `.env.example` to `.env` and update if needed.

2. Start the stack (from repo root):

```bash
docker compose up -d
```

3. Initialize the database (example):

```bash
docker compose exec postgres bash -lc "psql -U postgres -f /docker-entrypoint-initdb.d/init_db.sql"
```

4. Start producer on a tester PC (see `/prod/producer.py`) — make sure the script points to the Kafka broker address (e.g. `kafka:9092` in container network or `HOST_IP:9092` from external PCs with port mapping).

5. Airflow webserver UI: `http://localhost:8080` (default creds in this repo). Enable the DAG `mes_kafka_to_postgres_etl`.

---

## Docker Compose (development)

```yaml
# docker-compose.yml
version: '3.7'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 2181:2181

  kafka:
    image: confluentinc/cp-kafka:7.4.1
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    ports:
      - 9092:9092
      - 29092:29092

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: mesdb
    volumes:
      - ./scripts/init_db.sql:/docker-entrypoint-initdb.d/init_db.sql:ro
    ports:
      - 5432:5432

  airflow:
    image: apache/airflow:2.9.1
    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./etl:/opt/etl
    depends_on:
      - postgres
      - kafka
    ports:
      - 8080:8080
    entrypoint: /bin/bash -c "airflow db init && airflow users create -u admin -p admin -f Admin -l User -r Admin -e admin@example.com || true && airflow webserver & airflow scheduler"

# Note: For production, replace images/configs with robust deployments, use a proper executor, and secure credentials.
```

---

## Database schema (scripts/init_db.sql)

```sql
CREATE TABLE IF NOT EXISTS tester_logs (
  id SERIAL PRIMARY KEY,
  tester_id TEXT NOT NULL,
  test_time TIMESTAMP WITH TIME ZONE NOT NULL,
  test_name TEXT,
  result TEXT,
  details JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tester_logs_test_time ON tester_logs(test_time);
```

---

## Producer (run on each Tester PC)

File: `/prod/producer.py`

```python
#!/usr/bin/env python3
# Requirements: pip install kafka-python

import os
import time
import json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('KAFKA_TOPIC', 'mes-tester-logs')
TESTER_ID = os.getenv('TESTER_ID', 'tester-01')
LOG_FILE = os.getenv('LOG_FILE', './sample_logs/log.txt')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,
)

print(f"Producer started -> {KAFKA_BOOTSTRAP} topic={TOPIC}")

# Simple tail-like implementation
with open(LOG_FILE, 'r') as f:
    # go to EOF
    f.seek(0, 2)
    while True:
        line = f.readline()
        if not line:
            time.sleep(0.5)
            continue
        line = line.strip()
        if not line:
            continue
        # example parse: CSV or custom — adapt as needed
        # assume a simple JSON per-line OR CSV: timestamp,test_name,result,details
        payload = None
        try:
            # try parse json line
            payload = json.loads(line)
        except Exception:
            parts = line.split(',')
            payload = {
                'tester_id': TESTER_ID,
                'test_time': parts[0] if len(parts) > 0 else None,
                'test_name': parts[1] if len(parts) > 1 else None,
                'result': parts[2] if len(parts) > 2 else None,
                'details': { 'raw': ','.join(parts[3:]) } if len(parts) > 3 else {},
            }

        producer.send(TOPIC, payload)
        producer.flush()
        print('sent', payload)
```

**Usage on tester PC**

```bash
pip install kafka-python
export KAFKA_BOOTSTRAP=your.kafka.host:29092
export TESTER_ID=tester-17
python prod/producer.py
```

Adjust `LOG_FILE` and parsing logic to match your log format.

---

## ETL consumer (invoked by Airflow DAG)

File: `/etl/consumer_etl.py`

```python
#!/usr/bin/env python3
# Requirements: pip install kafka-python psycopg2-binary

import os
import json
import time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'mes-tester-logs')
PG_DSN = os.getenv('PG_DSN', 'host=postgres user=postgres password=postgres dbname=mesdb')
POLL_SECONDS = int(os.getenv('POLL_SECONDS', '20'))  # how long to poll each run
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', '1000'))

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mes-etl-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=1000,
)

conn = psycopg2.connect(PG_DSN)
conn.autocommit = True
cur = conn.cursor()

start = time.time()
count = 0
for message in consumer:
    try:
        data = message.value
        tester_id = data.get('tester_id')
        test_time = data.get('test_time')
        test_name = data.get('test_name')
        result = data.get('result')
        details = data.get('details') if 'details' in data else data

        cur.execute(
            "INSERT INTO tester_logs (tester_id, test_time, test_name, result, details) VALUES (%s, %s, %s, %s, %s);",
            (tester_id, test_time, test_name, result, Json(details))
        )
        count += 1
    except Exception as e:
        print('failed to write message', e)

    # stop if we've run long enough
    if (time.time() - start) > POLL_SECONDS or count >= MAX_MESSAGES:
        break

print(f'ETL run completed, messages processed: {count}')
cur.close()
conn.close()
consumer.close()
```

**Notes:**

* This consumer is intentionally short-lived: it polls for messages and exits. Airflow schedules it frequently (e.g., every minute). This avoids running long-lived processes inside Airflow.
* For stronger delivery semantics, consider manual offset management and transactional writes.

---

## Airflow DAG

File: `/airflow/dags/etl_dag.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'mes',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'mes_kafka_to_postgres_etl',
    default_args=default_args,
    description='Poll Kafka and load to Postgres (short-lived consumer)',
    schedule_interval='*/1 * * * *',  # every minute
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    run_etl = BashOperator(
        task_id='run_consumer_etl',
        bash_command='python /opt/etl/consumer_etl.py',
    )

    run_etl
```

**Deployment**: `docker-compose.yml` mounts `./etl` into `/opt/etl` in the Airflow container so the DAG can call it. Adjust paths if you use a different setup.

---

## Environment variables (example `.env.example`)

```
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPIC=mes-tester-logs
PG_DSN=host=postgres user=postgres password=postgres dbname=mesdb
POLL_SECONDS=20
MAX_MESSAGES=1000
```

---

## Production considerations (short checklist)

* Use a proper Airflow executor (Celery/Kubernetes) for scalability.
* Secure Kafka (SASL/SSL) and Postgres (TLS + credentials in secrets manager).
* For large message volume, use streaming frameworks and avoid frequent short-polling.
* Add monitoring (consumer lag, postgres write latency) and logging.
* Use idempotent writes or upsert logic to avoid duplicates (add message IDs).

---

## Troubleshooting

* If producers cannot reach Kafka from tester PCs, map and expose broker ports and ensure `KAFKA_ADVERTISED_LISTENERS` includes reachable addresses (host/IP and container host).
* Airflow Python environment must include `kafka-python` and `psycopg2-binary`. If using the official Airflow image, extend it with a Dockerfile to `pip install kafka-python psycopg2-binary`.

---
