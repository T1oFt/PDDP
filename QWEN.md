# PDDP - Parallel and Distributed Data Processing

Repository for laboratory assignments in the course "Parallel and Distributed Data Processing" (Параллельная и распределенная обработка данных).

## Project Structure

```
PDDP/
├── lab2/                      # Laboratory Work #2
│   ├── docker-compose.yaml    # Multi-container orchestration
│   ├── Dockerfile.producer    # Docker image for Kafka producer
│   ├── t.py                   # Utility script (CSV column inspector)
│   ├── producer/              # Kafka Producer Service
│   │   ├── producer.py        # Main producer script
│   │   ├── requirements.txt   # Python dependencies
│   │   └── data/              # Dataset directory
│   │       └── instagram_usage_lifestyle.csv
│   └── consumer/              # Spark Consumer Service
│       ├── consumer.py        # Main consumer script (empty)
│       └── requirements.txt   # Python dependencies
```

## Lab 2: Kafka + Spark Streaming Architecture

### Overview
This laboratory implements a real-time data streaming pipeline using:
- **Apache Kafka** - Message broker for streaming data
- **Apache Spark** - Distributed processing engine
- **PostgreSQL** - Data storage
- **Python** - Application logic

### Services (docker-compose.yaml)

| Service | Image | Purpose |
|---------|-------|---------|
| `spark-master` | apache/spark-py:latest | Spark cluster master |
| `spark-1`, `spark-2` | apache/spark-py:latest | Spark worker nodes |
| `postgres` | postgres:16 | Relational database |
| `broker` | apache/kafka:latest | Kafka message broker |
| `producer` | Custom (Python) | Kafka data producer |

### Producer Service

Streams Instagram user engagement data from a CSV file to a Kafka topic.

**Configuration (Environment Variables):**
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address (default: `broker:9092`)
- `KAFKA_TOPIC` - Topic name (default: `instagram-users`)
- `BATCH_SIZE` - Records per batch (default: `2000`)
- `DELAY_SECONDS` - Delay between batches (default: `1`)
- `CSV_FILE` - Source data file (default: `instagram_usage_lifestyle.csv`)

**Dependencies:** `kafka-python`, `pandas`

### Consumer Service

Spark-based consumer for processing streaming data from Kafka.

**Dependencies:** `pyspark`, `pandas`

> ⚠️ **Note:** The consumer `requirements.txt` has a typo - uses `=` instead of `==` for version pinning.

### Dataset

The `instagram_usage_lifestyle.csv` contains user engagement metrics with columns including:
- User demographics (age, gender, country, income, etc.)
- Instagram activity (daily_active_minutes, posts_created, followers, etc.)
- Lifestyle factors (exercise, sleep, diet, stress, etc.)
- Account settings (privacy, 2FA, subscription status, etc.)

## Building and Running

### Start All Services
```bash
cd lab2
docker-compose up -d
```

### Start Producer Only
```bash
cd lab2
docker-compose up -d producer broker
```

### View Logs
```bash
docker-compose logs -f producer
docker-compose logs -f spark-master
```

### Stop All Services
```bash
docker-compose down
```

### Stop and Remove Volumes
```bash
docker-compose down -v
```

## Development Notes

### Known Issues
1. **consumer/requirements.txt** - Invalid syntax: `pyspark=4.1.1` should be `pyspark==4.1.1`

### Kafka Topics
- Topic: `instagram-users`
- Partitions: 3 (default from broker config)
- Replication factor: 1

### Spark Cluster
- Master UI: `http://localhost:8080`
- Worker port: `7077`

### PostgreSQL
- Host: `localhost:5432`
- Credentials: `postgres` / `postgres`
- Database: `postgres`

### Kafka Broker
- Host: `localhost:9092`
- Internal: `broker:9092`

## Technologies

- **Python 3.11** - Runtime
- **Apache Kafka** - Message streaming
- **Apache Spark** - Distributed computing
- **PostgreSQL 16** - Database
- **Docker Compose** - Container orchestration
