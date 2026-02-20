The user wants me to create a comprehensive project summary in markdown format based on the conversation history. Let me analyze what happened:

1. User shared their project context (PDDP - a repository for laboratory works on parallel and distributed data processing)
2. User asked me to analyze the directory and generate a QWEN.md file
3. I explored the project structure, read multiple files, and created a comprehensive QWEN.md documentation file

Key findings from the exploration:
- This is a student repository for a "Parallel and Distributed Data Processing" course
- Lab 2 implements a Kafka + Spark streaming pipeline
- Services include: Spark master/workers, PostgreSQL, Kafka broker, and a Python producer
- I found a bug in consumer/requirements.txt (uses = instead of ==)
- The producer streams Instagram user engagement data to Kafka

Let me create a comprehensive summary following the specified format.# Project Summary

## Overall Goal
Document the PDDP repository structure and Lab 2 (Kafka + Spark streaming pipeline) for future development sessions.

## Key Knowledge

### Project Context
- **Repository**: PDDP - Laboratory assignments for "Parallel and Distributed Data Processing" course (Russian: "Параллельная и распределенная обработка данных")
- **Location**: `/home/maikal/projects/PDDP`
- **Language**: Python 3.11

### Lab 2 Architecture
| Component | Technology | Purpose |
|-----------|------------|---------|
| Message Broker | Apache Kafka | Stream ingestion |
| Processing | Apache Spark (PySpark) | Distributed stream processing |
| Storage | PostgreSQL 16 | Data persistence |
| Orchestration | Docker Compose | Multi-container deployment |

### Key Files
- `lab2/docker-compose.yaml` - Defines 5 services (spark-master, spark-1, spark-2, postgres, broker, producer)
- `lab2/producer/producer.py` - Streams CSV data to Kafka topic `instagram-users`
- `lab2/consumer/consumer.py` - Empty Spark consumer (to be implemented)
- `lab2/producer/data/instagram_usage_lifestyle.csv` - Dataset with user engagement/lifestyle metrics

### Configuration
- **Kafka**: `broker:9092` (internal), `localhost:9092` (external)
- **Spark Master UI**: `http://localhost:8080`
- **PostgreSQL**: `postgres:postgres@localhost:5432/postgres`
- **Producer env vars**: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `BATCH_SIZE`, `DELAY_SECONDS`, `CSV_FILE`

### Build/Run Commands
```bash
cd lab2
docker-compose up -d          # Start all services
docker-compose logs -f        # View logs
docker-compose down -v        # Stop and remove volumes
```

### Known Issues
- `consumer/requirements.txt` has invalid syntax: `pyspark=4.1.1` should be `pyspark==4.1.1`

## Recent Actions

1. **[DONE]** Explored project structure and read README.md
2. **[DONE]** Analyzed docker-compose.yaml and Dockerfile.producer
3. **[DONE]** Reviewed producer.py implementation and dependencies
4. **[DONE]** Inspected consumer directory (consumer.py is empty)
5. **[DONE]** Generated comprehensive `QWEN.md` documentation file

## Current Plan

| # | Task | Status |
|---|------|--------|
| 1 | Create QWEN.md documentation | [DONE] |
| 2 | Fix consumer/requirements.txt syntax error | [TODO] |
| 3 | Implement consumer.py (Spark streaming logic) | [TODO] |
| 4 | Test full pipeline (producer → Kafka → Spark → PostgreSQL) | [TODO] |

---

## Summary Metadata
**Update time**: 2026-02-17T08:51:03.704Z 
