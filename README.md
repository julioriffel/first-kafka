# Kafka High-Performance Micro-Infrastructure

A robust, performant Kafka producer-consumer application leveraging **Confluent Platform 8.0.3 (Apache Kafka 4.0)** in **KRaft mode**. This project is designed for massive horizontal scaling with automated partition management and persistent storage.

## ✨ Key Features

- **Kafka 8.0.3 (KRaft)**: Modern Zookeeper-less architecture.
- **Massive Scaling**: Configured with **24 partitions** and **6 consumer replicas** for ultra-high throughput.
- **High Performance**: 
  - **Producer**: Snappy compression, `linger.ms=20`, and throttled polling ($1:100$).
  - **Consumer**: Optimized batch fetching ($100$ messages/batch) and tuned network parameters.
- **Persistence**: Full data persistence across restarts via Docker volumes.
- **Auto-Initialization**: Dedicated `init-kafka` service for idempotent topic creation and configuration.

---

## 🏗️ Project Structure

```text
first-kafka/
├── src/
│   ├── producer.py     # Optimized async producer
│   ├── consumer.py     # Scalable batch consumer
│   └── config.py       # Pydantic-based configuration
├── tests/              # Pytest suite
├── docs/               # Technical documentation
├── Dockerfile          # Multi-stage optimized image
├── docker-compose.yml  # Full infrastructure (Kafka, UI, App)
└── pyproject.toml      # UV-managed dependencies
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- [uv](https://github.com/astral-sh/uv) (for local development)
- Python 3.12+

### 🐳 Running with Docker (Recommended)

Start the entire stack (Kafka, 6 Consumers, Producer, and Kafka UI):

```bash
docker compose up -d --build --scale consumer=6
```

- **Kafka UI**: [http://localhost:8080](http://localhost:8080)
- **External Kafka**: `localhost:9094`

### 💻 Local Development

1. **Install Dependencies**:
   ```bash
   uv sync
   ```

2. **Run Consumer**:
   ```bash
   uv run python -m src.consumer
   ```

3. **Run Producer (Performance Test)**:
   ```bash
   uv run python -m src.producer
   ```

---

## 📊 Infrastructure Details

### Network & Port Mapping
- **External Port**: `9094` (to avoid host conflicts with default 9092).
- **Internal Port**: `29092`.

### Scaling Strategy
The topic `test-topic` is initialized with **24 partitions**. 
- With 6 replicas, each consumer handles **4 partitions**.
- The cluster can scale up to **24 replicas** without reconfiguring Kafka.

### Data Persistence
Kafka data is stored in the `kafka_data` volume. To wipe all data and start fresh:
```bash
docker compose down -v
```

---

## 🛠️ Performance Tuning

| Component | Optimization | Value |
| :--- | :--- | :--- |
| **Producer** | `linger.ms` | `20` |
| **Producer** | `compression` | `snappy` |
| **Producer** | `batch.size` | `32KB` |
| **Consumer** | `batch_size` | `100` |
| **Consumer** | `fetch.min.bytes`| `16KB` |
| **Consumer** | `fetch.wait.max.ms` | `500` |

---

## 📝 Maintenance

- **View Logs**: `docker compose logs -f consumer`
- **Check Partitions**: `docker exec kafka kafka-topics --describe --topic test-topic --bootstrap-server localhost:29092`
- **Clean Up**: `docker compose down`
