# First Kafka Project

A sample Python-based Kafka producer and consumer implementation using `confluent-kafka`, Pydantic Settings, and Docker. This project demonstrates high-performance asynchronous message production and efficient batch consumption.

## Features

- **Asynchronous Producer**: Implements non-blocking message production with delivery reports.
- **Batch Consumer**: Efficiently consumes messages in batches to reduce network overhead.
- **Micro-Infrastructure**: Includes Docker Compose configuration for Kafka (KRaft mode) and Kafka UI.
- **Containerized**: Fully dockerized with a multi-stage `Dockerfile` using `uv`.
- **TDD Approach**: Unit tests with `pytest` and code quality with `ruff`.

## Project Structure

```text
first-kafka/
├── src/
│   ├── config.py       # Configuration management using Pydantic Settings
│   ├── producer.py     # Optimized Kafka Producer
│   └── consumer.py     # Optimized Batch Kafka Consumer
├── tests/
│   ├── test_producer.py
│   └── test_consumer.py
├── docs/
│   └── README.md       # Backup documentation
├── docker-compose.yml  # Kafka infrastructure and app services
├── Dockerfile          # Production-ready multi-stage build
├── pyproject.toml      # Project dependencies and tool configuration
├── uv.lock             # Dependency lockfile
└── changelog.md        # History of changes
```

## Installation

### Prerequisites
- Python 3.12+
- [uv](https://github.com/astral-sh/uv)
- Docker & Docker Compose

### Local Setup
1. Clone the repository and navigate to the directory.
2. Install dependencies:
   ```bash
   uv sync
   ```

## Infrastructure

The project uses port `9094` for external Kafka access (to avoid conflicts with default `9092`).

### Start Infrastructure
```bash
docker compose up -d
```
- **Kafka UI**: [http://localhost:8080](http://localhost:8080)
- **Kafka Broker**: `localhost:9094`

## Usage

### Using Docker (Recommended)
You can run the entire stack (Kafka KRaft + Producer + Consumer) automatically:
```bash
docker compose up -d
```

### Scaling Consumers
By default, the consumer runs with 3 replicas. You can scale it further using:
```bash
docker compose up -d --scale consumer=5
```

To view consumer logs:
```bash
docker compose logs -f consumer
```

### Running Locally
First, ensure the Docker infrastructure is running (for the broker).

**Run Producer:**
```bash
uv run python -m src.producer
```

**Run Consumer:**
```bash
uv run python -m src.consumer
```

## Development

### Running Tests
```bash
uv run pytest
```

### Linting & Formatting
```bash
uv run ruff check --fix .
uv run ruff format .
```
