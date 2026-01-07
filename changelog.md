# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Initial project structure with `uv`.
- Docker Compose configuration for Kafka and Kafka UI.
- Migrated Kafka to KRaft mode (removed Zookeeper).
- Pydantic settings for configuration management.
- Base implementation plan for Producer and Consumer.
- Optimized Producer (asynchronous) and Consumer (batching).
- Containerized application with multi-stage Dockerfile.
- Scaled consumer to 3 replicas.
- Upgraded Kafka infrastructure to Confluent Platform 8.0.0 (KRaft required).
- Scaled to 24 partitions and 6 replicas for massive throughput.
- Implemented Storage Persistence with Docker Volumes.
- Optimized Producer/Consumer codebase:
    - Added high-performance batching (32KB/20ms).
    - Enabled Snappy compression.
    - Reduced I/O overhead via throttled polling and streamlined logging.
- Added project shortcuts via `uv` and `taskipy` for producer, consumer, and infrastructure management.
- Configured `pre-commit` with `ruff` and `pytest` integration.
- Added GitHub Actions CI pipeline for automated testing and linting.
