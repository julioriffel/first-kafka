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
