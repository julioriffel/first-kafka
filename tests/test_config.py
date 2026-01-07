from src.config import Settings


def test_config_defaults():
    settings = Settings()
    assert settings.KAFKA_TOPIC == "test-topic"
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "localhost:9094"
    assert settings.KAFKA_GROUP_ID == "test-group"
